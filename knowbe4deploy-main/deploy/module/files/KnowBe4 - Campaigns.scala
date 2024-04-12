// Databricks notebook source
// MAGIC %python
// MAGIC spark.conf.set("spark.databricks.delta.state.corruptionIsFatal", False)

// COMMAND ----------

// MAGIC %python
// MAGIC import json
// MAGIC import time
// MAGIC import string
// MAGIC from urllib.error import HTTPError
// MAGIC import requests
// MAGIC import boto3
// MAGIC 
// MAGIC __url_cache__ = {}
// MAGIC 
// MAGIC class KnowBe4(object):
// MAGIC 
// MAGIC     def __init__(self, token=''):
// MAGIC         self._base_url = 'https://us.api.knowbe4.com/v1'
// MAGIC         self._token = token
// MAGIC 
// MAGIC     def _build_url(self, *args, **kwargs):
// MAGIC         parts = [kwargs.get('base_url') or self._base_url]
// MAGIC         parts.extend(args)
// MAGIC         parts = [str(p) for p in parts]
// MAGIC         key = tuple(parts)
// MAGIC         if key not in __url_cache__:
// MAGIC             __url_cache__[key] = '/'.join(parts)
// MAGIC         return __url_cache__[key]
// MAGIC 
// MAGIC     def _headers(self):
// MAGIC         return {'Authorization': 'Bearer {0}'.format(self._token)}
// MAGIC 
// MAGIC     def _json(self, response):
// MAGIC         if response is None:
// MAGIC             return None
// MAGIC         else:
// MAGIC             ret = response.json()
// MAGIC         return ret
// MAGIC 
// MAGIC     def _request(self, method, url, data=None, headers=None, json=True):
// MAGIC         headers = self._headers()
// MAGIC         resp = requests.request(method, url, data=data, headers=headers)
// MAGIC         
// MAGIC         if resp.status_code >= 500 or resp.status_code == 429: # custom
// MAGIC             print('Error ' + str(resp.status_code) + '... retrying after 5 minutes:' + url)
// MAGIC             time.sleep(300)
// MAGIC             resp = requests.request(method, url, data=data, headers=headers)
// MAGIC             return resp
// MAGIC         if resp.status_code == 204:
// MAGIC             return None
// MAGIC         if 200 <= resp.status_code < 300:
// MAGIC             return resp
// MAGIC         else:
// MAGIC             resp.raise_for_status()
// MAGIC 
// MAGIC     def _get(self, url):
// MAGIC         return self._request('GET', url)
// MAGIC 
// MAGIC 
// MAGIC     def _api_call(self, *args, filename: string, **kwargs):
// MAGIC         page_counter = 1 # counter to update url page
// MAGIC         data = [] # stores API data as dictionaries in each index
// MAGIC         users = [] # dictionary to be sent to S3
// MAGIC         json_result = [] # holds the most recent API request (list of one dictionary)
// MAGIC         
// MAGIC 
// MAGIC         while True: # "do while" loop
// MAGIC             #build url
// MAGIC             #page_endpoint = '?page=' + str(page_counter) + '&per_page=500'
// MAGIC             url = self._build_url(*args)#\ + page_endpoint
// MAGIC             #print(url)
// MAGIC             
// MAGIC             # json_result is a list of dictionaries from the URL API call
// MAGIC             json_result = list(self._json(self._get(url))) 
// MAGIC 
// MAGIC             
// MAGIC             if (json_result == []): # "do while" loop sentinel value
// MAGIC                 break
// MAGIC             elif url == 'https://us.api.knowbe4.com/v1/users': # get user data
// MAGIC                 #print(json_result[0]['id']) # get id of first user in json_result
// MAGIC                 page_counter = 1 # page_counter used to change pages in URL requests
// MAGIC                 user_count = 0 # user counter
// MAGIC                 user_ids = [] # array to store user ids
// MAGIC                 page_endpoint = '?status=active&page=' + str(page_counter) + '&per_page=500' # custom endpoint to get all user IDs
// MAGIC                 url = self._build_url(*args) + page_endpoint # concatenate url
// MAGIC                 json_result = list(self._json(self._get(url))) # make API call with new URL
// MAGIC 
// MAGIC                 while json_result != [] and user_count < len(json_result): # while result is not an empty array and user count != number of users...
// MAGIC                     user_ids.append(json_result[user_count]['id']) # add id value of user_count (employee num 1, num2, etc)
// MAGIC                     #print(user_ids[user_count])
// MAGIC                     user_count += 1 # increment user_count for every user
// MAGIC                     if user_count == 500: # if user count is 500...
// MAGIC                         user_count = 0 # reset user count - since you're starting a new page
// MAGIC                         page_counter += 1 # increment the page counter to go to the next page
// MAGIC                         page_endpoint = '?status=active&page=' + str(page_counter) + '&per_page=500' # update page counter in page endpoint 
// MAGIC                         url = self._build_url(*args) + page_endpoint # rebuild URL with new endpoint
// MAGIC                         time.sleep(1) 
// MAGIC                         json_result = list(self._json(self._get(url))) # get new API request
// MAGIC                         print(url)
// MAGIC                 
// MAGIC                 user_count = 0 # reset user count
// MAGIC                 users = [] # array stores dictionaries of each employee (user)
// MAGIC                 while user_count < len(user_ids): # loop through every user
// MAGIC                     risk_history_url = 'https://us.api.knowbe4.com/v1/users/' + str(user_ids[user_count]) + '/risk_score_history' # build risk history URL with individual ID
// MAGIC                     print(risk_history_url)
// MAGIC                     time.sleep(1)
// MAGIC                     risk_result = self._json(self._get(risk_history_url)) # get risk history for specific ID
// MAGIC 
// MAGIC                     if risk_result == requests.exceptions.HTTPError:
// MAGIC                         time.sleep(1)
// MAGIC                         risk_result = self._json(self._get(risk_history_url))
// MAGIC 
// MAGIC                     date_count = 0 # set date_count to 0 (counter for every date in list)
// MAGIC                     user_url = 'https://us.api.knowbe4.com/v1/users/' + str(user_ids[user_count]) # URL to get all info about one user
// MAGIC                     time.sleep(1)
// MAGIC                     user_dict = self._json(self._get(user_url)) # json_result is a dictionary of data about one specific user
// MAGIC                     while date_count < len(risk_result): # loop through all dates in risk history
// MAGIC                         user_dict.update(risk_result[date_count]) # update dictionary with risk score at specific date
// MAGIC                         date_count +=1 # increment date_count to move to next date
// MAGIC                         users.append(user_dict.copy()) # update total employee dictionary with risk score at specific date
// MAGIC                         #data += users #9/28 append users dictionary to data dictionary
// MAGIC                     user_count += 1 # increment user count
// MAGIC 
// MAGIC                     if user_count % 4 == 0: # rate limiting -> can't have more than 4 requests per second, sleep for 5 seconds as a precaution
// MAGIC                         print('5 second delay')
// MAGIC                         time.sleep(5)
// MAGIC                         
// MAGIC                     
// MAGIC                 #print(len(users)) # checks that the user list is growing with only active employees and their risk scores throughout time
// MAGIC                 f = open('users_with_risk.json', 'w')
// MAGIC                 json.dump(users, f, indent = 4)
// MAGIC                 f.close()
// MAGIC                 break
// MAGIC                 
// MAGIC                 
// MAGIC                 
// MAGIC                 
// MAGIC             elif url == 'https://us.api.knowbe4.com/v1/phishing/campaigns': # get campaign data
// MAGIC                 campaign_count = 0 # number of campaigns
// MAGIC                 campaign_ids = [] # array of campaign ids
// MAGIC                 while campaign_count < len(json_result): # get all campaign ids
// MAGIC                     campaign_ids.append(json_result[campaign_count]['psts'][0]['pst_id']) # append campaign id to campaign_ids list
// MAGIC                     campaign_count += 1 # update campaign count
// MAGIC                 self.get_campaign_recipients(campaign_ids) # get all recipients based on campaign ids
// MAGIC                 break
// MAGIC         
// MAGIC         # send to s3 bucket: ds-platform-analytics/Knowbe4/
// MAGIC         s3 = boto3.resource('s3')
// MAGIC         with open('/tmp/' + filename + '.json', 'w') as write_file:
// MAGIC             json.dump(users, write_file, indent = 4)
// MAGIC         write_file.close()
// MAGIC         s3.Bucket('ds-data-databricks-sandbox').upload_file('/tmp/' + filename + '.json', 'KnowBe4/raw/' + filename + '.json')
// MAGIC 
// MAGIC 
// MAGIC     def get_campaign_recipients(self, campaign_ids):
// MAGIC         #print(len(campaign_ids))
// MAGIC         campaign_page_counter = 1 # counter to update url page
// MAGIC         campaign_id_counter = 0
// MAGIC         data = [] # stores API data as dictionaries in each index -> to be sent to S3
// MAGIC         json_result = [] # holds the most recent API request (list of one dictionary)
// MAGIC 
// MAGIC         while campaign_id_counter < len(campaign_ids): # "do while" loop
// MAGIC             # update url to get all users of a specific campaign
// MAGIC             url = 'https://us.api.knowbe4.com/v1/phishing/security_tests/' + str(campaign_ids[campaign_id_counter]) + '/recipients' + '?page=' + str(campaign_page_counter) + '&per_page=500'
// MAGIC             print(url)
// MAGIC             time.sleep(1)
// MAGIC             
// MAGIC             json_result = self._json(self._get(url)) # store all users of a specific campaign id
// MAGIC 
// MAGIC             if (json_result == []): # if a blank result, reset page counter and increment to next campaign
// MAGIC                 campaign_page_counter = 0
// MAGIC                 campaign_id_counter += 1
// MAGIC 
// MAGIC             data += json_result # append result to dictionary
// MAGIC             campaign_page_counter += 1 # increment page counter
// MAGIC         
// MAGIC         # send to s3 bucket: ds-platform-analytics/Knowbe4/
// MAGIC         s3 = boto3.resource('s3')
// MAGIC         filename = 'all_campaign_recipients'
// MAGIC         with open('/tmp/' + filename + '.json', 'w') as write_file:
// MAGIC             json.dump(data, write_file, indent = 4)
// MAGIC         write_file.close()
// MAGIC         s3.Bucket('ds-data-databricks-sandbox').upload_file('/tmp/' + filename + '.json', 'KnowBe4/raw/' + filename + '.json')
// MAGIC 
// MAGIC         f = open('all_campaign_recipients.json', 'w')
// MAGIC         json.dump(data, f, indent = 4)
// MAGIC         f.close()
// MAGIC 
// MAGIC 
// MAGIC     def account(self):
// MAGIC         return self._api_call('account')
// MAGIC 
// MAGIC     def users(self):
// MAGIC         return self._api_call('users', filename="users_with_risk_history ")
// MAGIC 
// MAGIC     def groups(self):
// MAGIC         return self._api_call('groups', filename="groups")
// MAGIC 
// MAGIC     def group(self, id):
// MAGIC         return self._api_call('groups', id)
// MAGIC 
// MAGIC     def group_members(self, id):
// MAGIC         return self._api_call('groups', id, 'members')
// MAGIC 
// MAGIC     def user(self, id):
// MAGIC         return self._api_call('users', id)
// MAGIC 
// MAGIC     def phishing_campaigns(self):
// MAGIC         return self._api_call('phishing', 'campaigns', filename="phishing_campaigns")
// MAGIC 
// MAGIC     def phishing_campaign(self, id):
// MAGIC         return self._api_call('phishing', 'campaigns', id)
// MAGIC 
// MAGIC     def phishing_security_tests(self):
// MAGIC         return self._api_call('phishing', 'security_tests')
// MAGIC 
// MAGIC     def phishing_campaign_security_tests(self, id):
// MAGIC         return self._api_call('phishing', 'campaigns', id, 'security_tests')
// MAGIC 
// MAGIC     def phishing_campaign_security_test(self, id):
// MAGIC         return self._api_call('phishing', 'security_tests', id)
// MAGIC 
// MAGIC     def phishing_campaign_security_test_recipients(self, id):
// MAGIC         return self._api_call('phishing', 'security_tests', id, 'recipients')
// MAGIC 
// MAGIC     def phishing_campaign_security_test_recipient(self, pst_id, id):
// MAGIC         return self._api_call('phishing', 'security_tests', pst_id, 'recipients', id)
// MAGIC 
// MAGIC     def store_purchases(self):
// MAGIC         return self._api_call('training', 'store_purchases')
// MAGIC 
// MAGIC     def store_purchase(self, id):
// MAGIC         return self._api_call('training', 'store_purchases', id)
// MAGIC 
// MAGIC     def policies(self):
// MAGIC         return self._api_call('policies')
// MAGIC 
// MAGIC     def policy(self, id):
// MAGIC         return self._api_call('policies', id)
// MAGIC 
// MAGIC     def training_campaigns(self):
// MAGIC         return self._api_call('training', 'campaigns')
// MAGIC 
// MAGIC     def training_campaign(self, id):
// MAGIC         return self._api_call('training', 'campaigns', id)
// MAGIC 
// MAGIC     def training_enrollments(self):
// MAGIC         return self._api_call('training', 'enrollments')
// MAGIC 
// MAGIC     def training_enrollment(self, id):
// MAGIC         return self._api_call('training', 'enrollment', id)

// COMMAND ----------

// MAGIC %python
// MAGIC # nathan's test code
// MAGIC # define key
// MAGIC key=dbutils.secrets.get(scope="Knowbe4", key="secret_key")
// MAGIC 
// MAGIC def lambda_handler(event, context):
// MAGIC     kb4 = KnowBe4(key)
// MAGIC     # API requests
// MAGIC     kb4.phishing_campaigns()
// MAGIC     return None
// MAGIC 
// MAGIC lambda_handler(None,None)
