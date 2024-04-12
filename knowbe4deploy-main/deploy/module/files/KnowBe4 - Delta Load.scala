// Databricks notebook source
spark.sql("CREATE CATALOG IF NOT EXISTS knowbe4")
spark.sql("CREATE SCHEMA IF NOT EXISTS knowbe4.raw")

// COMMAND ----------

// MAGIC %python
// MAGIC import pandas as pd
// MAGIC import json
// MAGIC import boto3
// MAGIC from pyspark.sql.types import *
// MAGIC from io import StringIO
// MAGIC import sys
// MAGIC from pyspark.context import SparkContext
// MAGIC from pyspark.sql.session import SparkSession
// MAGIC from pandas import json_normalize
// MAGIC 
// MAGIC s3 = boto3.client('s3')
// MAGIC 
// MAGIC BUCKET = 'ds-data-databricks-sandbox'
// MAGIC file_users_with_risk_history = 'KnowBe4/raw/users_with_risk_history .json'
// MAGIC file_all_campaign = 'KnowBe4/raw/all_campaign_recipients.json'
// MAGIC 
// MAGIC # read phishing file from s3
// MAGIC #result_phishing = s3.get_object(Bucket=BUCKET, Key=file_phishing)
// MAGIC #phishing = json.loads(result_phishing["Body"].read().decode())
// MAGIC #df_phishing_raw=pd.DataFrame.from_dict(pd.json_normalize(phishing), orient='columns')
// MAGIC #df = df_phishing_raw.explode('psts').reset_index(drop=True)
// MAGIC #df_psts = df['psts'].apply(pd.Series)
// MAGIC #df_phishing = pd.concat([df, df_psts], axis = 1).drop('psts', axis = 1)
// MAGIC #status = {'status': ['status1', 'status2']}
// MAGIC #df_phishing = df_phishing.rename(columns=lambda c: status[c].pop(0) if c in status.keys() else c)
// MAGIC #df1 = df_phishing.explode('groups').reset_index(drop=True)
// MAGIC #df_groups = df1['groups'].apply(pd.Series)
// MAGIC #df_phishing_final = pd.concat([df1, df_groups], axis = 1).drop('groups', axis = 1)
// MAGIC #df_phishing_final = df_phishing_final.drop(0, axis=1)
// MAGIC #name = {'name': ['name1', 'name2']}
// MAGIC #df_phishing_final = df_phishing_final.rename(columns=lambda c: name[c].pop(0) if c in name.keys() else c)
// MAGIC 
// MAGIC # # define phishing schema
// MAGIC #phishing_schema = StructType([
// MAGIC #    StructField("campaign_id", StringType(), True),
// MAGIC #    StructField("name1", StringType(), True),
// MAGIC #    StructField("last_phish_prone_percentage", StringType(), True),
// MAGIC #    StructField("last_run", StringType(), True),
// MAGIC #    StructField("status1", StringType(), True),
// MAGIC #    StructField("hidden", StringType(), True),
// MAGIC #    StructField("send_duration", StringType(), True),
// MAGIC #    StructField("track_duration", StringType(), True),
// MAGIC #    StructField("frequency", StringType(), True),
// MAGIC #   StructField("difficulty_filter", StringType(), True),
// MAGIC #   StructField("create_date", StringType(), True),
// MAGIC #    StructField("psts_count", StringType(), True),
// MAGIC #    StructField("pst_id", StringType(), True),
// MAGIC #    StructField("status2", StringType(), True),
// MAGIC #    StructField("start_date", StringType(), True),
// MAGIC #    StructField("users_count", StringType(), True),
// MAGIC #    StructField("phish_prone_percentage", StringType(), True),
// MAGIC #    StructField("group_id", StringType(), True),
// MAGIC #    StructField("name2", StringType(), True)
// MAGIC     
// MAGIC #])
// MAGIC 
// MAGIC users_with_risk_history_result = s3.get_object(Bucket=BUCKET, Key=file_users_with_risk_history)
// MAGIC users_with_risk_history = json.loads(users_with_risk_history_result["Body"].read().decode())
// MAGIC df_users_with_risk_history_raw = pd.DataFrame(users_with_risk_history)
// MAGIC 
// MAGIC # users with risk history schema
// MAGIC users_schema = StructType([
// MAGIC     StructField("id", StringType(), True),
// MAGIC     StructField("employee_number", StringType(), True),
// MAGIC     StructField("first_name", StringType(), True),
// MAGIC     StructField("last_name", StringType(), True),
// MAGIC     StructField("job_title", StringType(), True),
// MAGIC     StructField("email", StringType(), True),
// MAGIC     StructField("phish_pronepercentage", StringType(), True),
// MAGIC     StructField("phone_number", StringType(), True),
// MAGIC     StructField("extension", StringType(), True),
// MAGIC     StructField("mobile_phone_number", StringType(), True),
// MAGIC     StructField("location", StringType(), True),
// MAGIC     StructField("division", StringType(), True),
// MAGIC     StructField("manager_name", StringType(), True),
// MAGIC     StructField("manager_email", StringType(), True),
// MAGIC     StructField("provisioning_managed", StringType(), True),
// MAGIC     StructField("provisioning_guid", StringType(), True),
// MAGIC     StructField("groups", StringType(), True),
// MAGIC     StructField("current_risk_score", StringType(), True),
// MAGIC     StructField("aliases", StringType(), True),
// MAGIC     StructField("joined_on", StringType(), True),
// MAGIC     StructField("last_sign_in", StringType(), True),
// MAGIC     StructField("status", StringType(), True),
// MAGIC     StructField("organization", StringType(), True),
// MAGIC     StructField("department", StringType(), True),
// MAGIC     StructField("language", StringType(), True),
// MAGIC     StructField("comment", StringType(), True),
// MAGIC     StructField("employee_start_date", StringType(), True),
// MAGIC     StructField("archived_at", StringType(), True),
// MAGIC     StructField("custom_field_1", StringType(), True),
// MAGIC     StructField("custom_field_2", StringType(), True),
// MAGIC     StructField("custom_field_3", StringType(), True),
// MAGIC     StructField("custom_field_4", StringType(), True),
// MAGIC     StructField("custom_date_1", StringType(), True),
// MAGIC     StructField("custom_date_2", StringType(), True),
// MAGIC     StructField("risk_score", StringType(), True),
// MAGIC     StructField("date", StringType(), True)
// MAGIC ])
// MAGIC 
// MAGIC #phishingsparkDF=spark.createDataFrame(df_phishing_final,phishing_schema)
// MAGIC users_with_risk_history_DF = spark.createDataFrame(df_users_with_risk_history_raw, users_schema)
// MAGIC 
// MAGIC # # define phishing schema
// MAGIC all_campaign_schema = StructType([
// MAGIC     StructField("recipient_id", StringType(), True),
// MAGIC     StructField("pst_id", StringType(), True),
// MAGIC     StructField("scheduled_at", StringType(), True),
// MAGIC     StructField("delivered_at", StringType(), True),
// MAGIC     StructField("opened_at", StringType(), True),
// MAGIC     StructField("clicked_at", StringType(), True),
// MAGIC     StructField("replied_at", StringType(), True),
// MAGIC     StructField("attachment_opened_at", StringType(), True),
// MAGIC     StructField("macro_enabled_at", StringType(), True),
// MAGIC     StructField("data_entered_at", StringType(), True),
// MAGIC     StructField("qr_code_scanned", StringType(), True),
// MAGIC     StructField("reported_at", StringType(), True),
// MAGIC     StructField("bounced_at", StringType(), True),
// MAGIC     StructField("ip", StringType(), True),
// MAGIC     StructField("ip_location", StringType(), True),
// MAGIC     StructField("browser", StringType(), True),
// MAGIC     StructField("browser_version", StringType(), True),
// MAGIC     StructField("os", StringType(), True),
// MAGIC     StructField("id1", StringType(), True),
// MAGIC     StructField("provisioning_guid", StringType(), True),
// MAGIC     StructField("first_name", StringType(), True),
// MAGIC     StructField("last_name", StringType(), True),
// MAGIC     StructField("email", StringType(), True),
// MAGIC     StructField("id2", StringType(), True),
// MAGIC     StructField("name", StringType(), True),
// MAGIC     StructField("difficulty", StringType(), True),
// MAGIC     StructField("type", StringType(), True)
// MAGIC ])
// MAGIC 
// MAGIC 
// MAGIC result = s3.get_object(Bucket=BUCKET, Key=file_all_campaign)
// MAGIC all_campaign = json.loads(result["Body"].read().decode())
// MAGIC df_all_campaign_raw = pd.DataFrame(all_campaign)
// MAGIC 
// MAGIC df_col_user = json_normalize(df_all_campaign_raw['user'])
// MAGIC df_col_template = json_normalize(df_all_campaign_raw['template'])
// MAGIC data = [df_all_campaign_raw, df_col_user, df_col_template]
// MAGIC df2 = pd.concat(data, axis=1)
// MAGIC df2.drop(['user', 'template'], axis=1, inplace=True)
// MAGIC colname = {'id': ['id1', 'id2']}
// MAGIC df2_final = df2.rename(columns=lambda c: colname[c].pop(0) if c in colname.keys() else c)
// MAGIC 
// MAGIC 
// MAGIC all_campaigns_df=spark.createDataFrame(df2_final,all_campaign_schema)

// COMMAND ----------

// MAGIC %python
// MAGIC 
// MAGIC users_w_risk_history = spark.createDataFrame(df_users_with_risk_history_raw) 
// MAGIC users_w_risk_history.createOrReplaceTempView("users_with_risk_history_df")
// MAGIC all_campaigns_df.createOrReplaceTempView("all_campaigns_df")

// COMMAND ----------

import io.delta.tables._

val phishingsparkDF = spark.sql("SELECT * FROM all_campaigns_df")
if(spark.catalog.tableExists("knowbe4.raw.all_campaign_recipients")) {
  val table = DeltaTable.forPath("s3://ds-data-databricks-sandbox/knowbe4/all_campaign_recipients")
  if(table == null) {
    phishingsparkDF.writeTo("knowbe4.raw.all_campaign_recipients").tableProperty("location", "s3://ds-data-databricks-sandbox/knowbe4/all_campaign_recipients").using("delta").createOrReplace()
  } else {
    table.as("origin").merge(phishingsparkDF.as("i"), "origin.recipient_id = i.recipient_id AND origin.pst_id = i.pst_id").whenMatched.updateAll().whenNotMatched.insertAll().execute()
  }
} else {
  phishingsparkDF.writeTo("knowbe4.raw.all_campaign_recipients").tableProperty("location", "s3://ds-data-databricks-sandbox/knowbe4/all_campaign_recipients").using("delta").createOrReplace()
}

// COMMAND ----------

// MAGIC %scala
// MAGIC import io.delta.tables._
// MAGIC 
// MAGIC val usersDF = spark.read
// MAGIC   .option("multiLine", true).option("mode", "PERMISSIVE")
// MAGIC   .json("s3://ds-data-databricks-sandbox/KnowBe4/raw/users_with_risk_history .json")
// MAGIC if(spark.catalog.tableExists("knowbe4.raw.users_risk_history")) {
// MAGIC   usersDF.writeTo("knowbe4.raw.users_risk_history").append()
// MAGIC } else {
// MAGIC   usersDF.writeTo("knowbe4.raw.users_risk_history").tableProperty("location", "s3://ds-data-databricks-sandbox/knowbe4/users_risk_history").using("delta").createOrReplace()
// MAGIC }

// COMMAND ----------

spark.sql("CREATE CATALOG IF NOT EXISTS knowbe4")
spark.sql("CREATE SCHEMA IF NOT EXISTS knowbe4.transform")

// COMMAND ----------

import org.apache.spark.sql.functions.{concat, lit}

var users = spark.sql("select * from knowbe4.raw.users_risk_history")
users = users.withColumn("full_name", concat($"first_name", lit(" "), $"last_name"))

users.writeTo("knowbe4.transform.users_risk_history_full_names").tableProperty("location", "s3://ds-data-databricks-sandbox/knowbe4/transform/users_risk_history_full_names").using("delta").createOrReplace()

// COMMAND ----------

import org.apache.spark.sql.functions.col
import sqlContext.implicits._
var df = spark.sql("select * from knowbe4.transform.users_risk_history_full_names")
df = df.coalesce(1).repartition(1).orderBy($"date".desc).dropDuplicates("provisioning_guid")
df.writeTo("knowbe4.transform.users_no_duplicates").using("delta").tableProperty("location", "s3://ds-data-databricks-sandbox/knowbe4/transform/users_no_duplicates").createOrReplace()
