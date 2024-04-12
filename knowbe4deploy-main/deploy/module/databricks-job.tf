resource "databricks_job" "knowbe4_users_job" {
  name = "KnowBe4 - Load Knowbe4 Users"

  max_concurrent_runs = 1
  timeout_seconds = 36000

  notebook_task {
    notebook_path = databricks_notebook.knowbe4_code_users.path
  }

  new_cluster {
    num_workers   = 2
    spark_version = data.databricks_spark_version.latest.id
    node_type_id  = data.databricks_node_type.smallest.id
    data_security_mode = "SINGLE_USER"
    aws_attributes {
      instance_profile_arn = data.aws_iam_instance_profile.profile.arn
    }
    single_user_name = "${var.account_name}"
    custom_tags = {
      "OwnerEmail":"datanomads@directsupply.com"
      "Application":"databricks"
      "LineOfBusiness":"CIS"
      "Role":"cluster"
      "Environment":"sandbox"
      "Lifespan":"permanent"
    }
  }

  schedule {
    quartz_cron_expression = "48 1 2 ? * Wed"
    timezone_id = "US/Eastern"
  }
}

resource "databricks_job" "knowbe4_campaigns_job" {
  name = "KnowBe4 - Load Knowbe4 Campaigns"

  max_concurrent_runs = 1
  timeout_seconds = 36000

  notebook_task {
    notebook_path = databricks_notebook.knowbe4_code_campaigns.path
  }

  new_cluster {
    num_workers   = 2
    spark_version = data.databricks_spark_version.latest.id
    node_type_id  = data.databricks_node_type.smallest.id
    data_security_mode = "SINGLE_USER"
    aws_attributes {
      instance_profile_arn = data.aws_iam_instance_profile.profile.arn
    }
    single_user_name = "${var.account_name}"
    custom_tags = {
      "OwnerEmail":"datanomads@directsupply.com"
      "Application":"databricks"
      "LineOfBusiness":"CIS"
      "Role":"cluster"
      "Environment":"sandbox"
      "Lifespan":"permanent"
    }
  }

  schedule {
    quartz_cron_expression = "48 1 4 ? * Wed"
    timezone_id = "US/Eastern"
  }
}

resource "databricks_job" "knowbe4_delta_job" {
  name = "KnowBe4 - Load Knowbe4 Delta Tables"

  max_concurrent_runs = 1
  timeout_seconds = 36000

  notebook_task {
    notebook_path = databricks_notebook.knowbe4_code_delta.path
  }

  new_cluster {
    num_workers   = 2
    spark_version = data.databricks_spark_version.latest.id
    node_type_id  = data.databricks_node_type.smallest.id
    data_security_mode = "SINGLE_USER"
    aws_attributes {
      instance_profile_arn = data.aws_iam_instance_profile.profile.arn
    }
    single_user_name = "${var.account_name}"
    custom_tags = {
      "OwnerEmail":"datanomads@directsupply.com"
      "Application":"databricks"
      "LineOfBusiness":"CIS"
      "Role":"cluster"
      "Environment":"sandbox"
      "Lifespan":"permanent"
    }
  }

  schedule {
    quartz_cron_expression = "48 1 6 ? * Wed"
    timezone_id = "US/Eastern"
  }
}
