# Data Loader

The purpose of this job is to build the raw delta layer by pulling data from SQL Server.

## Usage

loadincrementaldata server.db.schema.table

(Run on Databricks)

## Prerequisites

1) Setup vault dynamic credentials for Databricks. This code sets up a vault role for TRAN.

```
module "vault_role" {
  source = "git::ssh://git@gitlab.directsupply.cloud/terraform-modules/vault-role-for-mssql-role.git?ref=v1"

  mssql_role_name   = "DSIAPP_DatabricksReplication"
  mssql_server_name = "SQL-DSSI-DEV-TRAN"
  ttl_minutes       = local.vault_role_ttl_minutes
}

resource "vault_policy" "lambda_vault_policy" {
  name = "test-vault-policy"

  policy = <<EOT
    path "${local.vault_db_path}" {
      capabilities = ["read"]
    }
  EOT
}

resource "vault_generic_secret" "vault_role" {
  path = "auth/aws/role/${aws_iam_role.role_data_external.name}"

  data_json = <<EOT
  {
    "bound_iam_principal_arn": "${aws_iam_role.role_data_external.arn}",
    "policies": "${vault_policy.lambda_vault_policy.name}",
    "default_ttl": "60m",
    "max_ttl": "60m"
  }
  EOT
}
```

3) Setup permissions & CHANGETABLE for all the tables you will be replicating. This code will set this up on a DBUP deployment.

```
if not exists (select 1 from sys.database_principals where name='DSIAPP_DatabricksReplication' and Type = 'R')
begin
CREATE ROLE [DSIAPP_DatabricksReplication];
end

exec sp_addrolemember 'db_datareader', 'DSIAPP_DatabricksReplication';

GRANT VIEW CHANGE TRACKING ON SCHEMA::dbo TO DSIAPP_DatabricksReplication
```

This sample code will set up CHANGETABLE on sever & table:
```
IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_databases
               WHERE database_id = DB_ID())
BEGIN
     ALTER DATABASE [CHLIVE]
     SET CHANGE_TRACKING = ON;
END

IF NOT EXISTS (SELECT 1 FROM sys.change_tracking_tables
               WHERE object_id = OBJECT_ID('dbo.ChLinesMiscB'))
BEGIN
     ALTER TABLE dbo.ChLinesMiscB
     ENABLE CHANGE_TRACKING
     WITH (TRACK_COLUMNS_UPDATED = ON)
END
```

## Job Overview

On running for the first time, the job will replicate all existing data in the table and save the current version in the checkpoints table.
This is the initial load of the table, and on all subsequent runs it will perform incremental load. The data will be saved to a delta
table as such: server.db_schema_table. For example if replicating SQL-DSSI-DEV-TRAN, CHLIVE, ChCode, then it would save to the delta
table SQL_DSSI_DEV-TRAN.CHLIVE_dbo_ChCode.

On future runs of the job, it will perform incremental load. It will get the current table version and compare this against the known
checkpoint value. If the current version is newer, it will load all data belonging to the versions greater than the current version.
Data will be merged into the same table, and the checkpoint value will be updated.

Additionally, all dashes in the name will be replaced by underscores to satisfy naming requirements of the metastore.

To run the job manually after deployment, you can go to the Databricks console, Workflows section, find
the job "Load incremental data" and press "Run now with different parameters". As the parameters, write it
like this in the JSON section:
```
["SQL-DSSI-DEV-TRAN.CATALOG.dbo.Incat"]
```

The job also queries table CHANGE_TRACKING_MIN_VALID_VERSION(). If the minimum valid version is higher than the current
known version of the table, then the entire table will be reloaded due to the table having been purged.

## Checkpoint behavior

Whenever this job runs and replicates data, it will save the current version of the table in an internal delta table for change tracking. The
saved version will be the max value from the changetable.

The delta table saved to will be named data_platform.checkpoints. This table will be used for all current version checkpoints across
all servers. The table has columns: server, db, schema, table, value; the server/db/schema/table names form a composite key and value is the last retrieved
version checkpoint.

## Deployment

To deploy the jar, go to the Gitlab project for this code, click Pipelines, and run the manual scala-build step.

To deploy the job, go to the Gitlab project for the Terraform code, click Pipelines, and deploy.

You don't need to re-deploy the Terraform job every time you make a code update. Just deploy the updated code.

## Scheduling

This job will run on a schedule, every 15 minutes. When ran on a schedule, the arguments passed in will be empty and the job
will internally decide which group of tables it will process. There are 7 groups of tables that will be loaded in sequence, so
this will result in each table having an update period between 1h 45m and 2h. Job will log the time taken
to process all the tables.

## Debugging Checklist

Problems loading data can happen for the following reasons:

1) No account setup, or invalid access for account
- Check for the vault role DSIAPP_DatabricksReplication on the sql server. If it does not exist, create it using the Databricks terraform.
2) CHANGETABLE not set up on SQL Server
- Test and see if CHANGETABLE works on a table in SQL Server. If not, modify dbup job to enable CHANGETABLE
3) Incorrect arguments provided
- Arguments will fail if they don't have 4 parts, i.e. server.db.schema.table, or if the server does not match across all tables.