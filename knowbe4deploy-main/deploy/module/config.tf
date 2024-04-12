terraform {
  required_version = "~> 1.0.4" # which means any version equal & above 0.14 like 0.15, 0.16 etc and < 1.xx
  required_providers {
    archive = {
      source = "hashicorp/archive"
    }
    aws = {
      source  = "hashicorp/aws"
      version = "~> 3.0"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "1.2.0"
    }
  }
}
# Provider Block
provider "aws" {
  region = "us-east-1"
  profile = "directsupply-sandbox"
}
provider "vault" {
    address = "https://vault.us-east-1.management.directsupply-sandbox.cloud"
}

# Note - token & host provided through environment variables.
provider "databricks" {
}

terraform {
  backend "s3" {
    bucket = "ds-tfstate-sandbox"
    key = "databricks/automation/knowbe4_load"
    region = "us-east-1"
    dynamodb_table = "terraform-lock"
  }
}