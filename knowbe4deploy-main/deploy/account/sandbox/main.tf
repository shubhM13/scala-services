module "databricks_job_deployment" {
  source = "../../module"
  environment                   = "sandbox"
  account_name                  = "DataBricksSandbox@directsupply.com"
}
