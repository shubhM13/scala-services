resource "databricks_notebook" "knowbe4_code_users" {
  source = "${path.module}/files/KnowBe4 - Users.scala"
  path   = "/Shared/KnowBe4/KnowBe4 - Users"
}

resource "databricks_notebook" "knowbe4_code_campaigns" {
  source = "${path.module}/files/KnowBe4 - Campaigns.scala"
  path   = "/Shared/KnowBe4/KnowBe4 - Campaigns"
}

resource "databricks_notebook" "knowbe4_code_delta" {
  source = "${path.module}/files/KnowBe4 - Delta Load.scala"
  path   = "/Shared/KnowBe4/KnowBe4 - Delta Load"
}