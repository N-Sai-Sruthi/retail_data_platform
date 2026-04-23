provider "snowflake" {
  organization_name = "ICKPHXN"
  account_name      = "BK48769"

  user     = var.username
  password = var.password
  role     = var.role
}