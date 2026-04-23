terraform {
  required_providers {
    snowflake = {
      source  = "snowflakedb/snowflake"
      version = "~> 0.100"
    }
  }

  required_version = ">= 1.0"
}

module "tables" {
  source = "./tables"

  providers = {
    snowflake = snowflake
  }
}