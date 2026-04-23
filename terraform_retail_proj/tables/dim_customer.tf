resource "snowflake_table" "dim_customer" {
  database = "GOLD_LAYER_DB"
  schema   = "GOLD"
  name     = "DIM_CUSTOMER"

  column {
    name = "CUSTOMER_ID"
    type = "STRING"
  }

  column {
    name = "NAME"
    type = "STRING"
  }

  column {
    name = "CITY"
    type = "STRING"
  }
}