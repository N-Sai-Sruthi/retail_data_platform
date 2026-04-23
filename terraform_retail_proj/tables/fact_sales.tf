resource "snowflake_table" "fact_sales" {
  database = "GOLD_LAYER_DB"
  schema   = "GOLD"
  name     = "FACT_SALES"

  column {
    name = "ORDER_ID"
    type = "STRING"
  }

  column {
    name = "CUSTOMER_ID"
    type = "STRING"
  }

  column {
    name = "PRODUCT_ID"
    type = "STRING"
  }

  column {
    name = "DATE_KEY"
    type = "NUMBER"
  }

  column {
    name = "QUANTITY"
    type = "NUMBER"
  }

  column {
    name = "TOTAL_AMOUNT"
    type = "FLOAT"
  }
}