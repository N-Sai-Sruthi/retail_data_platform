resource "snowflake_table" "dim_product" {
  database = "GOLD_LAYER_DB"
  schema   = "GOLD"
  name     = "DIM_PRODUCT"

  column {
    name = "PRODUCT_ID"
    type = "STRING"
  }

  column {
    name = "PRODUCT_NAME"
    type = "STRING"
  }

  column {
    name = "CATEGORY"
    type = "STRING"
  }
}