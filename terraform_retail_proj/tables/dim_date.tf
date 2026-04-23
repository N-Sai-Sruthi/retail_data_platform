resource "snowflake_table" "dim_date" {
  database = "GOLD_LAYER_DB"
  schema   = "GOLD"
  name     = "DIM_DATE"

  column {
    name = "ORDER_DATE"
    type = "DATE"
  }

  column {
    name = "DATE_KEY"
    type = "NUMBER"
  }

  column {
    name = "FULL_DATE"
    type = "DATE"
  }

  column {
    name = "DAY"
    type = "NUMBER"
  }

  column {
    name = "MONTH"
    type = "NUMBER"
  }

  column {
    name = "YEAR"
    type = "NUMBER"
  }

  column {
    name = "QUARTER"
    type = "NUMBER"
  }

  column {
    name = "DAY_OF_WEEK"
    type = "STRING"
  }

  column {
    name = "MONTH_NAME"
    type = "STRING"
  }
}