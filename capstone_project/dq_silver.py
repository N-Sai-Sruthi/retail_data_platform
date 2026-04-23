# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  NOTEBOOK 04b — DATA QUALITY: SILVER LAYER                              ║
# ║                                                                         ║
# ║  SILVER DQ CHECKS:                                                      ║
# ║                                                                         ║
# ║  TYPE SANITISATION (Bronze → Silver boundary):                         ║
# ║  S1.  customer_id / product_id / order_id   : StringType               ║
# ║  S2.  signup_date / order_date              : DateType (not String)     ║
# ║  S3.  price                                 : DoubleType, 2 decimal     ║
# ║  S4.  quantity                              : IntegerType               ║
# ║  S5.  total_amount                          : DoubleType, 2 decimal     ║
# ║  S6.  is_current                            : BooleanType               ║
# ║  S7.  NULL semantics: no empty string survives into typed silver cols   ║
# ║  S8.  Precision check: price / total_amount max 2 decimal places       ║
# ║                                                                         ║
# ║  CONTENT CHECKS:                                                        ║
# ║  S9.  No null PKs in current dimension rows                             ║
# ║  S10. No negative price in valid products                               ║
# ║  S11. SCD2 integrity — no customer/product with 2+ is_current=true rows ║
# ║  S12. No null total_amount in fact_sales_clean                          ║
# ║  S13. total_amount = quantity × price (tolerance 0.01)                  ║
# ║  S14. No duplicate order_ids in fact_sales_clean                        ║
# ║  S15. Referential integrity: fact_sales → dim_customer_scd2             ║
# ║  S16. Referential integrity: fact_sales → dim_product_scd2 (priced)     ║
# ║  S17. Drop rate: log % of bronze sales dropped due to missing price      ║
# ╚══════════════════════════════════════════════════════════════════════════╝
#
# ════════════════════════════════════════════════════════════════════════════
# TYPE SANITISATION AT SILVER — WHY THIS MATTERS
# ─────────────────────────────────────────────────────────────────────────
#
# BRONZE → SILVER is the most dangerous type boundary in the pipeline.
# Bronze is all StringType. Silver applies cast() operations.
# Silent failures to watch for:
#
#  1. STRING → DATE casting
#     "2024-01-15"      → 2024-01-15  (correct)
#     "15/01/2024"      → NULL        (wrong format — silently null)
#     "2024-1-5"        → NULL        (wrong format — silently null)
#     "NULL"            → NULL        (correct)
#     ""                → NULL        (correct — emptyValue=None handled in bronze)
#
#  2. STRING → DOUBLE casting (price)
#     "15.99"           → 15.99       (correct)
#     "15.990000"       → 15.99       (correct, precision preserved)
#     "15.9999999"      → 15.9999999  (PRECISION ISSUE for financial reporting)
#     "15,99"           → NULL        (European decimal separator — silent null)
#     "$15.99"          → NULL        (currency symbol — silent null)
#     ""                → NULL        (correct)
#
#  3. NULL vs EMPTY after casting
#     After cast("double"), NULL and failed-cast BOTH produce NULL.
#     DQ must verify: null total_amount in clean fact = 0 (none should slip through)
#
#  4. PRECISION for financial reporting
#     Power BI reports use total_amount for revenue.
#     15.9999999 vs 16.00 is a rounding error at transaction level.
#     At 2M rows this becomes a significant discrepancy.
#     DQ checks: round(price,2) == price for all price values.
#
#  5. BOOLEAN type (is_current)
#     SCD2 uses is_current for every filter. If it's StringType "true"/"false"
#     instead of BooleanType, filter("is_current = true") silently returns 0 rows.
# ════════════════════════════════════════════════════════════════════════════

import logging
import sys
from dataclasses import dataclass
from datetime import datetime
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StringType, DoubleType, IntegerType, DateType,
    BooleanType, TimestampType, LongType, FloatType
)

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════
CATALOG  = "retail_data_project"
SILVER   = f"{CATALOG}.02_silver"
METADATA = f"{CATALOG}.04_metadata"

DQ_TABLE         = f"{METADATA}.dq_results"
CUSTOMER_SILVER  = f"{SILVER}.dim_customer_scd2"
PRODUCT_SILVER   = f"{SILVER}.dim_product_scd2"
SALES_CLEAN      = f"{SILVER}.fact_sales_clean"
TOTAL_AMOUNT_TOLERANCE = 0.01   # |total_amount - qty*price| <= 0.01
PRECISION_DECIMAL_PLACES = 2    # financial columns must have ≤ 2 decimal places
DROP_WARN_RATE  = 0.10          # warn if >10% of bronze sales were dropped
DROP_FAIL_RATE  = 0.50          # fail  if >50% of bronze sales were dropped

# ══════════════════════════════════════════════════════════════════════════════
# LOGGER + DQ FRAMEWORK (same pattern as bronze)
# ══════════════════════════════════════════════════════════════════════════════
def get_logger(name):
    logger = logging.getLogger(name)
    if not logger.handlers:
        h = logging.StreamHandler(sys.stdout)
        h.setFormatter(logging.Formatter(
            "%(asctime)s | %(levelname)-8s | %(name)s | %(message)s",
            "%Y-%m-%d %H:%M:%S"
        ))
        logger.addHandler(h)
        logger.setLevel(logging.INFO)
    return logger

logger   = get_logger("04b_dq_silver")
RUN_TS   = datetime.now()
results  = []
failures = []

@dataclass
class DQCheck:
    check_id:  str
    layer:     str
    table:     str
    description: str
    severity:  str   = "CRITICAL"
    threshold: float = 0.0

def run_check(check, failed_count, total_count, details):
    rate    = failed_count / total_count if total_count > 0 else 0
    exceeds = rate > check.threshold
    if not exceeds:
        status = "PASS"
    elif check.severity == "CRITICAL":
        status = "CRITICAL"
        failures.append(f"[CRITICAL][{check.layer}] {check.table}.{check.check_id}: {details}")
    else:
        status = "WARNING"
    icon = {"PASS": "✓", "WARNING": "⚠", "CRITICAL": "✗"}[status]
    logger.log(
        logging.ERROR if status == "CRITICAL" else
        logging.WARNING if status == "WARNING" else logging.INFO,
        f"  {icon} {check.check_id} | {check.table} | {details}"
    )
    results.append((
        RUN_TS, "silver", check.layer, check.table, check.check_id,
        check.description, status, int(failed_count), int(total_count),
        round(rate * 100, 4), details
    ))


# ══════════════════════════════════════════════════════════════════════════════
# LOAD SILVER TABLES
# ══════════════════════════════════════════════════════════════════════════════
df_cust      = spark.table(CUSTOMER_SILVER)
df_prod      = spark.table(PRODUCT_SILVER)
df_clean     = spark.table(SALES_CLEAN)

df_cust_curr   = df_cust.filter("is_current = true")
df_prod_curr   = df_prod.filter("is_current = true")
df_prod_priced = df_prod_curr.filter(F.col("price").isNotNull()).filter(F.col("price") > 0)

# Count bronze sales total to compute drop rate in S17
df_sales_bronze = spark.table(f"{CATALOG}.01_bronze.sales")
bronze_sales_total = df_sales_bronze.count()

logger.info(f"Silver loaded:")
logger.info(f"  dim_customer_scd2 (current) : {df_cust_curr.count():,}")
logger.info(f"  dim_product_scd2  (current) : {df_prod_curr.count():,}")
logger.info(f"  fact_sales_clean            : {df_clean.count():,}")
logger.info(f"  bronze sales total          : {bronze_sales_total:,} (used for S17 drop rate)")


# ══════════════════════════════════════════════════════════════════════════════
# TYPE SANITISATION: Bronze → Silver boundary
# ──────────────────────────────────────────────────────────────────────────
# Verifies every column that was StringType in Bronze is now the correct
# typed column in Silver. Mismatch here = cast() failed silently or was skipped.
# ══════════════════════════════════════════════════════════════════════════════

# Expected schema: {column_name: expected_type_class}
CUSTOMER_EXPECTED_TYPES = {
    "customer_id":  StringType,     # stays String — natural key
    "name":         StringType,
    "email_masked": StringType,
    "city":         StringType,
    "signup_date":  DateType,       # was String in Bronze
    "is_current":   BooleanType,    # added by pipeline
    "start_date":   DateType,
    "end_date":     DateType,
}

PRODUCT_EXPECTED_TYPES = {
    "product_id":   StringType,
    "product_name": StringType,
    "category":     StringType,
    "price":        DoubleType,     # was String in Bronze
    "is_current":   BooleanType,
}

FACT_EXPECTED_TYPES = {
    "order_id":     StringType,
    "customer_id":  StringType,
    "product_id":   StringType,
    "quantity":     IntegerType,    # was String in Bronze
    "price":        DoubleType,     # was String in Bronze
    "order_date":   DateType,       # was String in Bronze
    "total_amount": DoubleType,     # computed in Silver
}

logger.info("─" * 60)
logger.info("S1-S6: Type sanitisation (Bronze→Silver casting verification)")

for df, tname, expected_types in [
    (df_cust_curr,  "dim_customer_scd2", CUSTOMER_EXPECTED_TYPES),
    (df_prod_curr,  "dim_product_scd2",  PRODUCT_EXPECTED_TYPES),
    (df_clean,      "fact_sales_clean",  FACT_EXPECTED_TYPES),
]:
    wrong = []
    for col_name, expected_cls in expected_types.items():
        if col_name in df.columns:
            actual = df.schema[col_name].dataType
            if not isinstance(actual, expected_cls):
                wrong.append(
                    f"{col_name}: expected={expected_cls.__name__} "
                    f"actual={actual.simpleString()}"
                )
        else:
            wrong.append(f"{col_name}: COLUMN MISSING")

    run_check(
        DQCheck("S1_type_sanitisation", "silver", tname,
                "All columns must have correct types after Bronze→Silver cast",
                "CRITICAL", threshold=0.0),
        failed_count = len(wrong), total_count = len(expected_types),
        details = (
            f"Type mismatches: {wrong}" if wrong
            else f"All {len(expected_types)} column types correct ✓"
        )
    )


# ══════════════════════════════════════════════════════════════════════════════
# S7 — NULL SEMANTICS: No empty string in typed columns
# ──────────────────────────────────────────────────────────────────────────
# After casting, typed columns (DateType, DoubleType, IntegerType) cannot
# contain empty strings — they would have become NULL during cast.
# This checks the STRING columns in silver for any remaining empty strings
# that Silver's trim/clean operations should have eliminated.
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("S7: NULL semantics — no empty strings in Silver string columns")

for df, tname in [
    (df_cust_curr, "dim_customer_scd2"),
    (df_prod_curr, "dim_product_scd2"),
    (df_clean,     "fact_sales_clean"),
]:
    total = df.count()
    str_cols = [f.name for f in df.schema.fields
                if isinstance(f.dataType, StringType)
                and f.name not in {"source_file", "change_hash"}]

    empty_count = 0
    empty_cols  = []
    for c in str_cols:
        n = df.filter(F.col(c) == "").count()
        if n > 0:
            empty_count += n
            empty_cols.append(f"{c}:{n}")

    run_check(
        DQCheck("S7_empty_string_in_silver", "silver", tname,
                "No empty strings should exist in Silver string columns after trim()",
                "WARNING", threshold=0.001),
        failed_count = empty_count, total_count = max(total, 1),
        details = (
            f"Empty strings in: {empty_cols}" if empty_cols
            else "No empty strings ✓"
        )
    )


# ══════════════════════════════════════════════════════════════════════════════
# S8 — PRECISION CHECK: price and total_amount must have ≤ 2 decimal places
# ──────────────────────────────────────────────────────────────────────────
# For financial reporting, price=15.9999999 causes:
#   1. Revenue discrepancies vs source system (different rounding)
#   2. Power BI showing inconsistent decimal display
#   3. SUM(total_amount) differing from external audits by small amounts
#
# Check: round(price, 2) == price for all non-null price values.
# Same for total_amount.
#
# NOTE: This is a WARNING, not CRITICAL.
# We don't fail the pipeline but the ops team must decide if source
# prices are intentionally high-precision (e.g. commodity trading) or
# this is a data quality issue.
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("S8: Precision check — price/total_amount max 2 decimal places")

# dim_product price precision
df_prod_with_price = df_prod_curr.filter(F.col("price").isNotNull())
prod_total = df_prod_with_price.count()

price_over_precision = df_prod_with_price.filter(
    F.round(F.col("price"), PRECISION_DECIMAL_PLACES) != F.col("price")
).count()

run_check(
    DQCheck("S8a_price_precision", "silver", "dim_product_scd2",
            f"price must have <= {PRECISION_DECIMAL_PLACES} decimal places for financial reporting",
            "WARNING", threshold=0.005),   # warn if >0.5% have excess precision
    failed_count = price_over_precision, total_count = max(prod_total, 1),
    details = (
        f"{price_over_precision:,} products with price > {PRECISION_DECIMAL_PLACES} decimal places "
        f"(e.g. 15.9999999 instead of 16.00) — may cause revenue discrepancies"
        if price_over_precision > 0
        else f"All prices have <= {PRECISION_DECIMAL_PLACES} decimal places ✓"
    )
)

# fact_sales total_amount precision
df_clean_with_amount = df_clean.filter(F.col("total_amount").isNotNull())
clean_total = df_clean_with_amount.count()

amount_over_precision = df_clean_with_amount.filter(
    F.round(F.col("total_amount"), PRECISION_DECIMAL_PLACES) != F.col("total_amount")
).count()

run_check(
    DQCheck("S8b_total_amount_precision", "silver", "fact_sales_clean",
            f"total_amount must have <= {PRECISION_DECIMAL_PLACES} decimal places",
            "WARNING", threshold=0.005),
    failed_count = amount_over_precision, total_count = max(clean_total, 1),
    details = (
        f"{amount_over_precision:,} rows with total_amount > "
        f"{PRECISION_DECIMAL_PLACES} decimal places"
        if amount_over_precision > 0
        else f"All total_amount values have <= {PRECISION_DECIMAL_PLACES} decimal places ✓"
    )
)


# ══════════════════════════════════════════════════════════════════════════════
# S9 — NO NULL PRIMARY KEYS IN CURRENT DIMENSION ROWS
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("S9: No null PKs in current dimension rows")

for df, pk, tname in [
    (df_cust_curr, "customer_id", "dim_customer_scd2"),
    (df_prod_curr, "product_id",  "dim_product_scd2"),
    (df_clean,     "order_id",    "fact_sales_clean"),
]:
    total = df.count()
    nulls = df.filter(F.col(pk).isNull()).count()
    run_check(
        DQCheck("S9_null_pk", "silver", tname,
                f"No null {pk} in current rows", "CRITICAL"),
        failed_count=nulls, total_count=max(total,1),
        details=f"{nulls:,} null {pk} values"
    )


# ══════════════════════════════════════════════════════════════════════════════
# S10 — NO NEGATIVE PRICE IN PRICED PRODUCTS
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("S10: No negative price")

total = df_prod_curr.filter(F.col("price").isNotNull()).count()
neg   = df_prod_curr.filter(F.col("price").isNotNull()).filter(F.col("price") <= 0).count()
run_check(
    DQCheck("S10_negative_price", "silver", "dim_product_scd2",
            "No product with price <= 0 (NULL price means related sales rows are dropped)",
            "CRITICAL"),
    failed_count=neg, total_count=max(total,1),
    details=f"{neg:,} products with price <= 0"
)


# ══════════════════════════════════════════════════════════════════════════════
# S11 — SCD2 INTEGRITY
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("S11: SCD2 integrity — no entity with 2+ is_current=true rows")

from pyspark.sql.functions import count as spark_count

for df, pk, tname in [
    (df_cust, "customer_id", "dim_customer_scd2"),
    (df_prod, "product_id",  "dim_product_scd2"),
]:
    multi = (
        df.filter("is_current = true")
        .groupBy(pk).agg(spark_count("*").alias("cnt"))
        .filter(F.col("cnt") > 1).count()
    )
    run_check(
        DQCheck("S11_scd2_integrity", "silver", tname,
                f"No {pk} with multiple is_current=true rows",
                "CRITICAL"),
        failed_count=multi, total_count=1,
        details=f"{multi:,} {pk}s with 2+ is_current=true rows (SCD2 MERGE bug)"
    )


# ══════════════════════════════════════════════════════════════════════════════
# S12 — NO NULL total_amount IN CLEAN FACT
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("S12: No null total_amount in clean fact")

total  = df_clean.count()
null_t = df_clean.filter(F.col("total_amount").isNull()).count()
run_check(
    DQCheck("S12_null_total_amount", "silver", "fact_sales_clean",
            "No null total_amount in fact_sales_clean — silver drop gate must catch these",
            "CRITICAL"),
    failed_count=null_t, total_count=max(total,1),
    details=f"{null_t:,} rows with null total_amount (silver product gate failed to drop these)"
)


# ══════════════════════════════════════════════════════════════════════════════
# S13 — total_amount ACCURACY: |total - qty*price| <= tolerance
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("S13: total_amount accuracy")

wrong_calc = (
    df_clean
    .filter(F.col("total_amount").isNotNull())
    .filter(F.col("price").isNotNull())
    .filter(
        F.abs(F.col("total_amount") - (F.col("quantity") * F.col("price")))
        > TOTAL_AMOUNT_TOLERANCE
    ).count()
)
clean_total = df_clean.count()
run_check(
    DQCheck("S13_total_amount_calc", "silver", "fact_sales_clean",
            f"|total_amount - qty×price| must be <= {TOTAL_AMOUNT_TOLERANCE}",
            "CRITICAL"),
    failed_count=wrong_calc, total_count=max(clean_total,1),
    details=f"{wrong_calc:,} rows with |total_amount - qty×price| > {TOTAL_AMOUNT_TOLERANCE}"
)


# ══════════════════════════════════════════════════════════════════════════════
# S14 — NO DUPLICATE order_ids IN CLEAN FACT
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("S14: No duplicate order_ids in clean fact")

total  = df_clean.count()
unique = df_clean.select("order_id").distinct().count()
dups   = total - unique
run_check(
    DQCheck("S14_duplicate_order_ids", "silver", "fact_sales_clean",
            "No duplicate order_ids (insert-only MERGE should prevent this)",
            "CRITICAL"),
    failed_count=dups, total_count=max(total,1),
    details=f"{dups:,} duplicate order_ids ({total:,} total, {unique:,} unique)"
)


# ══════════════════════════════════════════════════════════════════════════════
# S15/S16 — REFERENTIAL INTEGRITY
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("S15-S16: Referential integrity")

# S15: clean fact → dim_customer
orphan_cust = (
    df_clean.join(df_cust_curr.select("customer_id"),
                  on="customer_id", how="left_anti").count()
)
run_check(
    DQCheck("S15_ri_customer", "silver", "fact_sales_clean → dim_customer",
            "Every customer_id in clean fact must exist in current dim_customer",
            "CRITICAL"),
    failed_count=orphan_cust, total_count=max(df_clean.count(),1),
    details=f"{orphan_cust:,} orphan customer_ids"
)

# S16: clean fact → priced dim_product
orphan_prod = (
    df_clean.join(df_prod_priced.select("product_id"),
                  on="product_id", how="left_anti").count()
)
run_check(
    DQCheck("S16_ri_product", "silver", "fact_sales_clean → dim_product_priced",
            "Every product_id in clean fact must reference a priced product",
            "CRITICAL"),
    failed_count=orphan_prod, total_count=max(df_clean.count(),1),
    details=f"{orphan_prod:,} orphan product_ids (referencing unpriced or missing products)"
)


# ══════════════════════════════════════════════════════════════════════════════
# S17 — SALES DROP RATE MONITORING
# ──────────────────────────────────────────────────────────────────────────
# Compares fact_sales_clean row count against bronze sales total.
# Dropped rows = bronze sales that were removed because their product_id had
# no valid price in dim_product_scd2.
#
# WARN  if drop rate > 10%  → more products than expected have missing prices
# FAIL  if drop rate > 50%  → majority of sales are lost — upstream data issue
#
# Rising drop rate week-over-week = new products arriving without prices.
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("S17: Sales drop rate — bronze vs fact_sales_clean")

clean_count  = df_clean.count()
dropped      = max(bronze_sales_total - clean_count, 0)
drop_rate    = dropped / bronze_sales_total if bronze_sales_total > 0 else 0

sev = "CRITICAL" if drop_rate > DROP_FAIL_RATE else "WARNING"
thr = DROP_FAIL_RATE if drop_rate > DROP_FAIL_RATE else DROP_WARN_RATE

run_check(
    DQCheck("S17_sales_drop_rate", "silver", "fact_sales_clean",
            f"Drop rate: WARN>{DROP_WARN_RATE:.0%}, FAIL>{DROP_FAIL_RATE:.0%}. "
            f"Dropped = sales with no valid product price.",
            sev, thr),
    failed_count=dropped, total_count=max(bronze_sales_total, 1),
    details=(
        f"{dropped:,} sales dropped ({drop_rate:.1%} of {bronze_sales_total:,} bronze rows) | "
        f"{clean_count:,} rows in fact_sales_clean | "
        + ("CRITICAL — majority of sales lost" if drop_rate > DROP_FAIL_RATE
           else "HIGH — many products missing prices" if drop_rate > DROP_WARN_RATE
           else "OK")
    )
)

# Log which products are causing drops (products with no valid price)
logger.info("S17: Products with no valid price (causing sales drops):")
(
    df_prod_curr
    .filter(F.col("price").isNull() | (F.col("price") <= 0))
    .select("product_id", "product_name", "category", "price")
    .orderBy("product_id")
    .show(20, truncate=60)
)


# ══════════════════════════════════════════════════════════════════════════════
# WRITE + GATE
# ══════════════════════════════════════════════════════════════════════════════
if results:
    from pyspark.sql.types import StructType, StructField
    schema = StructType([
        StructField("run_timestamp",  TimestampType(), False),
        StructField("notebook",       StringType(),    False),
        StructField("layer",          StringType(),    False),
        StructField("table_name",     StringType(),    False),
        StructField("check_id",       StringType(),    False),
        StructField("description",    StringType(),    True),
        StructField("status",         StringType(),    False),
        StructField("failed_count",   LongType(),      False),
        StructField("total_count",    LongType(),      False),
        StructField("fail_rate_pct",  FloatType(),     True),
        StructField("details",        StringType(),    True),
    ])
    spark.createDataFrame(results, schema=schema) \
        .write.format("delta").mode("append").saveAsTable(DQ_TABLE)
    logger.info(f"DQ results written → {DQ_TABLE}")

total_c  = len(results)
pass_c   = sum(1 for r in results if r[6] == "PASS")
warn_c   = sum(1 for r in results if r[6] == "WARNING")
fail_c   = len(failures)
duration = (datetime.now() - RUN_TS).seconds

logger.info(f"{'═'*60}")
logger.info(f"  SILVER DQ SUMMARY")
logger.info(f"{'═'*60}")
logger.info(f"  Total   : {total_c}")
logger.info(f"  Passed  : {pass_c}")
logger.info(f"  Warnings: {warn_c}")
logger.info(f"  Critical: {fail_c}")
logger.info(f"  Duration: {duration}s")
logger.info(f"{'═'*60}")

if failures:
    for f in failures:
        logger.error(f"  FAILED: {f}")
    raise Exception(
        f"SILVER DQ GATE FAILED — {fail_c} critical check(s).\n"
        + "\n".join(failures)
    )
else:
    logger.info("Silver DQ gate PASSED \u2713 — Gold layer may proceed")