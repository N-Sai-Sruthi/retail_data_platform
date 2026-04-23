# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  NOTEBOOK 04c — DATA QUALITY: GOLD LAYER                                ║
# ║                                                                         ║
# ║  GOLD DQ CHECKS:                                                        ║
# ║                                                                         ║
# ║  TYPE SANITISATION (Silver → Gold boundary):                           ║
# ║  G1.  fact_sales column types (date_key=INT, total_amount=DOUBLE etc)  ║
# ║  G2.  dim_date column types (date_key=INT, year/month/day=INT etc)      ║
# ║  G3.  date_key format: must be YYYYMMDD integer (8 digits, valid range) ║
# ║  G4.  total_amount precision: <= 2 decimal places for Power BI          ║
# ║  G5.  NULL semantics: no empty strings anywhere in gold                 ║
# ║                                                                         ║
# ║  CONTENT CHECKS:                                                        ║
# ║  G6.  Row count: fact_sales must match fact_sales_clean in Silver       ║
# ║  G7.  No duplicate order_ids in fact_sales                              ║
# ║  G8.  No null customer_id / product_id / date_key in fact              ║
# ║  G9.  Referential integrity: fact → dim_customer (by customer_id)      ║
# ║  G10. Referential integrity: fact → dim_product (by product_id)        ║
# ║  G11. Referential integrity: fact → dim_date (by date_key)             ║
# ║  G12. No negative total_amount                                          ║
# ║  G13. dim_date completeness: all dates in fact exist in dim_date        ║
# ║  G14. date_key consistency: date_key == date_format(order_date)         ║
# ╚══════════════════════════════════════════════════════════════════════════╝
#
# ════════════════════════════════════════════════════════════════════════════
# TYPE SANITISATION AT GOLD — WHY IT MATTERS
# ─────────────────────────────────────────────────────────────────────────
#
# Gold is read directly by Power BI. Type issues here appear as:
#   - "Text" columns in Power BI where you expected "Number"
#   - Date columns that Power BI treats as text (can't use date slicer)
#   - Integer columns with decimal precision (looks wrong in visuals)
#
# SILVER → GOLD type boundary issues:
#
#  1. date_key must be INT (not STRING, not BIGINT)
#     Gold build computes: F.date_format("order_date","yyyyMMdd").cast("int")
#     If cast fails → NULL date_key → Power BI "date" dimension breaks
#     Range: 20200101 to 20301231 (valid YYYYMMDD). Out-of-range = data error.
#
#  2. total_amount precision in Gold
#     Silver compute: quantity * price (both may have many decimal places)
#     Gold should round to 2 decimals for consistent reporting.
#     DQ checks that all gold total_amount values are <= 2 decimal places.
#     If not → gold notebook should apply F.round(total_amount, 2) before write.
#
#  3. NULL semantics in Gold
#     Gold is analytics-ready. NULL customer_id in fact = missing dimension.
#     Power BI shows "(Blank)" for that row in every customer-related visual.
#     Zero tolerance for NULLs in foreign key columns.
#
#  4. dim_date must cover all dates in fact_sales
#     Gold build derives dim_date from fact_sales dates.
#     If a date in fact has no row in dim_date, the date slicer in Power BI
#     will silently exclude those orders from time-based aggregations.
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
GOLD     = f"{CATALOG}.03_gold"
METADATA = f"{CATALOG}.04_metadata"

DQ_TABLE     = f"{METADATA}.dq_results"
FACT_SALES   = f"{GOLD}.fact_sales"
DIM_CUSTOMER = f"{GOLD}.dim_customer"
DIM_PRODUCT  = f"{GOLD}.dim_product"
DIM_DATE     = f"{GOLD}.dim_date"
SALES_CLEAN  = f"{SILVER}.fact_sales_clean"

PRECISION_DP   = 2        # max decimal places for financial cols
DATE_KEY_MIN   = 20190101 # earliest acceptable date_key
DATE_KEY_MAX   = 20991231 # latest acceptable date_key

# ══════════════════════════════════════════════════════════════════════════════
# LOGGER + DQ FRAMEWORK
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

logger   = get_logger("04c_dq_gold")
RUN_TS   = datetime.now()
results  = []
failures = []

@dataclass
class DQCheck:
    check_id:    str
    layer:       str
    table:       str
    description: str
    severity:    str   = "CRITICAL"
    threshold:   float = 0.0

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
        RUN_TS, "gold", check.layer, check.table, check.check_id,
        check.description, status, int(failed_count), int(total_count),
        round(rate * 100, 4), details
    ))


# ══════════════════════════════════════════════════════════════════════════════
# LOAD GOLD TABLES
# ══════════════════════════════════════════════════════════════════════════════
df_fact    = spark.table(FACT_SALES)
df_cust    = spark.table(DIM_CUSTOMER)
df_prod    = spark.table(DIM_PRODUCT)
df_date    = spark.table(DIM_DATE)
df_silver_clean = spark.table(SALES_CLEAN)

logger.info(f"Gold loaded:")
logger.info(f"  fact_sales   : {df_fact.count():,}")
logger.info(f"  dim_customer : {df_cust.count():,}")
logger.info(f"  dim_product  : {df_prod.count():,}")
logger.info(f"  dim_date     : {df_date.count():,}")


# ══════════════════════════════════════════════════════════════════════════════
# G1 — TYPE SANITISATION: fact_sales column types
# ──────────────────────────────────────────────────────────────────────────
# fact_sales in Gold has different types from Silver.
# Silver fact_sales_clean has:
#   order_date (DATE), price (DOUBLE)
# Gold fact_sales has:
#   date_key (INT) — derived from order_date
#   price dropped (it's in dim_product)
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("G1: Type sanitisation — fact_sales column types")

FACT_EXPECTED = {
    "order_id":     StringType,
    "customer_id":  StringType,
    "product_id":   StringType,
    "date_key":     IntegerType,   # ← was DateType in silver, now INT YYYYMMDD
    "quantity":     IntegerType,
    "total_amount": DoubleType,
}

wrong = []
for col_name, expected_cls in FACT_EXPECTED.items():
    if col_name in df_fact.columns:
        actual = df_fact.schema[col_name].dataType
        if not isinstance(actual, expected_cls):
            wrong.append(f"{col_name}: expected={expected_cls.__name__} actual={actual.simpleString()}")
    else:
        wrong.append(f"{col_name}: COLUMN MISSING")

run_check(
    DQCheck("G1_fact_type_sanitisation", "gold", "fact_sales",
            "All fact_sales columns must have correct Gold types",
            "CRITICAL", threshold=0.0),
    failed_count=len(wrong), total_count=len(FACT_EXPECTED),
    details=(f"Type mismatches: {wrong}" if wrong else f"All fact_sales types correct ✓")
)


# ══════════════════════════════════════════════════════════════════════════════
# G2 — TYPE SANITISATION: dim_date column types
# ──────────────────────────────────────────════════════════════════════════
# dim_date is entirely derived in Gold. Column types must match what
# Power BI expects for its Date Intelligence functions.
# year, month, day, quarter must be INT (not BIGINT) for Power BI compatibility.
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("G2: Type sanitisation — dim_date column types")

DATE_EXPECTED = {
    "date_key":    IntegerType,   # YYYYMMDD integer
    "full_date":   DateType,      # actual DATE type
    "day":         IntegerType,
    "month":       IntegerType,
    "year":        IntegerType,
    "quarter":     IntegerType,
    "day_of_week": StringType,    # "Monday" etc
    "month_name":  StringType,    # "January" etc
}

wrong = []
for col_name, expected_cls in DATE_EXPECTED.items():
    if col_name in df_date.columns:
        actual = df_date.schema[col_name].dataType
        if not isinstance(actual, expected_cls):
            wrong.append(f"{col_name}: expected={expected_cls.__name__} actual={actual.simpleString()}")
    else:
        wrong.append(f"{col_name}: COLUMN MISSING")

run_check(
    DQCheck("G2_dim_date_types", "gold", "dim_date",
            "All dim_date columns must have correct types for Power BI date intelligence",
            "CRITICAL", threshold=0.0),
    failed_count=len(wrong), total_count=len(DATE_EXPECTED),
    details=(f"Type mismatches: {wrong}" if wrong else "All dim_date types correct ✓")
)


# ══════════════════════════════════════════════════════════════════════════════
# G3 — date_key FORMAT VALIDATION
# ──────────────────────────────────────────────────────────────────────────
# date_key is computed as: date_format("order_date","yyyyMMdd").cast("int")
# Valid range: DATE_KEY_MIN to DATE_KEY_MAX
# Invalid date_keys: NULL (cast failed), 0, negative, out of range
# Also verify: digit count is exactly 8 (YYYYMMDD)
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("G3: date_key format validation (must be valid YYYYMMDD integer)")

total = df_fact.count()

# Null date_keys
null_dk = df_fact.filter(F.col("date_key").isNull()).count()
run_check(
    DQCheck("G3a_date_key_null", "gold", "fact_sales",
            "No null date_key — every sale must map to a date", "CRITICAL"),
    failed_count=null_dk, total_count=max(total,1),
    details=f"{null_dk:,} null date_keys"
)

# Out of range date_keys
oor_dk = df_fact.filter(F.col("date_key").isNotNull()).filter(
    (F.col("date_key") < DATE_KEY_MIN) | (F.col("date_key") > DATE_KEY_MAX)
).count()
run_check(
    DQCheck("G3b_date_key_range", "gold", "fact_sales",
            f"date_key must be between {DATE_KEY_MIN} and {DATE_KEY_MAX}",
            "CRITICAL"),
    failed_count=oor_dk, total_count=max(total,1),
    details=f"{oor_dk:,} date_keys outside valid YYYYMMDD range"
)

# date_key digit count ≠ 8 (e.g. 2024115 instead of 20240115)
wrong_digits = df_date.filter(
    F.length(F.col("date_key").cast("string")) != 8
).count()
run_check(
    DQCheck("G3c_date_key_digits", "gold", "dim_date",
            "date_key must be exactly 8 digits (YYYYMMDD format)",
            "CRITICAL"),
    failed_count=wrong_digits, total_count=max(df_date.count(),1),
    details=f"{wrong_digits:,} date_keys with != 8 digits"
)


# ══════════════════════════════════════════════════════════════════════════════
# G4 — PRECISION: total_amount must have <= 2 decimal places
# ──────────────────────────────────────────────────────────────────────────
# Power BI revenue visuals should show currency values.
# 1897159.4289999 displayed as revenue is confusing.
# Gold should store total_amount with max 2 decimal places.
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("G4: Precision — total_amount <= 2 decimal places")

df_with_amount = df_fact.filter(F.col("total_amount").isNotNull())
total_with     = df_with_amount.count()

over_precision = df_with_amount.filter(
    F.round(F.col("total_amount"), PRECISION_DP) != F.col("total_amount")
).count()

run_check(
    DQCheck("G4_total_amount_precision", "gold", "fact_sales",
            f"total_amount must have <= {PRECISION_DP} decimal places for Power BI reporting",
            "WARNING", threshold=0.005),
    failed_count=over_precision, total_count=max(total_with,1),
    details=(
        f"{over_precision:,} rows with total_amount > {PRECISION_DP} decimal places — "
        f"apply F.round(total_amount,{PRECISION_DP}) in gold build notebook"
        if over_precision > 0
        else f"All total_amount values have <= {PRECISION_DP} decimal places ✓"
    )
)


# ══════════════════════════════════════════════════════════════════════════════
# G5 — NULL SEMANTICS: No empty strings in Gold
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("G5: No empty strings in Gold tables")

for df, tname in [(df_fact,"fact_sales"),(df_cust,"dim_customer"),(df_prod,"dim_product")]:
    total   = df.count()
    str_cols = [f.name for f in df.schema.fields if isinstance(f.dataType, StringType)]
    empty_n  = 0
    empty_c  = []
    for c in str_cols:
        n = df.filter(F.col(c) == "").count()
        if n > 0:
            empty_n += n
            empty_c.append(f"{c}:{n}")
    run_check(
        DQCheck("G5_empty_strings", "gold", tname,
                "No empty strings in Gold — analytics layer must have clean values",
                "WARNING", threshold=0.0),
        failed_count=empty_n, total_count=max(total,1),
        details=(f"Empty strings in: {empty_c}" if empty_c else "No empty strings ✓")
    )


# ══════════════════════════════════════════════════════════════════════════════
# G6 — ROW COUNT: fact_sales must match silver fact_sales_clean
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("G6: Row count — gold fact must match silver clean fact")

silver_n = df_silver_clean.count()
gold_n   = df_fact.count()
diff     = abs(silver_n - gold_n)

run_check(
    DQCheck("G6_row_count_match", "gold", "fact_sales",
            "Gold fact_sales must have same rows as silver fact_sales_clean",
            "CRITICAL"),
    failed_count=diff, total_count=max(silver_n,1),
    details=f"Silver clean={silver_n:,} | Gold={gold_n:,} | Diff={diff:,}"
)


# ══════════════════════════════════════════════════════════════════════════════
# G7 — NO DUPLICATE order_ids
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("G7: No duplicate order_ids in gold fact")

total  = df_fact.count()
unique = df_fact.select("order_id").distinct().count()
dups   = total - unique
run_check(
    DQCheck("G7_duplicate_order_ids", "gold", "fact_sales", "No duplicate order_ids",
            "CRITICAL"),
    failed_count=dups, total_count=max(total,1),
    details=f"{dups:,} duplicate order_ids"
)


# ══════════════════════════════════════════════════════════════════════════════
# G8 — NO NULL FK COLUMNS IN FACT
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("G8: No null FK columns in fact_sales")

for fk_col in ["customer_id", "product_id", "date_key"]:
    n = df_fact.filter(F.col(fk_col).isNull()).count()
    run_check(
        DQCheck(f"G8_null_{fk_col}", "gold", "fact_sales",
                f"No null {fk_col} in fact (null FK = broken dimension join)",
                "CRITICAL"),
        failed_count=n, total_count=max(total,1),
        details=f"{n:,} null {fk_col} values"
    )


# ══════════════════════════════════════════════════════════════════════════════
# G9-G11 — REFERENTIAL INTEGRITY (by natural keys, gold uses natural keys)
# ──────────────────────────────────────────────────────────────────────────
# Gold notebook uses customer_id / product_id as join keys (not surrogate keys).
# Every FK in fact must resolve to a row in its dimension.
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("G9-G11: Referential integrity")

fact_total = df_fact.count()

# G9: fact → dim_customer
orphan_c = df_fact.join(df_cust.select("customer_id"),
                        on="customer_id", how="left_anti").count()
run_check(
    DQCheck("G9_ri_customer", "gold", "fact_sales → dim_customer",
            "Every customer_id in fact must exist in dim_customer", "CRITICAL"),
    failed_count=orphan_c, total_count=max(fact_total,1),
    details=f"{orphan_c:,} orphan customer_ids (Power BI shows (Blank) for these)"
)

# G10: fact → dim_product
orphan_p = df_fact.join(df_prod.select("product_id"),
                        on="product_id", how="left_anti").count()
run_check(
    DQCheck("G10_ri_product", "gold", "fact_sales → dim_product",
            "Every product_id in fact must exist in dim_product", "CRITICAL"),
    failed_count=orphan_p, total_count=max(fact_total,1),
    details=f"{orphan_p:,} orphan product_ids"
)

# G11: fact → dim_date
orphan_d = df_fact.filter(F.col("date_key").isNotNull()) \
    .join(df_date.select("date_key"), on="date_key", how="left_anti").count()
run_check(
    DQCheck("G11_ri_date", "gold", "fact_sales → dim_date",
            "Every date_key in fact must exist in dim_date", "CRITICAL"),
    failed_count=orphan_d, total_count=max(fact_total,1),
    details=f"{orphan_d:,} date_keys in fact not found in dim_date"
)


# ══════════════════════════════════════════════════════════════════════════════
# G12 — NO NEGATIVE total_amount
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("G12: No negative total_amount")

neg = df_fact.filter(F.col("total_amount").isNotNull()).filter(F.col("total_amount") <= 0).count()
run_check(
    DQCheck("G12_negative_total", "gold", "fact_sales",
            "No negative or zero total_amount in gold", "CRITICAL"),
    failed_count=neg, total_count=max(fact_total,1),
    details=f"{neg:,} rows with total_amount <= 0"
)


# ══════════════════════════════════════════════════════════════════════════════
# G13 — DIM_DATE COMPLETENESS
# ──────────────────────────────────────────────────────────────────────────
# All unique dates in fact_sales must have a row in dim_date.
# Gold build derives dim_date from fact_sales — they should always match.
# If they don't, a partial dim_date build happened.
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("G13: dim_date completeness — all fact dates must exist in dim_date")

missing_dates = (
    df_fact.filter(F.col("date_key").isNotNull())
    .select("date_key").distinct()
    .join(df_date.select("date_key"), on="date_key", how="left_anti")
    .count()
)
run_check(
    DQCheck("G13_dim_date_completeness", "gold", "dim_date",
            "dim_date must contain a row for every date in fact_sales",
            "CRITICAL"),
    failed_count=missing_dates, total_count=max(df_fact.select("date_key").distinct().count(),1),
    details=(
        f"{missing_dates:,} distinct date_keys in fact have no dim_date row — "
        f"Power BI date slicer will exclude these orders"
        if missing_dates > 0 else "All fact dates covered in dim_date ✓"
    )
)


# ══════════════════════════════════════════════════════════════════════════════
# G14 — date_key CONSISTENCY
# ──────────────────────────────────────────────────────────────────────────
# In Silver, order_date is a DATE column.
# In Gold, date_key = date_format(order_date,"yyyyMMdd").cast("int")
# Verify: re-derive date_key from full_date in dim_date and compare.
# If they differ, the date_key formula has a timezone or format issue.
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("G14: date_key consistency — verify YYYYMMDD derivation is correct")

wrong_dk = df_date.filter(
    F.col("date_key") != F.date_format("full_date", "yyyyMMdd").cast("int")
).count()

run_check(
    DQCheck("G14_date_key_consistency", "gold", "dim_date",
            "date_key must equal date_format(full_date,'yyyyMMdd').cast('int')",
            "CRITICAL"),
    failed_count=wrong_dk, total_count=max(df_date.count(),1),
    details=(
        f"{wrong_dk:,} rows where date_key != YYYYMMDD(full_date) — "
        f"timezone or formula issue in gold build"
        if wrong_dk > 0 else "date_key derivation correct ✓"
    )
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
logger.info(f"  GOLD DQ SUMMARY")
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
        f"GOLD DQ GATE FAILED — {fail_c} critical check(s).\n"
        + "\n".join(failures)
    )
else:
    logger.info("Gold DQ gate PASSED ✓ — Pipeline complete, dashboards may refresh")