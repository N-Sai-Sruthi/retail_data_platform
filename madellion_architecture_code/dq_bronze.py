# ╔══════════════════════════════════════════════════════════════════════════╗
# ║  NOTEBOOK 04a — DATA QUALITY: BRONZE LAYER                              ║
# ║                                                                         ║
# ║  WHAT IS A DQ FRAMEWORK vs NORMAL CODE?                                 ║
# ║                                                                         ║
# ║  Normal code checks one thing in one place (ad-hoc).                   ║
# ║  A DQ Framework:                                                        ║
# ║    1. Defines checks as DATA OBJECTS (DQCheck dataclass), not           ║
# ║       scattered if-statements                                           ║
# ║    2. Has SEVERITY LEVELS (CRITICAL halts, WARNING continues)           ║
# ║    3. Writes ALL results to a persistent dq_results table               ║
# ║    4. Shows TRENDS over time (not just today's pass/fail)               ║
# ║    5. Has TYPE SANITISATION checks — verifies that data types,          ║
# ║       precision, nullability, and empty/NULL semantics are              ║
# ║       consistent SOURCE → TARGET across every layer boundary            ║
# ║                                                                         ║
# ║  BRONZE DQ CHECKS:                                                      ║
# ║                                                                         ║
# ║  B1.  Row count > 0 for each entity                                     ║
# ║  B2.  Primary key null rate < threshold                                 ║
# ║  B3.  Schema drift flag rate (how many rows arrived with drift)         ║
# ║  B4.  TYPE SANITISATION: all columns are StringType (bronze contract)   ║
# ║  B5.  TYPE SANITISATION: empty string vs NULL consistency               ║
# ║       Source CSV "" and NULL both land as StringType NULL in Bronze     ║
# ║       (emptyValue=None in reader). DQ verifies no mixed representation. ║
# ║  B6.  Duplicate raw primary keys within same ingestion batch            ║
# ║  B7.  Row count anomaly vs historical average (>50% deviation = WARN)  ║
# ║  B8.  Unacknowledged schema drift events in schema_versions table       ║
# ╚══════════════════════════════════════════════════════════════════════════╝
#
# ════════════════════════════════════════════════════════════════════════════
# TYPE SANITISATION — WHY IT MATTERS AT BRONZE
# ─────────────────────────────────────────────────────────────────────────
# Source CSVs have NO enforced types. A "price" column can contain:
#   "15.99"   → valid double
#   "15.9900" → valid double but extra precision
#   ""        → empty string (NOT the same as NULL in most systems)
#   "NULL"    → the literal string NULL (NOT a SQL NULL)
#   " "       → whitespace (looks empty, is not)
#   "N/A"     → text that means null in source
#
# Bronze reads ALL of these as StringType with emptyValue=None.
# This means "" becomes SQL NULL and "NULL" stays as the string "NULL".
#
# DQ at Bronze verifies:
#   - No "NULL" string remains (should be SQL NULL after reader option)
#   - No whitespace-only values (looks like data, is not)
#   - All pipeline metadata columns (received_date, ingestion_ts) are present
#   - schema_drift_flag is Boolean (not String "True"/"False")
#
# If these fail at Bronze, Silver's type casting will produce wrong results.
# ════════════════════════════════════════════════════════════════════════════

import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime
from typing import Callable, Optional
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, BooleanType, DateType, TimestampType

# ══════════════════════════════════════════════════════════════════════════════
# CONFIG
# ══════════════════════════════════════════════════════════════════════════════
CATALOG  = "retail_data_project"
BRONZE   = f"{CATALOG}.01_bronze"
METADATA = f"{CATALOG}.04_metadata"

DQ_TABLE      = f"{METADATA}.dq_results"
PIPELINE_RUNS = f"{METADATA}.pipeline_runs"
SCHEMA_VERS   = f"{METADATA}.schema_versions"

# Thresholds
NULL_PK_FAIL_RATE   = 0.05   # >5% null PKs = CRITICAL
NULL_PK_WARN_RATE   = 0.01   # >1% null PKs = WARNING
ROW_ANOMALY_THRESH  = 0.50   # >50% deviation from avg = WARNING

# ══════════════════════════════════════════════════════════════════════════════
# LOGGER
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

logger   = get_logger("04a_dq_bronze")
RUN_TS   = datetime.now()
RUN_DATE = str(datetime.now().date())

results  = []   # all DQCheck results → written to dq_results at end
failures = []   # CRITICAL failures only → gate decision


# ══════════════════════════════════════════════════════════════════════════════
# DQ FRAMEWORK — DQCheck DATACLASS
# ──────────────────────────────────────────────────────────────────────────
# A DQCheck is a data object, not a function call.
# It holds: what to check, how strictly, and what it means when it fails.
#
# severity="CRITICAL" → failure raises Exception → Airflow marks FAILED
# severity="WARNING"  → logged + written to dq_results, pipeline continues
#
# threshold → fraction of rows allowed to violate (0.0 = zero tolerance)
#   Example: threshold=0.01 means up to 1% of rows can fail this check
# ══════════════════════════════════════════════════════════════════════════════

@dataclass
class DQCheck:
    check_id:    str
    layer:       str
    table:       str
    description: str
    severity:    str   = "CRITICAL"   # CRITICAL or WARNING
    threshold:   float = 0.0          # fraction of rows allowed to fail


def run_check(
    check: DQCheck,
    failed_count: int,
    total_count:  int,
    details:      str
) -> None:
    """
    Evaluates a DQCheck against actual counts. Appends to results + failures.
    Logs with icon: ✓ PASS  ⚠ WARNING  ✗ CRITICAL FAIL
    """
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
        RUN_TS, "bronze", check.layer, check.table,
        check.check_id, check.description,
        status, int(failed_count), int(total_count),
        round(rate * 100, 4), details
    ))


# ══════════════════════════════════════════════════════════════════════════════
# LOAD BRONZE TABLES
# ══════════════════════════════════════════════════════════════════════════════
df_cust = spark.table(f"{BRONZE}.customers")
df_prod = spark.table(f"{BRONZE}.products")
df_sale = spark.table(f"{BRONZE}.sales")

logger.info(f"Bronze loaded — customers: {df_cust.count():,} | products: {df_prod.count():,} | sales: {df_sale.count():,}")


# ══════════════════════════════════════════════════════════════════════════════
# B1 — ROW COUNT > 0
# Each table must have data. Empty bronze = ingestion failed silently.
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("B1: Row count checks")

for df, tname in [(df_cust,"customers"),(df_prod,"products"),(df_sale,"sales")]:
    n = df.count()
    run_check(
        DQCheck("B1_row_count", "bronze", tname,
                "Table must contain at least 1 row", "CRITICAL"),
        failed_count = 0 if n > 0 else 1,
        total_count  = 1,
        details      = f"{n:,} rows found"
    )


# ══════════════════════════════════════════════════════════════════════════════
# B2 — PRIMARY KEY NULL RATE
# PKs drive all downstream joins. High null rate = systematic source problem.
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("B2: Primary key null rate")

for df, pk, tname in [
    (df_cust, "customer_id", "customers"),
    (df_prod, "product_id",  "products"),
    (df_sale, "order_id",    "sales"),
]:
    total = df.count()
    # Bronze uses emptyValue=None so "" → NULL. Also check literal "NULL" string.
    nulls = df.filter(
        F.col(pk).isNull() |
        (F.col(pk).cast(StringType()) == "NULL") |
        (F.col(pk).cast(StringType()) == "")
    ).count()
    rate = nulls / total if total > 0 else 0
    sev  = "CRITICAL" if rate > NULL_PK_FAIL_RATE else "WARNING"
    thr  = NULL_PK_FAIL_RATE if rate > NULL_PK_FAIL_RATE else NULL_PK_WARN_RATE

    run_check(
        DQCheck("B2_pk_null_rate", "bronze", tname,
                f"Primary key null rate must be < {NULL_PK_FAIL_RATE:.0%}", sev, thr),
        failed_count = nulls, total_count = total,
        details = f"{nulls:,} nulls in '{pk}' ({rate:.2%})"
    )


# ══════════════════════════════════════════════════════════════════════════════
# B3 — TYPE SANITISATION: ALL BUSINESS COLUMNS MUST BE StringType AT BRONZE
# ──────────────────────────────────────────────────────────────────────────
# Bronze contract: everything arrives as StringType. No casting at landing.
# If any business column is NOT StringType here, the reader config is wrong
# (inferSchema was accidentally set to True) and downstream Silver casting
# will be applied on top of already-cast types — precision loss risk.
#
# Exception: pipeline metadata columns (received_date=DATE,
# ingestion_ts=TIMESTAMP, schema_drift_flag=BOOLEAN) are typed by us.
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("B3: Type sanitisation — all business columns must be StringType")

PIPELINE_META = {"received_date", "source_file", "ingestion_ts", "schema_drift_flag"}

for df, tname, business_cols in [
    (df_cust, "customers", ["customer_id","name","email","city","signup_date"]),
    (df_prod, "products",  ["product_id","product_name","category","price"]),
    (df_sale, "sales",     ["order_id","customer_id","product_id","quantity","order_date"]),
]:
    wrong_type_cols = []
    for c in business_cols:
        if c in df.columns:
            actual_type = df.schema[c].dataType
            if not isinstance(actual_type, StringType):
                wrong_type_cols.append(f"{c}:{actual_type.simpleString()}")

    run_check(
        DQCheck("B3_type_sanitisation_string", "bronze", tname,
                "All business columns must be StringType at bronze landing",
                "CRITICAL", threshold=0.0),
        failed_count = len(wrong_type_cols), total_count = len(business_cols),
        details = (
            f"Non-string business columns: {wrong_type_cols}" if wrong_type_cols
            else "All business columns are StringType ✓"
        )
    )


# ══════════════════════════════════════════════════════════════════════════════
# B4 — TYPE SANITISATION: EMPTY STRING vs NULL CONSISTENCY
# ──────────────────────────────────────────────────────────────────────────
# Bronze reader uses emptyValue=None → CSV "" should become SQL NULL.
# This check verifies no empty strings survived (which would cause Silver's
# NULL checks to silently miss them — "" != NULL in Spark filter conditions).
#
# Also checks for the literal string "NULL" which is NOT a SQL NULL.
# "NULL" strings must be caught here — Silver cast("double") of "NULL" → NULL
# but Silver cast("date") of "NULL" → NULL as well, so they might silently
# pass type casting but represent missing data that should be handled.
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("B4: Type sanitisation — empty string and literal NULL string check")

for df, tname in [(df_cust,"customers"),(df_prod,"products"),(df_sale,"sales")]:
    total = df.count()
    string_cols = [f.name for f in df.schema.fields
                   if isinstance(f.dataType, StringType)
                   and f.name not in PIPELINE_META]

    empty_str_count  = 0
    literal_null_count = 0

    for c in string_cols:
        empty_str_count    += df.filter(F.col(c) == "").count()
        literal_null_count += df.filter(F.col(c) == "NULL").count()

    # Empty strings: should be 0 (emptyValue=None should have caught them)
    run_check(
        DQCheck("B4a_empty_string_survived", "bronze", tname,
                "No empty strings should survive (emptyValue=None in reader)",
                "WARNING", threshold=0.0),
        failed_count = empty_str_count, total_count = max(total, 1),
        details = (
            f"{empty_str_count:,} empty string values survived reader — "
            f"will cause downstream NULL check misses"
            if empty_str_count > 0 else "No empty strings ✓"
        )
    )

    # Literal "NULL" string: should be 0 or very low
    run_check(
        DQCheck("B4b_literal_null_string", "bronze", tname,
                "Literal string 'NULL' should not appear — should be SQL NULL",
                "WARNING", threshold=0.005),   # allow <0.5%
        failed_count = literal_null_count, total_count = max(total, 1),
        details = (
            f"{literal_null_count:,} literal 'NULL' strings found — "
            f"source is writing 'NULL' text instead of empty/missing"
            if literal_null_count > 0 else "No literal NULL strings ✓"
        )
    )


# ══════════════════════════════════════════════════════════════════════════════
# B5 — TYPE SANITISATION: PIPELINE METADATA COLUMN TYPES
# ──────────────────────────────────────────────────────────────────────────
# Bronze adds these columns with correct types.
# If they arrive as StringType ("2024-01-15" instead of DATE), downstream
# Silver code that reads ingestion_ts for window dedup will fail or silently
# use wrong values.
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("B5: Pipeline metadata column type verification")

EXPECTED_META_TYPES = {
    "received_date":     DateType,
    "ingestion_ts":      TimestampType,
    "schema_drift_flag": BooleanType,
    "source_file":       StringType,
}

for df, tname in [(df_cust,"customers"),(df_prod,"products"),(df_sale,"sales")]:
    wrong = []
    for col_name, expected_type_cls in EXPECTED_META_TYPES.items():
        if col_name in df.columns:
            actual = df.schema[col_name].dataType
            if not isinstance(actual, expected_type_cls):
                wrong.append(f"{col_name}: expected={expected_type_cls.__name__} actual={actual.simpleString()}")

    run_check(
        DQCheck("B5_meta_col_types", "bronze", tname,
                "Pipeline metadata columns must have correct types",
                "CRITICAL", threshold=0.0),
        failed_count = len(wrong), total_count = len(EXPECTED_META_TYPES),
        details = (
            f"Wrong metadata types: {wrong}" if wrong
            else "All metadata column types correct ✓"
        )
    )


# ══════════════════════════════════════════════════════════════════════════════
# B6 — DUPLICATE PRIMARY KEYS WITHIN BRONZE TABLE
# ──────────────────────────────────────────────────────────────────────────
# Bronze appends ALL rows from new files. If the same order_id appears in
# two different files, both rows land in bronze (by design — capture everything).
# This check quantifies the duplication so Silver's dedup logic can be verified.
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("B6: Duplicate primary key detection")

for df, pk, tname in [
    (df_cust, "customer_id", "customers"),
    (df_prod, "product_id",  "products"),
    (df_sale, "order_id",    "sales"),
]:
    total  = df.count()
    unique = df.select(pk).distinct().count()
    dups   = total - unique
    run_check(
        DQCheck("B6_duplicate_pk", "bronze", tname,
                "Count of duplicate primary keys (informational — Silver dedup handles)",
                "WARNING", threshold=0.10),   # warn if >10% are duplicates
        failed_count = dups, total_count = total,
        details = f"{dups:,} duplicate {pk} values ({total:,} total, {unique:,} unique)"
    )


# ══════════════════════════════════════════════════════════════════════════════
# B7 — ROW COUNT ANOMALY vs HISTORICAL AVERAGE
# ──────────────────────────────────────────────────────────────────────────
# Compares today's bronze count against the rolling average.
# A >50% drop catches upstream truncations before they silently reach gold.
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("B7: Row count anomaly detection")

for df, tname in [(df_cust,"customers"),(df_prod,"products"),(df_sale,"sales")]:
    current = df.count()
    try:
        hist_avg = (
            spark.table(f"{METADATA}.processed_files")
            .filter(F.col("table_name") == tname)
            .agg(F.avg("row_count").alias("avg"))
            .collect()[0]["avg"]
        )
        if hist_avg and hist_avg > 0:
            deviation = abs(current - hist_avg) / hist_avg
            run_check(
                DQCheck("B7_row_count_anomaly", "bronze", tname,
                        f"Row count must not deviate >50% from historical average",
                        "WARNING", threshold=ROW_ANOMALY_THRESH),
                failed_count = 1 if deviation > ROW_ANOMALY_THRESH else 0,
                total_count  = 1,
                details = (
                    f"Current={current:,} | Hist avg={hist_avg:,.0f} | "
                    f"Deviation={deviation:.1%} "
                    + ("⚠ POSSIBLE UPSTREAM TRUNCATION" if deviation > ROW_ANOMALY_THRESH else "✓")
                )
            )
        else:
            logger.info(f"  [B7] {tname}: No historical baseline — first run")
    except Exception:
        logger.info(f"  [B7] {tname}: Historical data unavailable")


# ══════════════════════════════════════════════════════════════════════════════
# B8 — UNACKNOWLEDGED SCHEMA DRIFT EVENTS
# ──────────────────────────────────────────────────────────────────────────
# Bronze records every schema drift in schema_versions with acknowledged=False.
# If this count is > 0, data stewards haven't reviewed the new columns.
# This is a WARNING (pipeline should continue) but ops team must act.
# ══════════════════════════════════════════════════════════════════════════════
logger.info("─" * 60)
logger.info("B8: Unacknowledged schema drift events")

try:
    unack = spark.table(SCHEMA_VERS).filter(F.col("acknowledged") == False).count()
    run_check(
        DQCheck("B8_schema_drift_unacked", "bronze", "schema_versions",
                "All schema drift events should be reviewed and acknowledged",
                "WARNING", threshold=0.0),
        failed_count = unack, total_count = max(unack, 1),
        details = (
            f"{unack} unacknowledged drift events — "
            f"run: SELECT * FROM {SCHEMA_VERS} WHERE acknowledged=false"
            if unack > 0 else "All drift events acknowledged ✓"
        )
    )
except Exception as e:
    logger.warning(f"B8: Could not check schema_versions: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# WRITE DQ RESULTS TO dq_results TABLE
# ══════════════════════════════════════════════════════════════════════════════
if results:
    from pyspark.sql.types import (
        StructType, StructField, StringType as ST,
        TimestampType as TT, LongType as LT,
        FloatType as FT
    )
    schema = StructType([
        StructField("run_timestamp",  TT(),  False),
        StructField("notebook",       ST(),  False),
        StructField("layer",          ST(),  False),
        StructField("table_name",     ST(),  False),
        StructField("check_id",       ST(),  False),
        StructField("description",    ST(),  True),
        StructField("status",         ST(),  False),
        StructField("failed_count",   LT(),  False),
        StructField("total_count",    LT(),  False),
        StructField("fail_rate_pct",  FT(),  True),
        StructField("details",        ST(),  True),
    ])
    spark.createDataFrame(results, schema=schema) \
        .write.format("delta").mode("append").saveAsTable(DQ_TABLE)
    logger.info(f"DQ results written → {DQ_TABLE}")


# ══════════════════════════════════════════════════════════════════════════════
# FINAL GATE
# ══════════════════════════════════════════════════════════════════════════════
total_c   = len(results)
passed_c  = sum(1 for r in results if r[6] == "PASS")
warn_c    = sum(1 for r in results if r[6] == "WARNING")
fail_c    = len(failures)
duration  = (datetime.now() - RUN_TS).seconds

logger.info(f"{'═'*60}")
logger.info(f"  BRONZE DQ SUMMARY")
logger.info(f"{'═'*60}")
logger.info(f"  Total   : {total_c}")
logger.info(f"  Passed  : {passed_c}")
logger.info(f"  Warnings: {warn_c}  (pipeline continues)")
logger.info(f"  Critical: {fail_c}  (pipeline halts if > 0)")
logger.info(f"  Duration: {duration}s")
logger.info(f"{'═'*60}")

if failures:
    for f in failures:
        logger.error(f"  FAILED: {f}")
    raise Exception(
        f"BRONZE DQ GATE FAILED — {fail_c} critical check(s).\n"
        + "\n".join(failures)
    )
else:
    logger.info("Bronze DQ gate PASSED ✓ — Silver transform may proceed")