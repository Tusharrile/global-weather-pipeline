# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Error Handling & Pipeline Run Tracking
# MAGIC **Pipeline:** Real-Time-Weather-Data-Pipeline
# MAGIC **Purpose:** Centralized error logging, pipeline run tracking, and safe execution
# MAGIC **Run order:** This notebook orchestrates all layers in sequence:
# MAGIC Bronze → Silver → Gold → DQ Checks
# MAGIC
# MAGIC **Tables created:**
# MAGIC - `global_weather_catalog.pipeline_logs.run_log` — tracks every pipeline run
# MAGIC - `global_weather_catalog.pipeline_logs.error_log` — captures all errors

# COMMAND ----------

import datetime
import traceback
import logging
from pyspark.sql.types import (
    StructType, StructField, StringType, TimestampType, IntegerType
)
from pyspark.sql.functions import lit, current_timestamp

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("pipeline_error_handler")

CATALOG    = "global_weather_catalog"
BUCKET     = "s3://global-weather-pipeline"
batch_date = datetime.date.today().strftime("%Y-%m-%d")
run_id     = f"run_{datetime.datetime.now().strftime('%Y%m%d_%H%M%S')}"

# ── Pipeline run tracker ───────────────────────────────────
pipeline_runs  = []   # one entry per layer
error_log      = []   # one entry per error

def log_run(layer, status, rows_processed=0, duration_sec=0, error_msg=""):
    """Log a pipeline layer run result."""
    pipeline_runs.append({
        "run_id"          : run_id,
        "batch_date"      : batch_date,
        "layer"           : layer,
        "status"          : status,
        "rows_processed"  : str(rows_processed),
        "duration_sec"    : str(round(duration_sec, 2)),
        "error_message"   : error_msg[:500] if error_msg else "",
        "logged_at"       : datetime.datetime.now().isoformat()
    })
    icon = "✅" if status == "SUCCESS" else ("⚠️ " if status == "WARNING" else "❌")
    print(f"   {icon} [{layer}] status={status} | rows={rows_processed:,} | duration={duration_sec:.1f}s")

def log_error(layer, error_type, error_msg, cell_info=""):
    """Log a detailed error entry."""
    error_log.append({
        "run_id"      : run_id,
        "batch_date"  : batch_date,
        "layer"       : layer,
        "error_type"  : error_type,
        "error_msg"   : str(error_msg)[:1000],
        "cell_info"   : cell_info,
        "logged_at"   : datetime.datetime.now().isoformat()
    })
    logger.error(f"[{layer}] {error_type}: {error_msg}")

# ── Create logging schema ──────────────────────────────────
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.pipeline_logs")

print("✅ Error handling framework initialized!")
print(f"   Run ID     : {run_id}")
print(f"   Batch date : {batch_date}")
print(f"   Catalog    : {CATALOG}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## LAYER 1 — Bronze Ingestion

# COMMAND ----------

print("=" * 60)
print("LAYER 1 — Bronze Ingestion")
print("=" * 60)

bronze_start = datetime.datetime.now()
bronze_rows  = 0
bronze_error = ""

try:
    # ── Validate S3 source ─────────────────────────────────
    logger.info("[BRONZE] Checking S3 source...")
    try:
        source_df    = spark.read.option("header", "true").csv(f"{BUCKET}/raw/")
        source_count = source_df.count()
        if source_count == 0:
            raise ValueError("S3 source is empty — no files found in raw/")
        logger.info(f"[BRONZE] Source records found: {source_count:,}")
    except Exception as e:
        log_error("BRONZE", "SourceValidationError", str(e), "S3 source check")
        raise

    # ── Validate Bronze table exists and has data ──────────
    logger.info("[BRONZE] Validating bronze table...")
    try:
        bronze_df   = spark.table(f"{CATALOG}.bronze.bronze_weather")
        bronze_rows = bronze_df.count()
        if bronze_rows == 0:
            raise ValueError("Bronze table is empty after ingestion")
        logger.info(f"[BRONZE] Bronze table rows: {bronze_rows:,}")
    except Exception as e:
        log_error("BRONZE", "TableValidationError", str(e), "Bronze table check")
        raise

    # ── Schema validation ──────────────────────────────────
    logger.info("[BRONZE] Validating schema...")
    required_bronze_cols = [
        "weather_record_id", "country", "location_name",
        "temperature_celsius", "humidity", "wind_kph",
        "air_quality_pm2_5", "_batch_date", "_ingestion_ts"
    ]
    missing = [c for c in required_bronze_cols if c not in bronze_df.columns]
    if missing:
        log_error("BRONZE", "SchemaValidationError",
                  f"Missing columns: {missing}", "Schema check")
        raise ValueError(f"Bronze schema invalid — missing: {missing}")

    # ── Row count sanity check ─────────────────────────────
    if bronze_rows < 1000:
        log_error("BRONZE", "RowCountWarning",
                  f"Only {bronze_rows} rows — expected 100,000+", "Row count check")
        log_run("BRONZE", "WARNING", bronze_rows,
                (datetime.datetime.now() - bronze_start).total_seconds(),
                f"Low row count: {bronze_rows}")
    else:
        duration = (datetime.datetime.now() - bronze_start).total_seconds()
        log_run("BRONZE", "SUCCESS", bronze_rows, duration)
        print(f"\n✅ Bronze validation passed!")
        print(f"   Rows     : {bronze_rows:,}")
        print(f"   Columns  : {len(bronze_df.columns)}")
        print(f"   Duration : {duration:.1f}s")

except Exception as e:
    duration = (datetime.datetime.now() - bronze_start).total_seconds()
    bronze_error = str(e)
    log_error("BRONZE", type(e).__name__, str(e), "Bronze layer execution")
    log_run("BRONZE", "FAILED", bronze_rows, duration, str(e))
    print(f"\n❌ Bronze layer FAILED: {e}")
    print("   Pipeline will attempt to continue with existing Bronze data...")

# COMMAND ----------

# MAGIC %md
# MAGIC ## LAYER 2 — Silver Ingestion (My Pipeline)

# COMMAND ----------

print("\n" + "=" * 60)
print("LAYER 2 — Silver Ingestion (Your Pipeline — silver_v2)")
print("=" * 60)

silver_start = datetime.datetime.now()
silver_rows  = 0
silver_error = ""

try:
    # ── Check Silver table exists ──────────────────────────
    logger.info("[SILVER_V2] Validating silver_v2 table...")
    try:
        silver_df   = spark.table(f"{CATALOG}.silver_v2.weather_silver")
        silver_rows = silver_df.count()
        if silver_rows == 0:
            raise ValueError("Silver V2 table is empty")
        logger.info(f"[SILVER_V2] Rows: {silver_rows:,}")
    except Exception as e:
        log_error("SILVER_V2", "TableValidationError", str(e), "Silver V2 table check")
        raise

    # ── Validate outlier column exists ─────────────────────
    if "is_outlier" not in silver_df.columns:
        raise ValueError("is_outlier column missing from Silver V2 — Gold layer will fail")

    # ── Check clean record ratio ───────────────────────────
    clean_cnt   = silver_df.filter(silver_df.is_outlier == False).count()
    clean_pct   = round((clean_cnt / silver_rows) * 100, 2)
    outlier_cnt = silver_rows - clean_cnt

    if clean_pct < 50:
        log_error("SILVER_V2", "DataQualityWarning",
                  f"Only {clean_pct}% clean records — too many outliers",
                  "Outlier ratio check")
        raise ValueError(f"Silver V2 quality too low — only {clean_pct}% clean records")

    # ── Validate required columns ──────────────────────────
    required_silver = [
        "weather_record_id", "country", "location_name",
        "temperature_celsius", "humidity", "season",
        "heat_index", "wind_compass_zone", "is_outlier"
    ]
    missing = [c for c in required_silver if c not in silver_df.columns]
    if missing:
        log_error("SILVER_V2", "SchemaError", f"Missing: {missing}", "Schema check")
        raise ValueError(f"Silver V2 schema invalid — missing: {missing}")

    duration = (datetime.datetime.now() - silver_start).total_seconds()
    log_run("SILVER_V2", "SUCCESS", silver_rows, duration)
    print(f"\n✅ Silver V2 validation passed!")
    print(f"   Total rows  : {silver_rows:,}")
    print(f"   Clean rows  : {clean_cnt:,} ({clean_pct}%)")
    print(f"   Outliers    : {outlier_cnt:,}")
    print(f"   Duration    : {duration:.1f}s")

except Exception as e:
    duration = (datetime.datetime.now() - silver_start).total_seconds()
    silver_error = str(e)
    log_error("SILVER_V2", type(e).__name__, str(e), "Silver V2 execution")
    log_run("SILVER_V2", "FAILED", silver_rows, duration, str(e))
    print(f"\n❌ Silver V2 FAILED: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## LAYER 3 — Silver Ingestion (Teammate's Pipeline)

# COMMAND ----------

print("\n" + "=" * 60)
print("LAYER 3 — Silver Ingestion (Teammate — silver)")
print("=" * 60)

tm_silver_start = datetime.datetime.now()
tm_silver_rows  = 0

try:
    tm_silver_df   = spark.table(f"{CATALOG}.silver.weather_silver")
    tm_silver_rows = tm_silver_df.count()

    if tm_silver_rows == 0:
        raise ValueError("Teammate Silver table is empty")

    required_tm = [
        "weather_record_id", "country", "location_name",
        "temperature_celsius", "humidity", "is_outlier"
    ]
    missing_tm = [c for c in required_tm if c not in tm_silver_df.columns]
    if missing_tm:
        raise ValueError(f"Teammate Silver schema invalid — missing: {missing_tm}")

    duration = (datetime.datetime.now() - tm_silver_start).total_seconds()
    log_run("SILVER_TM", "SUCCESS", tm_silver_rows, duration)
    print(f"\n✅ Teammate Silver validation passed!")
    print(f"   Rows     : {tm_silver_rows:,}")
    print(f"   Duration : {duration:.1f}s")

except Exception as e:
    duration = (datetime.datetime.now() - tm_silver_start).total_seconds()
    log_error("SILVER_TM", type(e).__name__, str(e), "Teammate Silver check")
    log_run("SILVER_TM", "FAILED", tm_silver_rows, duration, str(e))
    print(f"\n❌ Teammate Silver FAILED: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## LAYER 4 — Gold Layer (My Pipeline)

# COMMAND ----------

print("\n" + "=" * 60)
print("LAYER 4 — Gold Layer (Your Pipeline — gold_analytics)")
print("=" * 60)

gold_start = datetime.datetime.now()
gold_rows  = 0

try:
    # ── Check all Gold tables exist ────────────────────────
    gold_tables = [
        "dim_location", "dim_date",
        "dim_weather_condition", "dim_air_quality",
        "fact_weather_events"
    ]

    logger.info("[GOLD] Checking all gold_analytics tables...")
    gold_counts = {}
    for tbl in gold_tables:
        try:
            cnt = spark.sql(
                f"SELECT COUNT(*) AS cnt FROM {CATALOG}.gold_analytics.{tbl}"
            ).collect()[0]["cnt"]
            if cnt == 0:
                log_error("GOLD", "EmptyTableError",
                          f"{tbl} is empty", f"Table check: {tbl}")
            gold_counts[tbl] = cnt
            logger.info(f"[GOLD] {tbl}: {cnt:,} rows")
        except Exception as e:
            log_error("GOLD", "TableMissingError", str(e), f"Table: {tbl}")
            raise ValueError(f"Gold table missing or unreadable: {tbl}")

    gold_rows = gold_counts.get("fact_weather_events", 0)

    # ── FK integrity check ─────────────────────────────────
    logger.info("[GOLD] Running FK integrity check...")
    fk_result = spark.sql(f"""
        SELECT
            SUM(CASE WHEN location_id   IS NULL THEN 1 ELSE 0 END) AS null_loc,
            SUM(CASE WHEN date_id       IS NULL THEN 1 ELSE 0 END) AS null_date,
            SUM(CASE WHEN condition_id  IS NULL THEN 1 ELSE 0 END) AS null_cond,
            SUM(CASE WHEN air_quality_id IS NULL THEN 1 ELSE 0 END) AS null_aq
        FROM {CATALOG}.gold_analytics.fact_weather_events
    """).collect()[0]

    fk_errors = {k: v for k, v in fk_result.asDict().items() if v and v > 0}
    if fk_errors:
        for fk, cnt in fk_errors.items():
            log_error("GOLD", "FKIntegrityError",
                      f"{fk} has {cnt} nulls", "FK check")
        raise ValueError(f"Gold FK integrity failed: {fk_errors}")

    duration = (datetime.datetime.now() - gold_start).total_seconds()
    log_run("GOLD", "SUCCESS", gold_rows, duration)
    print(f"\n✅ Gold validation passed!")
    for tbl, cnt in gold_counts.items():
        print(f"   {tbl:<30} : {cnt:,} rows")
    print(f"   FK integrity          : ✅ All clean")
    print(f"   Duration              : {duration:.1f}s")

except Exception as e:
    duration = (datetime.datetime.now() - gold_start).total_seconds()
    log_error("GOLD", type(e).__name__, str(e), "Gold layer execution")
    log_run("GOLD", "FAILED", gold_rows, duration, str(e))
    print(f"\n❌ Gold layer FAILED: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## LAYER 5 — Gold Layer (Teammate's Pipeline)

# COMMAND ----------

print("\n" + "=" * 60)
print("LAYER 5 — Gold Layer (Teammate — gold)")
print("=" * 60)

tm_gold_start = datetime.datetime.now()
tm_gold_rows  = 0

try:
    tm_gold_tables = [
        "dim_location", "dim_condition", "dim_astronomy",
        "dim_date", "fact_weather"
    ]

    tm_gold_counts = {}
    for tbl in tm_gold_tables:
        try:
            cnt = spark.sql(
                f"SELECT COUNT(*) AS cnt FROM {CATALOG}.gold.{tbl}"
            ).collect()[0]["cnt"]
            tm_gold_counts[tbl] = cnt
        except Exception as e:
            log_error("GOLD_TM", "TableMissingError", str(e), f"Table: {tbl}")
            raise ValueError(f"Teammate Gold table missing: {tbl}")

    tm_gold_rows = tm_gold_counts.get("fact_weather", 0)

    if tm_gold_rows == 0:
        raise ValueError("Teammate fact_weather is empty")

    duration = (datetime.datetime.now() - tm_gold_start).total_seconds()
    log_run("GOLD_TM", "SUCCESS", tm_gold_rows, duration)
    print(f"\n✅ Teammate Gold validation passed!")
    for tbl, cnt in tm_gold_counts.items():
        print(f"   {tbl:<20} : {cnt:,} rows")
    print(f"   Duration : {duration:.1f}s")

except Exception as e:
    duration = (datetime.datetime.now() - tm_gold_start).total_seconds()
    log_error("GOLD_TM", type(e).__name__, str(e), "Teammate Gold check")
    log_run("GOLD_TM", "FAILED", tm_gold_rows, duration, str(e))
    print(f"\n❌ Teammate Gold FAILED: {e}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pipeline Run Summary + Log to Delta

# COMMAND ----------

print("\n" + "=" * 60)
print("PIPELINE RUN SUMMARY")
print("=" * 60)
print(f"\n   Run ID     : {run_id}")
print(f"   Batch date : {batch_date}")

total_layers  = len(pipeline_runs)
success_layers = sum(1 for r in pipeline_runs if r["status"] == "SUCCESS")
failed_layers  = sum(1 for r in pipeline_runs if r["status"] == "FAILED")
warning_layers = sum(1 for r in pipeline_runs if r["status"] == "WARNING")
total_errors   = len(error_log)

print(f"\n📊 Layer Results:")
for r in pipeline_runs:
    icon = "✅" if r["status"] == "SUCCESS" else ("⚠️ " if r["status"] == "WARNING" else "❌")
    print(f"   {icon} {r['layer']:<15} | {r['status']:<10} | rows={int(r['rows_processed']):>10,} | {r['duration_sec']}s")

print(f"\n📊 Overall:")
print(f"   Total layers  : {total_layers}")
print(f"   ✅ Succeeded  : {success_layers}")
print(f"   ⚠️  Warnings  : {warning_layers}")
print(f"   ❌ Failed     : {failed_layers}")
print(f"   Total errors  : {total_errors}")

overall_status = "SUCCESS" if failed_layers == 0 else (
    "PARTIAL" if success_layers > 0 else "FAILED"
)
print(f"\n   Pipeline Status : {overall_status}")

# COMMAND ----------

# ── Write run log to Delta ─────────────────────────────────
run_schema = StructType([
    StructField("run_id",         StringType(),  True),
    StructField("batch_date",     StringType(),  True),
    StructField("layer",          StringType(),  True),
    StructField("status",         StringType(),  True),
    StructField("rows_processed", StringType(),  True),
    StructField("duration_sec",   StringType(),  True),
    StructField("error_message",  StringType(),  True),
    StructField("logged_at",      StringType(),  True),
])

error_schema = StructType([
    StructField("run_id",     StringType(), True),
    StructField("batch_date", StringType(), True),
    StructField("layer",      StringType(), True),
    StructField("error_type", StringType(), True),
    StructField("error_msg",  StringType(), True),
    StructField("cell_info",  StringType(), True),
    StructField("logged_at",  StringType(), True),
])

RUN_LOG_PATH   = f"{BUCKET}/pipeline_logs/run_log/"
ERROR_LOG_PATH = f"{BUCKET}/pipeline_logs/error_log/"

# ── Write run log ──────────────────────────────────────────
df_run_log = spark.createDataFrame(pipeline_runs, schema=run_schema)
df_run_log.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("batch_date") \
    .save(RUN_LOG_PATH)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.pipeline_logs.run_log
    USING DELTA LOCATION '{RUN_LOG_PATH}'
""")

# ── Write error log ────────────────────────────────────────
if error_log:
    df_error_log = spark.createDataFrame(error_log, schema=error_schema)
    df_error_log.write \
        .format("delta") \
        .mode("append") \
        .option("mergeSchema", "true") \
        .partitionBy("batch_date") \
        .save(ERROR_LOG_PATH)

    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {CATALOG}.pipeline_logs.error_log
        USING DELTA LOCATION '{ERROR_LOG_PATH}'
    """)
    print(f"\n✅ Error log written → {CATALOG}.pipeline_logs.error_log ({len(error_log)} errors)")
else:
    print(f"\n✅ No errors to log!")

print(f"✅ Run log written → {CATALOG}.pipeline_logs.run_log ({len(pipeline_runs)} layers)")

# COMMAND ----------

# ── Display logs ───────────────────────────────────────────
print("\n📊 Run Log:")
display(df_run_log)

if error_log:
    print("\n📊 Error Log:")
    display(df_error_log)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Historical Run Query

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View last 10 pipeline runs
# MAGIC SELECT
# MAGIC     run_id,
# MAGIC     batch_date,
# MAGIC     layer,
# MAGIC     status,
# MAGIC     CAST(rows_processed AS BIGINT) AS rows_processed,
# MAGIC     CAST(duration_sec AS DOUBLE)   AS duration_sec,
# MAGIC     error_message,
# MAGIC     logged_at
# MAGIC FROM global_weather_catalog.pipeline_logs.run_log
# MAGIC ORDER BY logged_at DESC
# MAGIC LIMIT 50

# COMMAND ----------

# MAGIC %sql
# MAGIC -- View all errors (only exists if errors occurred)
# MAGIC SELECT * FROM global_weather_catalog.pipeline_logs.run_log
# MAGIC ORDER BY logged_at DESC
# MAGIC LIMIT 50

# COMMAND ----------

# ── Final pipeline gate ────────────────────────────────────
if failed_layers > 0:
    failed_names = [r["layer"] for r in pipeline_runs if r["status"] == "FAILED"]
    raise ValueError(
        f"🚨 Pipeline Run FAILED — {failed_layers} layer(s) failed: {failed_names}. "
        f"Run ID: {run_id}. "
        f"Check {CATALOG}.pipeline_logs.error_log for details."
    )
else:
    print(f"\n🎉 Pipeline Run COMPLETED SUCCESSFULLY!")
    print(f"   Run ID         : {run_id}")
    print(f"   Batch date     : {batch_date}")
    print(f"   Layers passed  : {success_layers}/{total_layers}")
    print(f"   Total errors   : {total_errors}")
    print(f"   Run log        : {CATALOG}.pipeline_logs.run_log")
    print(f"   Error log      : {CATALOG}.pipeline_logs.error_log")
