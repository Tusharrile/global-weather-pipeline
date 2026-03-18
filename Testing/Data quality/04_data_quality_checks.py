# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import col, lit, current_timestamp, sum as spark_sum
from pyspark.sql.types import StructType, StructField, StringType
import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("data_quality")

CATALOG    = "global_weather_catalog"
batch_date = datetime.date.today().strftime("%Y-%m-%d")

# ── DQ Result tracker ──────────────────────────────────────
dq_results = []

def log_check(pipeline, layer, check_name, status, expected, actual, details=""):
    icon = "✅" if status == "PASS" else ("⚠️ " if status == "WARN" else "❌")
    dq_results.append({
        "batch_date"  : batch_date,
        "pipeline"    : pipeline,
        "layer"       : layer,
        "check_name"  : check_name,
        "status"      : status,
        "expected"    : str(expected),
        "actual"      : str(actual),
        "details"     : details,
        "checked_at"  : datetime.datetime.now().isoformat()
    })
    print(f"   {icon} [{pipeline}|{layer}] {check_name} | expected={expected} | actual={actual} {details}")

# ── Shared range checks used by both pipelines ─────────────
range_checks = {
    "temperature_celsius" : (-90,   60),
    "humidity"            : (0,    100),
    "wind_kph"            : (0,    500),
    "pressure_mb"         : (870, 1084),
    "uv_index"            : (0,     20),
    "visibility_km"       : (0,    100),
    "precip_mm"           : (0,   1000),
    "cloud"               : (0,    100),
}

critical_cols = [
    "country", "location_name", "last_updated",
    "temperature_celsius", "humidity", "wind_kph",
    "pressure_mb", "uv_index", "air_quality_pm2_5",
    "condition_text", "weather_record_id"
]

print("✅ DQ framework initialized!")
print(f"   Catalog    : {CATALOG}")
print(f"   Batch date : {batch_date}")
print(f"   Pipelines  : YOUR (silver_v2/gold_analytics) + TEAMMATE (silver/gold)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SECTION 1 — My Pipeline (silver_v2 → gold_analytics)

# COMMAND ----------

print("=" * 65)
print("YOUR PIPELINE — Silver V2 + Gold Analytics")
print("=" * 65)

df_your_silver    = spark.table(f"{CATALOG}.silver_v2.weather_silver")
your_silver_count = df_your_silver.count()
print(f"\n📊 silver_v2.weather_silver rows: {your_silver_count:,}")

# ── CHECK 1: Row Count ─────────────────────────────────────
print("\n📋 CHECK 1 — Row Count")
if your_silver_count == 0:
    log_check("YOURS", "SILVER", "row_count", "FAIL", ">0", your_silver_count, "Empty!")
elif your_silver_count < 1000:
    log_check("YOURS", "SILVER", "row_count", "WARN", ">1000", your_silver_count)
else:
    log_check("YOURS", "SILVER", "row_count", "PASS", ">1000", f"{your_silver_count:,}")

# ── CHECK 2: Null Checks ───────────────────────────────────
print("\n📋 CHECK 2 — Null Checks")
null_counts = df_your_silver.select([
    spark_sum(col(c).isNull().cast("int")).alias(c) for c in critical_cols
]).collect()[0].asDict()

for col_name, null_cnt in null_counts.items():
    pct = round((null_cnt / your_silver_count) * 100, 2) if your_silver_count > 0 else 0
    status = "PASS" if null_cnt == 0 else ("WARN" if pct <= 5 else "FAIL")
    log_check("YOURS", "SILVER", f"null_{col_name}", status, "0 nulls", null_cnt, f"({pct}%)" if null_cnt > 0 else "")

# ── CHECK 3: Range Validation ──────────────────────────────
print("\n📋 CHECK 3 — Range Validation")
for col_name, (min_val, max_val) in range_checks.items():
    oor = df_your_silver.filter((col(col_name) < min_val) | (col(col_name) > max_val)).count()
    pct = round((oor / your_silver_count) * 100, 2) if your_silver_count > 0 else 0
    status = "PASS" if oor == 0 else ("WARN" if pct <= 1 else "FAIL")
    log_check("YOURS", "SILVER", f"range_{col_name}", status, f"{min_val}–{max_val}", oor if oor > 0 else "all in range", f"({pct}%)" if oor > 0 else "")

# ── CHECK 4: Duplicates ────────────────────────────────────
print("\n📋 CHECK 4 — Duplicates")
dupes = your_silver_count - df_your_silver.dropDuplicates(["weather_record_id"]).count()
dup_pct = round((dupes / your_silver_count) * 100, 2) if your_silver_count > 0 else 0
status = "PASS" if dupes == 0 else ("WARN" if dup_pct <= 1 else "FAIL")
log_check("YOURS", "SILVER", "duplicates", status, "0", dupes, f"({dup_pct}%)" if dupes > 0 else "")

# ── CHECK 5: Outlier Distribution ─────────────────────────
print("\n📋 CHECK 5 — Outlier Distribution")
outlier_map  = {str(r["is_outlier"]): r["count"] for r in df_your_silver.groupBy("is_outlier").count().collect()}
outlier_cnt  = outlier_map.get("true", 0)
outlier_pct  = round((outlier_cnt / your_silver_count) * 100, 2) if your_silver_count > 0 else 0
status = "PASS" if outlier_pct <= 5 else ("WARN" if outlier_pct <= 15 else "FAIL")
log_check("YOURS", "SILVER", "outlier_pct", status, "<=5%", f"{outlier_pct}%", f"({outlier_cnt:,} outliers)")

# ── CHECK 6: Season Completeness ──────────────────────────
print("\n📋 CHECK 6 — Season Completeness")
seasons = {r["season"] for r in df_your_silver.select("season").distinct().collect()}
missing = {"Spring", "Summer", "Autumn", "Winter"} - seasons
log_check("YOURS", "SILVER", "seasons", "PASS" if not missing else "WARN",
          "4 seasons", sorted(seasons), f"missing: {missing}" if missing else "")

# ── CHECK 7: Data Freshness ────────────────────────────────
print("\n📋 CHECK 7 — Data Freshness")
latest = df_your_silver.agg(F.max("_batch_date").alias("l")).collect()[0]["l"]
log_check("YOURS", "SILVER", "freshness",
          "PASS" if latest == batch_date else "WARN",
          batch_date, latest, "" if latest == batch_date else "Not today's run")

# COMMAND ----------

# ── Load Your Gold ─────────────────────────────────────────
print("\n📋 Loading Your Gold tables (gold_analytics)...")
df_your_fact     = spark.table(f"{CATALOG}.gold_analytics.fact_weather_events")
df_your_loc      = spark.table(f"{CATALOG}.gold_analytics.dim_location")
df_your_date     = spark.table(f"{CATALOG}.gold_analytics.dim_date")
df_your_cond     = spark.table(f"{CATALOG}.gold_analytics.dim_weather_condition")
df_your_aq       = spark.table(f"{CATALOG}.gold_analytics.dim_air_quality")
your_fact_count  = df_your_fact.count()

print(f"   fact_weather_events   : {your_fact_count:,}")
print(f"   dim_location          : {df_your_loc.count():,}")
print(f"   dim_date              : {df_your_date.count():,}")
print(f"   dim_weather_condition : {df_your_cond.count():,}")
print(f"   dim_air_quality       : {df_your_aq.count():,}")

# ── CHECK 8: Fact Retention ────────────────────────────────
print("\n📋 CHECK 8 — Fact vs Silver Retention")
silver_clean   = df_your_silver.filter(col("is_outlier") == False).count()
ret_pct        = round((your_fact_count / silver_clean) * 100, 2) if silver_clean > 0 else 0
status = "PASS" if ret_pct >= 95 else ("WARN" if ret_pct >= 80 else "FAIL")
log_check("YOURS", "GOLD", "fact_retention", status, ">=95%", f"{ret_pct}%",
          f"(silver_clean={silver_clean:,}, fact={your_fact_count:,})")

# ── CHECK 9: FK Integrity ──────────────────────────────────
print("\n📋 CHECK 9 — FK Integrity")
fk_nulls = df_your_fact.select([
    spark_sum(col(c).isNull().cast("int")).alias(c)
    for c in ["location_id", "date_id", "condition_id", "air_quality_id"]
]).collect()[0].asDict()

for fk, null_cnt in fk_nulls.items():
    log_check("YOURS", "GOLD", f"fk_{fk}",
              "PASS" if null_cnt == 0 else "FAIL",
              "0 nulls", null_cnt,
              f"Broken FK for {null_cnt} rows!" if null_cnt > 0 else "")

# ── CHECK 10: Dim PK Uniqueness ────────────────────────────
print("\n📋 CHECK 10 — Dim PK Uniqueness")
for tbl, (pk, df_d) in {
    "dim_location"          : ("location_id",   df_your_loc),
    "dim_date"              : ("date_id",        df_your_date),
    "dim_weather_condition" : ("condition_id",   df_your_cond),
    "dim_air_quality"       : ("air_quality_id", df_your_aq),
}.items():
    dupes = df_d.count() - df_d.dropDuplicates([pk]).count()
    log_check("YOURS", "GOLD", f"pk_{tbl}",
              "PASS" if dupes == 0 else "FAIL",
              "unique", f"{df_d.count():,} rows" if dupes == 0 else f"{dupes} dupes!")

# ── CHECK 11: Gold Range Validation ───────────────────────
print("\n📋 CHECK 11 — Gold Range Validation")
for col_name, (min_val, max_val) in {
    "temperature_celsius" : (-90, 60),
    "humidity"            : (0, 100),
    "uv_index"            : (0, 20),
    "pressure_mb"         : (870, 1084),
    "wind_kph"            : (0, 500),
}.items():
    oor = df_your_fact.filter((col(col_name) < min_val) | (col(col_name) > max_val)).count()
    pct = round((oor / your_fact_count) * 100, 2) if your_fact_count > 0 else 0
    log_check("YOURS", "GOLD", f"range_{col_name}",
              "PASS" if oor == 0 else "FAIL",
              f"{min_val}–{max_val}", "all in range" if oor == 0 else oor, f"({pct}%)" if oor > 0 else "")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SECTION 2 — TEAMMATE'S Pipeline (silver → gold)

# COMMAND ----------

print("\n" + "=" * 65)
print("TEAMMATE'S PIPELINE — Silver + Gold")
print("=" * 65)

df_tm_silver    = spark.table(f"{CATALOG}.silver.weather_silver")
tm_silver_count = df_tm_silver.count()
print(f"\n📊 silver.weather_silver rows: {tm_silver_count:,}")

# ── CHECK 12: Row Count ────────────────────────────────────
print("\n📋 CHECK 12 — Row Count")
status = "PASS" if tm_silver_count > 1000 else ("WARN" if tm_silver_count > 0 else "FAIL")
log_check("TEAMMATE", "SILVER", "row_count", status, ">1000", f"{tm_silver_count:,}")

# ── CHECK 13: Null Checks ──────────────────────────────────
print("\n📋 CHECK 13 — Null Checks")
tm_nulls = df_tm_silver.select([
    spark_sum(col(c).isNull().cast("int")).alias(c) for c in critical_cols
]).collect()[0].asDict()

for col_name, null_cnt in tm_nulls.items():
    pct = round((null_cnt / tm_silver_count) * 100, 2) if tm_silver_count > 0 else 0
    status = "PASS" if null_cnt == 0 else ("WARN" if pct <= 5 else "FAIL")
    log_check("TEAMMATE", "SILVER", f"null_{col_name}", status, "0 nulls", null_cnt, f"({pct}%)" if null_cnt > 0 else "")

# ── CHECK 14: Duplicates ───────────────────────────────────
print("\n📋 CHECK 14 — Duplicates")
tm_dupes = tm_silver_count - df_tm_silver.dropDuplicates(["weather_record_id"]).count()
tm_dup_pct = round((tm_dupes / tm_silver_count) * 100, 2) if tm_silver_count > 0 else 0
status = "PASS" if tm_dupes == 0 else ("WARN" if tm_dup_pct <= 1 else "FAIL")
log_check("TEAMMATE", "SILVER", "duplicates", status, "0", tm_dupes, f"({tm_dup_pct}%)" if tm_dupes > 0 else "")

# ── CHECK 15: Range Validation ─────────────────────────────
print("\n📋 CHECK 15 — Range Validation")
for col_name, (min_val, max_val) in range_checks.items():
    oor = df_tm_silver.filter((col(col_name) < min_val) | (col(col_name) > max_val)).count()
    pct = round((oor / tm_silver_count) * 100, 2) if tm_silver_count > 0 else 0
    status = "PASS" if oor == 0 else ("WARN" if pct <= 1 else "FAIL")
    log_check("TEAMMATE", "SILVER", f"range_{col_name}", status, f"{min_val}–{max_val}",
              "all in range" if oor == 0 else oor, f"({pct}%)" if oor > 0 else "")

# COMMAND ----------

# ── Load Teammate's Gold ───────────────────────────────────
print("\n📋 Loading Teammate's Gold tables (gold)...")
df_tm_fact    = spark.table(f"{CATALOG}.gold.fact_weather")
df_tm_loc     = spark.table(f"{CATALOG}.gold.dim_location")
df_tm_cond    = spark.table(f"{CATALOG}.gold.dim_condition")
df_tm_astro   = spark.table(f"{CATALOG}.gold.dim_astronomy")
df_tm_date    = spark.table(f"{CATALOG}.gold.dim_date")
tm_fact_count = df_tm_fact.count()

print(f"   fact_weather  : {tm_fact_count:,}")
print(f"   dim_location  : {df_tm_loc.count():,}")
print(f"   dim_condition : {df_tm_cond.count():,}")
print(f"   dim_astronomy : {df_tm_astro.count():,}")
print(f"   dim_date      : {df_tm_date.count():,}")

# ── CHECK 16: Fact Retention ───────────────────────────────
print("\n📋 CHECK 16 — Fact vs Silver Retention")
tm_clean   = df_tm_silver.filter(col("is_outlier") == False).count()
tm_ret_pct = round((tm_fact_count / tm_clean) * 100, 2) if tm_clean > 0 else 0
status = "PASS" if tm_ret_pct >= 95 else ("WARN" if tm_ret_pct >= 80 else "FAIL")
log_check("TEAMMATE", "GOLD", "fact_retention", status, ">=95%", f"{tm_ret_pct}%",
          f"(silver={tm_clean:,}, fact={tm_fact_count:,})")

# ── CHECK 17: Fact Null Checks ─────────────────────────────
print("\n📋 CHECK 17 — Fact Null Checks")
tm_fact_nulls = df_tm_fact.select([
    spark_sum(col(c).isNull().cast("int")).alias(c)
    for c in ["temperature_celsius", "humidity", "wind_kph",
              "pressure_mb", "uv_index", "air_quality_pm2_5"]
]).collect()[0].asDict()

for col_name, null_cnt in tm_fact_nulls.items():
    pct = round((null_cnt / tm_fact_count) * 100, 2) if tm_fact_count > 0 else 0
    status = "PASS" if null_cnt == 0 else ("WARN" if pct <= 2 else "FAIL")
    log_check("TEAMMATE", "GOLD", f"null_{col_name}", status, "0 nulls", null_cnt, f"({pct}%)" if null_cnt > 0 else "")

# ── CHECK 18: Dim PK Uniqueness ────────────────────────────
print("\n📋 CHECK 18 — Dim PK Uniqueness")
for tbl, df_d in {
    "dim_location"  : df_tm_loc,
    "dim_condition" : df_tm_cond,
    "dim_astronomy" : df_tm_astro,
    "dim_date"      : df_tm_date,
}.items():
    dupes = df_d.count() - df_d.dropDuplicates(["weather_record_id"]).count()
    log_check("TEAMMATE", "GOLD", f"pk_{tbl}",
              "PASS" if dupes == 0 else "FAIL",
              "unique", f"{df_d.count():,} rows" if dupes == 0 else f"{dupes} dupes!")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SECTION 3 — Cross-Pipeline Comparison

# COMMAND ----------

print("\n" + "=" * 65)
print("CROSS-PIPELINE COMPARISON")
print("=" * 65)

print(f"\n{'Metric':<35} {'YOUR Pipeline':>20} {'TEAMMATE':>15}")
print("-" * 72)
print(f"{'Silver table':<35} {'silver_v2':>20} {'silver':>15}")
print(f"{'Silver rows':<35} {your_silver_count:>20,} {tm_silver_count:>15,}")
print(f"{'Gold schema':<35} {'gold_analytics':>20} {'gold':>15}")
print(f"{'Fact rows':<35} {your_fact_count:>20,} {tm_fact_count:>15,}")
print(f"{'Dimensions':<35} {'4 (surrogate keys)':>20} {'4 (natural keys)':>15}")

diff_pct = round((abs(your_silver_count - tm_silver_count) / max(your_silver_count, tm_silver_count)) * 100, 2)
log_check("BOTH", "COMPARE", "silver_row_match",
          "PASS" if diff_pct <= 1 else "WARN",
          "<=1% diff", f"{diff_pct}% diff",
          f"(yours={your_silver_count:,}, teammate={tm_silver_count:,})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## SECTION 4 — DQ Summary + Log to Delta

# COMMAND ----------

total_checks = len(dq_results)
passed       = sum(1 for r in dq_results if r["status"] == "PASS")
warned       = sum(1 for r in dq_results if r["status"] == "WARN")
failed       = sum(1 for r in dq_results if r["status"] == "FAIL")
dq_score     = round((passed / total_checks) * 100, 1) if total_checks > 0 else 0

your_r = [r for r in dq_results if r["pipeline"] == "YOURS"]
tm_r   = [r for r in dq_results if r["pipeline"] == "TEAMMATE"]

print("\n" + "=" * 65)
print("FULL DATA QUALITY SUMMARY REPORT")
print("=" * 65)
print(f"\n📊 Overall:")
print(f"   Total checks : {total_checks}")
print(f"   ✅ PASSED    : {passed}")
print(f"   ⚠️  WARNED   : {warned}")
print(f"   ❌ FAILED    : {failed}")
print(f"   DQ Score     : {dq_score}%")
print(f"\n📊 Your Pipeline   — Passed: {sum(1 for r in your_r if r['status']=='PASS')} / {len(your_r)}")
print(f"📊 Teammate Pipeline — Passed: {sum(1 for r in tm_r if r['status']=='PASS')} / {len(tm_r)}")

if failed > 0:
    print(f"\n🚨 FAILED CHECKS:")
    for r in [r for r in dq_results if r["status"] == "FAIL"]:
        print(f"   ❌ [{r['pipeline']}|{r['layer']}] {r['check_name']}: {r['details']}")

if warned > 0:
    print(f"\n⚠️  WARNINGS:")
    for r in [r for r in dq_results if r["status"] == "WARN"]:
        print(f"   ⚠️  [{r['pipeline']}|{r['layer']}] {r['check_name']}: {r['details']}")

# COMMAND ----------

# ── Log to Delta ───────────────────────────────────────────
dq_schema = StructType([
    StructField("batch_date",  StringType(), True),
    StructField("pipeline",    StringType(), True),
    StructField("layer",       StringType(), True),
    StructField("check_name",  StringType(), True),
    StructField("status",      StringType(), True),
    StructField("expected",    StringType(), True),
    StructField("actual",      StringType(), True),
    StructField("details",     StringType(), True),
    StructField("checked_at",  StringType(), True),
])

df_dq_log = spark.createDataFrame(dq_results, schema=dq_schema)

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.data_quality")

DQ_PATH = "s3://global-weather-pipeline/data_quality/"
df_dq_log.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("batch_date") \
    .save(DQ_PATH)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.data_quality.dq_check_log
    USING DELTA LOCATION '{DQ_PATH}'
""")

print(f"\n✅ DQ results logged → {CATALOG}.data_quality.dq_check_log")
print(f"   Rows logged : {len(dq_results)}")
display(df_dq_log.orderBy("pipeline", "layer", "status"))

# COMMAND ----------

# ── DQ Gate ───────────────────────────────────────────────
if failed > 0:
    raise ValueError(
        f"🚨 DQ Gate FAILED — {failed} check(s) did not pass. "
        f"Check {CATALOG}.data_quality.dq_check_log for batch_date={batch_date}"
    )
else:
    print(f"\n✅ Data Quality Gate PASSED!")
    print(f"   DQ Score     : {dq_score}%")
    print(f"   Total checks : {total_checks}")
    print(f"   Both pipelines validated ✅")
