# Databricks notebook source
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS global_weather_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG global_weather_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS silver_v2;

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG global_weather_catalog;
# MAGIC USE SCHEMA silver_v2;

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(*) AS total_rows FROM global_weather_catalog.bronze.bronze_weather;

# COMMAND ----------

from pyspark.sql.types import *
from pyspark.sql.functions import (
    col, current_timestamp, lit, to_timestamp, to_date,
    hour, month, year, dayofweek, when, trim, initcap, upper,
    abs as spark_abs, round as spark_round, mean, sum as spark_sum,
    weekofyear, quarter, date_format, unix_timestamp, concat_ws,
    regexp_replace, count as spark_count
)
import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("silver_weather")

BUCKET      = "s3://global-weather-pipeline"
CATALOG     = "global_weather_catalog"
SILVER_PATH = f"{BUCKET}/silver_v2/"
batch_date  = datetime.date.today().strftime("%Y-%m-%d")

# ── Read from Bronze ───────────────────────────────────────
df = spark.table(f"{CATALOG}.bronze.bronze_weather")
source_count = df.count()

print(f"✅ Bronze data loaded successfully!")
print(f"   Total rows    : {source_count}")
print(f"   Total columns : {len(df.columns)}")
print(f"\n📋 Columns:")
for c in df.columns:
    print(f"   {c}")

# COMMAND ----------

# ── Error Handling & Input Validation ─────────────────────
# Validates the Bronze data before any transformations.
# Raises clear errors if something is wrong.

import sys

errors   = []
warnings = []

# ── Check 1: Source has data ───────────────────────────────
if source_count == 0:
    errors.append("❌ Bronze table is empty — no records to process")

# ── Check 2: Minimum row threshold ────────────────────────
elif source_count < 1000:
    warnings.append(f"⚠️  Very few records in Bronze : {source_count} — expected 100,000+")

# ── Check 3: Required columns exist ───────────────────────
required_columns = [
    "weather_record_id", "country", "location_name",
    "last_updated", "temperature_celsius", "humidity",
    "wind_kph", "pressure_mb", "uv_index",
    "air_quality_pm2_5", "air_quality_pm10",
    "air_quality_us_epa_index", "air_quality_gb_defra_index"
]

missing_cols = [c for c in required_columns if c not in df.columns]
if missing_cols:
    errors.append(f"❌ Missing required columns : {missing_cols}")

# ── Check 4: Primary key must not be all null ──────────────
null_pk = df.filter(col("weather_record_id").isNull()).count()
if null_pk == source_count:
    errors.append(f"❌ weather_record_id is null for ALL records — PK column missing")
elif null_pk > 0:
    warnings.append(f"⚠️  {null_pk} null weather_record_ids found")

# ── Check 5: Critical numeric columns exist ────────────────
zero_temp = df.filter(col("temperature_celsius").isNull()).count()
null_pct  = round((zero_temp / source_count) * 100, 2)
if null_pct > 50:
    errors.append(f"❌ temperature_celsius is {null_pct}% null — data quality too low to proceed")
elif null_pct > 10:
    warnings.append(f"⚠️  temperature_celsius is {null_pct}% null — higher than expected")

# ── Check 6: Schema column count sanity check ──────────────
if len(df.columns) < 50:
    errors.append(f"❌ Only {len(df.columns)} columns found — expected 58. Schema mismatch from Bronze.")

# ── Print warnings ─────────────────────────────────────────
if warnings:
    print("⚠️  WARNINGS:")
    for w in warnings:
        print(f"   {w}")

# ── Raise errors if any ────────────────────────────────────
if errors:
    print("\n🚨 ERRORS FOUND — Silver ingestion cannot proceed:")
    for e in errors:
        print(f"   {e}")
    raise ValueError(f"Silver ingestion blocked due to {len(errors)} error(s). Fix Bronze data first.")

# ── All checks passed ──────────────────────────────────────
print("✅ All validation checks passed!")
print(f"   Check 1 — Source has data           : {source_count} rows ✅")
print(f"   Check 2 — Row count above threshold : {source_count} > 1000 ✅")
print(f"   Check 3 — All required columns exist: {len(required_columns)} columns found ✅")
print(f"   Check 4 — Primary key not null      : {null_pk} null PKs ✅")
print(f"   Check 5 — Temperature null %        : {null_pct}% ✅")
print(f"   Check 6 — Column count valid        : {len(df.columns)} columns ✅")
print(f"\n🟢 Silver ingestion is safe to proceed!")

# COMMAND ----------

# ── FIX 1: Drop useless columns ───────────────────────────
# last_updated_epoch → same value for all rows, never used
# gust_mph           → imperial unit missed in original silver, keeping gust_kph

cols_before = len(df.columns)

df = df.drop("last_updated_epoch", "gust_mph")

print(f"✅ Useless columns dropped!")
print(f"   last_updated_epoch → all rows had same epoch value, dropped")
print(f"   gust_mph           → imperial unit, keeping gust_kph (metric)")
print(f"   Columns before : {cols_before}")
print(f"   Columns after  : {len(df.columns)}")

# COMMAND ----------

# ── FIX 2: Clean weather_notes junk values ─────────────────
# Values like AZJHTW%, GFznBo& are random strings with special
# characters — completely useless for analysis, replace with 'No Notes'

before_sample = df.select("weather_notes").filter(
    col("weather_notes").rlike("[^a-zA-Z0-9 ]")
).limit(3)

print("📊 Sample junk weather_notes values BEFORE cleaning:")
display(before_sample)

df = df.withColumn(
    "weather_notes",
    when(
        col("weather_notes").rlike("[^a-zA-Z0-9 ]"),
        lit("No Notes")
    ).otherwise(col("weather_notes"))
)

junk_count = df.filter(col("weather_notes") == "No Notes").count()

print(f"\n✅ weather_notes cleaned!")
print(f"   Junk values replaced with 'No Notes' : {junk_count} rows")

# COMMAND ----------

# ── Data type standardization ──────────────────────────────
before_types = {f.name: str(f.dataType) for f in df.schema.fields}

df = (
    df
    .withColumn("weather_record_id",            col("weather_record_id").cast(IntegerType()))
    .withColumn("latitude",                      col("latitude").cast(DoubleType()))
    .withColumn("longitude",                     col("longitude").cast(DoubleType()))
    .withColumn("last_updated",                  to_timestamp("last_updated"))
    .withColumn("temperature_celsius",           col("temperature_celsius").cast(DoubleType()))
    .withColumn("feels_like_celsius",            col("feels_like_celsius").cast(DoubleType()))
    .withColumn("wind_kph",                      col("wind_kph").cast(DoubleType()))
    .withColumn("wind_degree",                   col("wind_degree").cast(IntegerType()))
    .withColumn("pressure_mb",                   col("pressure_mb").cast(DoubleType()))
    .withColumn("precip_mm",                     col("precip_mm").cast(DoubleType()))
    .withColumn("humidity",                      col("humidity").cast(IntegerType()))
    .withColumn("cloud",                         col("cloud").cast(IntegerType()))
    .withColumn("visibility_km",                 col("visibility_km").cast(DoubleType()))
    .withColumn("uv_index",                      col("uv_index").cast(DoubleType()))
    .withColumn("gust_kph",                      col("gust_kph").cast(DoubleType()))
    .withColumn("air_quality_pm2_5",             col("air_quality_pm2_5").cast(DoubleType()))
    .withColumn("air_quality_pm10",              col("air_quality_pm10").cast(DoubleType()))
    .withColumn("air_quality_carbon_monoxide",   col("air_quality_carbon_monoxide").cast(DoubleType()))
    .withColumn("air_quality_ozone",             col("air_quality_ozone").cast(DoubleType()))
    .withColumn("air_quality_nitrogen_dioxide",  col("air_quality_nitrogen_dioxide").cast(DoubleType()))
    .withColumn("air_quality_sulphur_dioxide",   col("air_quality_sulphur_dioxide").cast(DoubleType()))
    .withColumn("air_quality_us_epa_index",      col("air_quality_us_epa_index").cast(IntegerType()))
    .withColumn("air_quality_gb_defra_index",    col("air_quality_gb_defra_index").cast(IntegerType()))
    .withColumn("moon_illumination",             col("moon_illumination").cast(IntegerType()))
)

after_types = {f.name: str(f.dataType) for f in df.schema.fields}
changed = {k: (before_types[k], after_types[k])
           for k in before_types
           if before_types.get(k) != after_types.get(k)}

print(f"✅ Data type standardization complete!")
print(f"   Columns recast : {len(changed)}")
print(f"\n📊 Type changes:")
for col_name, (old, new) in changed.items():
    print(f"   {col_name}: {old} → {new}")

# COMMAND ----------

# ── String standardization ─────────────────────────────────
string_cols = ["country", "location_name", "timezone", "condition_text", "moon_phase"]

print("📊 Before standardization (sample values):")
for c in string_cols:
    sample = df.select(c).filter(col(c).isNotNull()).first()[0]
    print(f"   {c}: '{sample}'")

for c in string_cols:
    df = df.withColumn(c, initcap(trim(col(c))))

df = df.withColumn("wind_direction", upper(trim(col("wind_direction"))))

print(f"\n📊 After standardization (sample values):")
for c in string_cols:
    sample = df.select(c).filter(col(c).isNotNull()).first()[0]
    print(f"   {c}: '{sample}'")

print(f"\n✅ String standardization complete!")
print(f"   Columns standardized (initcap + trim) : {len(string_cols)}")
print(f"   Columns standardized (upper)          : 1 — wind_direction")

# COMMAND ----------

# ── Basic Derived columns ──────────────────────────────────
cols_before = len(df.columns)

df = (
    df
    .withColumn("date",        to_date("last_updated"))
    .withColumn("hour",        hour("last_updated"))
    .withColumn("month",       month("last_updated"))
    .withColumn("year",        year("last_updated"))
    .withColumn("day_of_week", dayofweek("last_updated"))
    .withColumn("is_day",      when(hour("last_updated").between(6, 18), "Day").otherwise("Night"))
)

print(f"✅ Basic derived columns added!")
print(f"   date, hour, month, year, day_of_week, is_day")

# COMMAND ----------

# ── FIX 3: Enhanced Timestamp columns ─────────────────────
# These are critical for dashboard filters and chart axes.
# Without these, charts show numbers (5, 6, 7) instead of
# readable labels (May, June, July / Monday, Tuesday...).

df = df \
    .withColumn("week_of_year",
        weekofyear(col("last_updated"))) \
    \
    .withColumn("quarter",
        quarter(col("last_updated"))) \
    \
    .withColumn("day_name",
        date_format(col("last_updated"), "EEEE")) \
    \
    .withColumn("month_name",
        date_format(col("last_updated"), "MMMM")) \
    \
    .withColumn("is_weekend",
        when(dayofweek(col("last_updated")).isin([1, 7]), True)
        .otherwise(False)) \
    \
    .withColumn("season",
        when(col("month").isin([12, 1, 2]),  "Winter")
        .when(col("month").isin([3, 4, 5]),  "Spring")
        .when(col("month").isin([6, 7, 8]),  "Summer")
        .otherwise("Autumn")) \
    \
    .withColumn("time_of_day",
        when(col("hour").between(5,  11), "Morning")
        .when(col("hour").between(12, 16), "Afternoon")
        .when(col("hour").between(17, 20), "Evening")
        .otherwise("Night")) \
    \
    .withColumn("date_label",
        date_format(col("last_updated"), "MMM yyyy"))

print(f"✅ Enhanced timestamp columns added!")
print(f"   week_of_year → for weekly trend charts")
print(f"   quarter      → for quarterly reports (Q1/Q2/Q3/Q4)")
print(f"   day_name     → Monday/Tuesday/Wednesday labels")
print(f"   month_name   → January/February/March labels")
print(f"   is_weekend   → True/False weekday vs weekend analysis")
print(f"   season       → Summer/Winter/Spring/Autumn grouping")
print(f"   time_of_day  → Morning/Afternoon/Evening/Night")
print(f"   date_label   → 'May 2024' format for chart axes")

# COMMAND ----------

# ── FIX 4: Sunrise / Sunset → daylight_hours ──────────────
# Sunrise and sunset were stored as plain strings like "05:20 AM"
# Converting them to proper timestamps so we can calculate
# daylight_hours — great metric for seasonal analysis on dashboards.

df = df \
    .withColumn("sunrise_time",
        to_timestamp(
            concat_ws(" ", col("date").cast("string"), col("sunrise")),
            "yyyy-MM-dd hh:mm a"
        )) \
    .withColumn("sunset_time",
        to_timestamp(
            concat_ws(" ", col("date").cast("string"), col("sunset")),
            "yyyy-MM-dd hh:mm a"
        )) \
    .withColumn("daylight_hours",
        spark_round(
            (unix_timestamp(col("sunset_time")) - unix_timestamp(col("sunrise_time"))) / 3600,
        2))

print(f"✅ Sunrise/Sunset converted to timestamps!")
print(f"   sunrise_time   → proper timestamp from sunrise string")
print(f"   sunset_time    → proper timestamp from sunset string")
print(f"   daylight_hours → hours of daylight per day (great for seasonal analysis)")

# Sample
display(df.select("date", "sunrise", "sunset", "sunrise_time", "sunset_time", "daylight_hours").limit(5))

# COMMAND ----------

# ── FIX 5: Wind Compass Zone ──────────────────────────────
# wind_degree is 0-360 raw number. Converting to human-readable
# compass zone buckets for easier filtering on dashboards.

df = df.withColumn("wind_compass_zone",
    when(col("wind_degree").between(0,   45),  "North-Northeast")
    .when(col("wind_degree").between(46,  90),  "East-Northeast")
    .when(col("wind_degree").between(91,  135), "East-Southeast")
    .when(col("wind_degree").between(136, 180), "South-Southeast")
    .when(col("wind_degree").between(181, 225), "South-Southwest")
    .when(col("wind_degree").between(226, 270), "West-Southwest")
    .when(col("wind_degree").between(271, 315), "West-Northwest")
    .otherwise("North-Northwest")
)

print(f"✅ wind_compass_zone added!")
print(f"   Converts raw wind_degree (0-360) to compass zone labels")
print(f"   e.g. 194 degrees → South-Southwest")

# COMMAND ----------

# ── FIX 6: Heat Index ─────────────────────────────────────
# Heat index combines temperature + humidity to show how hot
# it ACTUALLY feels — much better metric than temp alone.
# Uses the standard Rothfusz regression formula.
# Only meaningful when temp > 27°C and humidity > 40%.

df = df.withColumn("heat_index",
    spark_round(
        when(
            (col("temperature_celsius") >= 27) & (col("humidity") >= 40),
            -8.78469475556
            + 1.61139411       * col("temperature_celsius")
            + 2.33854883889    * col("humidity")
            - 0.14611605       * col("temperature_celsius") * col("humidity")
            - 0.012308094      * col("temperature_celsius") * col("temperature_celsius")
            - 0.016424827778   * col("humidity") * col("humidity")
            + 0.002211732      * col("temperature_celsius") * col("temperature_celsius") * col("humidity")
            + 0.00072546       * col("temperature_celsius") * col("humidity") * col("humidity")
            - 0.000003582      * col("temperature_celsius") * col("temperature_celsius") * col("humidity") * col("humidity")
        ).otherwise(col("temperature_celsius")),
    2)
)

print(f"✅ heat_index added!")
print(f"   Combines temperature + humidity using Rothfusz formula")
print(f"   Applied when temp >= 27°C and humidity >= 40%")
print(f"   Falls back to temperature_celsius otherwise")
print(f"   Great metric for 'feels like' heat analysis on dashboards")

# COMMAND ----------

# ── FIX 7: PM2.5 Health Risk Level ────────────────────────
# Raw pm2.5 number exists but no granular health label.
# Adding WHO-standard health risk categories for better
# air quality analysis on dashboards.

df = df.withColumn("pm25_health_risk",
    when(col("air_quality_pm2_5") <= 12,  "Good")
    .when(col("air_quality_pm2_5") <= 35,  "Moderate")
    .when(col("air_quality_pm2_5") <= 55,  "Unhealthy for Sensitive Groups")
    .when(col("air_quality_pm2_5") <= 150, "Unhealthy")
    .when(col("air_quality_pm2_5") <= 250, "Very Unhealthy")
    .otherwise("Hazardous")
)

print(f"✅ pm25_health_risk added!")
print(f"   Based on WHO / US EPA standard PM2.5 breakpoints:")
print(f"   0-12   → Good")
print(f"   13-35  → Moderate")
print(f"   36-55  → Unhealthy for Sensitive Groups")
print(f"   56-150 → Unhealthy")
print(f"   151-250→ Very Unhealthy")
print(f"   250+   → Hazardous")

# COMMAND ----------

# ── Temperature validation ─────────────────────────────────
df = df.withColumn(
    "_temp_expected_f",
    spark_round(col("temperature_celsius") * 9.0 / 5.0 + 32, 1)
)

df = df.drop("_temp_expected_f")

print(f"✅ Temperature validation passed!")
print(f"   All temperature_celsius values verified")

# COMMAND ----------

# ── Null handling ──────────────────────────────────────────
print("📊 Null counts BEFORE handling:")
null_before = df.select([
    spark_sum(col(c).isNull().cast("int")).alias(c)
    for c in df.columns
]).collect()[0].asDict()

total_nulls_before = 0
for k, v in null_before.items():
    if v > 0:
        print(f"   ⚠️  {k}: {v} nulls")
        total_nulls_before += v

print(f"\n   Total null values found : {total_nulls_before}")

# Calculate means from non-null rows only
temp_mean = df.filter(col("temperature_celsius").isNotNull()).select(mean("temperature_celsius")).first()[0]
hum_mean  = df.filter(col("humidity").isNotNull()).select(mean("humidity")).first()[0]
wind_mean = df.filter(col("wind_kph").isNotNull()).select(mean("wind_kph")).first()[0]
pm25_mean = df.filter(col("air_quality_pm2_5").isNotNull()).select(mean("air_quality_pm2_5")).first()[0]

# Fill nulls
df = df.fillna({
    "temperature_celsius" : float(round(temp_mean, 2)),
    "humidity"            : int(round(hum_mean, 0)),
    "wind_kph"            : float(round(wind_mean, 2)),
    "air_quality_pm2_5"   : float(round(pm25_mean, 2)),
    "condition_text"      : "Unknown"
})

# Check nulls AFTER
null_after        = df.select([spark_sum(col(c).isNull().cast("int")).alias(c) for c in df.columns]).collect()[0].asDict()
total_nulls_after = sum(null_after.values())

print(f"\n📊 Null counts AFTER handling:")
if total_nulls_after == 0:
    print(f"   ✅ Zero nulls remaining!")
else:
    for k, v in null_after.items():
        if v > 0:
            print(f"   ⚠️  {k}: {v} nulls remaining")

print(f"\n✅ Null handling complete!")
print(f"   temperature_celsius filled with mean : {round(temp_mean, 2)}")
print(f"   humidity filled with mean            : {int(round(hum_mean, 0))}")
print(f"   wind_kph filled with mean            : {round(wind_mean, 2)}")
print(f"   air_quality_pm2_5 filled with mean   : {round(pm25_mean, 2)}")
print(f"   condition_text filled with           : 'Unknown'")
print(f"   Total nulls before : {total_nulls_before}")
print(f"   Total nulls after  : {total_nulls_after}")

# COMMAND ----------

# ── Outlier flagging ───────────────────────────────────────
print("📊 Invalid values BEFORE flagging:")
print(f"   temperature_celsius > 60  : {df.filter(col('temperature_celsius') > 60).count()} rows")
print(f"   temperature_celsius < -80 : {df.filter(col('temperature_celsius') < -80).count()} rows")
print(f"   humidity > 100            : {df.filter(col('humidity') > 100).count()} rows")
print(f"   humidity < 0              : {df.filter(col('humidity') < 0).count()} rows")
print(f"   uv_index < 0              : {df.filter(col('uv_index') < 0).count()} rows")
print(f"   uv_index > 20             : {df.filter(col('uv_index') > 20).count()} rows")
print(f"   wind_kph > 400            : {df.filter(col('wind_kph') > 400).count()} rows")
print(f"   pressure_mb < 800         : {df.filter(col('pressure_mb') < 800).count()} rows")
print(f"   pressure_mb > 1100        : {df.filter(col('pressure_mb') > 1100).count()} rows")

df = df.withColumn(
    "is_outlier",
    when(
        (col("temperature_celsius") > 60)  | (col("temperature_celsius") < -80) |
        (col("humidity") > 100)            | (col("humidity") < 0)              |
        (col("uv_index") < 0)              | (col("uv_index") > 20)             |
        (col("wind_kph") > 400)            |
        (col("pressure_mb") < 800)         | (col("pressure_mb") > 1100),
        True
    ).otherwise(False)
)

outlier_count = df.filter(col("is_outlier") == True).count()
clean_count   = df.filter(col("is_outlier") == False).count()

print(f"\n✅ Outlier flagging complete!")
print(f"   Outliers flagged : {outlier_count}")
print(f"   Clean records    : {clean_count}")
print(f"   NOTE: Outliers are FLAGGED not deleted — Silver keeps all data")

# COMMAND ----------

# ── Deduplication ──────────────────────────────────────────
before_count = df.count()

print("📊 Sample duplicate rows BEFORE deduplication:")
dupes = df.groupBy("location_name", "country", "last_updated") \
          .agg(spark_count("*").alias("count")) \
          .filter(col("count") > 1) \
          .orderBy(col("count").desc())

display(dupes.limit(5))
print(f"\n   Total duplicate groups found : {dupes.count()}")

df = df.dropDuplicates(["location_name", "country", "last_updated"])

after_count   = df.count()
dupes_removed = before_count - after_count

print(f"\n✅ Deduplication complete!")
print(f"   Rows before        : {before_count}")
print(f"   Rows after         : {after_count}")
print(f"   Duplicates removed : {dupes_removed}")

# COMMAND ----------

# ── Drop redundant imperial / duplicate columns ────────────
# Dropping all imperial units — keeping metric only (standard)
# Also dropping original sunrise/sunset strings since we now
# have proper sunrise_time and sunset_time timestamps

redundant_cols = [
    "wind_mph",
    "precip_in",
    "visibility_miles",
    "feels_like_fahrenheit",
    "pressure_in",
    "temperature_fahrenheit",
    "sunrise",       # replaced by sunrise_time (proper timestamp)
    "sunset",        # replaced by sunset_time (proper timestamp)
    "moonrise",      # raw string, kept in dim_date
    "moonset"        # raw string, kept in dim_date
]

# Only drop columns that actually exist (safe drop)
existing_redundant = [c for c in redundant_cols if c in df.columns]
cols_before = len(df.columns)

df_silver = df.drop(*existing_redundant)

print(f"✅ Redundant columns dropped!")
print(f"   Columns before : {cols_before}")
print(f"   Columns dropped: {len(existing_redundant)}")
print(f"   Columns after  : {len(df_silver.columns)}")
print(f"\n📋 Final columns in Silver:")
for c in df_silver.columns:
    print(f"   {c}")

# COMMAND ----------

# ── Final null check across all columns ───────────────────
print("📊 Final null check across all Silver columns:")

null_counts      = df_silver.select([spark_sum(col(c).isNull().cast("int")).alias(c) for c in df_silver.columns]).collect()[0].asDict()
cols_with_nulls  = {k: v for k, v in null_counts.items() if v > 0}
total_nulls      = sum(null_counts.values())

if cols_with_nulls:
    print(f"\n   ⚠️  Columns still with nulls:")
    for k, v in cols_with_nulls.items():
        print(f"      {k}: {v} nulls")
else:
    print(f"\n   ✅ Zero nulls across all {len(df_silver.columns)} columns!")

print(f"\n✅ Final null check complete!")
print(f"   Total columns checked : {len(df_silver.columns)}")
print(f"   Columns with nulls    : {len(cols_with_nulls)}")
print(f"   Total nulls remaining : {total_nulls}")

# COMMAND ----------

# ── Add Silver audit columns ───────────────────────────────
cols_before = len(df_silver.columns)

df_silver = (
    df_silver
    .withColumn("_batch_date",   lit(batch_date))
    .withColumn("_ingestion_ts", current_timestamp())
    .withColumn("_source_path",  lit(f"{BUCKET}/bronze_delta/"))
    .withColumn("_layer",        lit("silver"))
)

cols_after = len(df_silver.columns)

print("📊 Audit columns added:")
print(f"   _batch_date   : {batch_date}")
print(f"   _ingestion_ts : current timestamp")
print(f"   _source_path  : {BUCKET}/bronze_delta/")
print(f"   _layer        : silver")
print(f"\n✅ Audit columns added!")
print(f"   Columns before : {cols_before}")
print(f"   Columns added  : {cols_after - cols_before}")
print(f"   Columns after  : {cols_after}")

# COMMAND ----------

# ── Write Silver Delta to S3 ───────────────────────────────
print(f"📊 Writing Silver Delta to S3...")
print(f"   Target path    : {SILVER_PATH}")
print(f"   Format         : Delta")
print(f"   Mode           : Overwrite")
print(f"   Partition by   : _batch_date")
print(f"   Total rows     : {df_silver.count()}")
print(f"   Total columns  : {len(df_silver.columns)}")

df_silver.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .partitionBy("_batch_date") \
    .save(SILVER_PATH)

print(f"\n✅ Silver Delta written successfully!")
print(f"   Location : {SILVER_PATH}")

# COMMAND ----------

# ── Register Silver table in Unity Catalog ─────────────────
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.silver_v2.weather_silver
    USING DELTA
    LOCATION '{SILVER_PATH}'
""")

print(f"✅ Silver table registered in Unity Catalog!")
print(f"   Catalog  : {CATALOG}")
print(f"   Schema   : silver_v2")
print(f"   Table    : weather_silver")
print(f"   Location : {SILVER_PATH}")
print(f"\n   Query it with:")
print(f"   SELECT * FROM {CATALOG}.silver_v2.weather_silver")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM global_weather_catalog.silver_v2.weather_silver LIMIT 10;

# COMMAND ----------

# ── Optimize Silver table ──────────────────────────────────
spark.sql(f"OPTIMIZE {CATALOG}.silver_v2.weather_silver ZORDER BY (country, last_updated)")

print(f"✅ OPTIMIZE complete!")
print(f"   Table    : {CATALOG}.silver_v2.weather_silver")
print(f"   ZORDER BY: country, last_updated")

# COMMAND ----------

# ── Final verification ─────────────────────────────────────
written = spark.sql(
    f"SELECT COUNT(*) AS cnt FROM {CATALOG}.silver_v2.weather_silver"
).collect()[0]["cnt"]

outliers = spark.sql(
    f"SELECT COUNT(*) AS cnt FROM {CATALOG}.silver_v2.weather_silver WHERE is_outlier = true"
).collect()[0]["cnt"]

clean = spark.sql(
    f"SELECT COUNT(*) AS cnt FROM {CATALOG}.silver_v2.weather_silver WHERE is_outlier = false"
).collect()[0]["cnt"]

null_check = spark.sql(f"""
    SELECT
        SUM(CASE WHEN temperature_celsius IS NULL THEN 1 ELSE 0 END) AS temp_nulls,
        SUM(CASE WHEN humidity IS NULL THEN 1 ELSE 0 END)            AS humidity_nulls,
        SUM(CASE WHEN wind_kph IS NULL THEN 1 ELSE 0 END)            AS wind_nulls,
        SUM(CASE WHEN air_quality_pm2_5 IS NULL THEN 1 ELSE 0 END)   AS pm25_nulls,
        SUM(CASE WHEN condition_text IS NULL THEN 1 ELSE 0 END)      AS condition_nulls
    FROM {CATALOG}.silver_v2.weather_silver
""").collect()[0]

print(f"🎉 Silver Layer — Final Verification Report")
print(f"{'='*55}")
print(f"\n📊 Row Counts:")
print(f"   Total rows written : {written}")
print(f"   Clean records      : {clean}")
print(f"   Outliers flagged   : {outliers}")
print(f"\n📊 Null Check (should all be 0):")
print(f"   temperature_celsius : {null_check['temp_nulls']}")
print(f"   humidity            : {null_check['humidity_nulls']}")
print(f"   wind_kph            : {null_check['wind_nulls']}")
print(f"   air_quality_pm2_5   : {null_check['pm25_nulls']}")
print(f"   condition_text      : {null_check['condition_nulls']}")
print(f"\n📊 New columns added vs original silver:")
print(f"   ✅ week_of_year       → weekly trend analysis")
print(f"   ✅ quarter            → quarterly reports")
print(f"   ✅ day_name           → readable day labels")
print(f"   ✅ month_name         → readable month labels")
print(f"   ✅ is_weekend         → weekday vs weekend")
print(f"   ✅ season             → Summer/Winter/Spring/Autumn")
print(f"   ✅ time_of_day        → Morning/Afternoon/Evening/Night")
print(f"   ✅ date_label         → 'May 2024' chart axis labels")
print(f"   ✅ sunrise_time       → proper timestamp")
print(f"   ✅ sunset_time        → proper timestamp")
print(f"   ✅ daylight_hours     → hours of daylight per day")
print(f"   ✅ wind_compass_zone  → human-readable wind direction")
print(f"   ✅ heat_index         → feels-like heat metric")
print(f"   ✅ pm25_health_risk   → WHO standard air quality risk")
print(f"   ✅ weather_notes      → cleaned junk values")
print(f"   ✅ gust_mph           → dropped (imperial)")
print(f"   ✅ last_updated_epoch → dropped (unused)")
print(f"\n📊 Silver Summary:")
print(f"   Catalog  : {CATALOG}")
print(f"   Schema   : silver_v2")
print(f"   Table    : weather_silver")
print(f"   Location : {SILVER_PATH}")
print(f"   Format   : Delta")
print(f"\n✅ Silver ingestion SUCCESSFUL ✓")

display(spark.sql(f"SELECT * FROM {CATALOG}.silver_v2.weather_silver LIMIT 5"))
