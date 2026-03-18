# Databricks notebook source
# MAGIC %md
# MAGIC # 06 — PyTest Unit Tests
# MAGIC **Pipeline:** Real-Time-Weather-Data-Pipeline
# MAGIC **Purpose:** Unit tests for Bronze, Silver and Gold layer transformations
# MAGIC **Framework:** pytest + pyspark
# MAGIC
# MAGIC **Test Coverage:**
# MAGIC - Bronze: Schema, null checks, column renaming, audit columns
# MAGIC - Silver: Type casting, string standardization, outlier detection, derived columns
# MAGIC - Gold: Star schema integrity, FK checks, dimension uniqueness
# MAGIC - Utilities: DQ checks, range validation, duplicate detection

# COMMAND ----------

# MAGIC %pip install pytest pytest-html

# COMMAND ----------

import pytest
import datetime
import sys
import os
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, lit, when, rand
from io import StringIO

# ── Test results tracker ───────────────────────────────────
test_results = []

def record(test_name, status, message=""):
    icon = "✅" if status == "PASS" else "❌"
    test_results.append({
        "test_name" : test_name,
        "status"    : status,
        "message"   : message
    })
    print(f"   {icon} {test_name}: {message if message else status}")

CATALOG = "global_weather_catalog"
BUCKET  = "s3://global-weather-pipeline"

print("✅ PyTest framework initialized!")
print(f"   Catalog : {CATALOG}")
print(f"   Python  : {sys.version}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## TEST SUITE 1 — Bronze Layer Tests

# COMMAND ----------

print("=" * 60)
print("TEST SUITE 1 — Bronze Layer")
print("=" * 60)

# ── Test 1.1: Bronze table exists ─────────────────────────
def test_bronze_table_exists():
    try:
        df = spark.table(f"{CATALOG}.bronze.bronze_weather")
        assert df is not None
        record("test_bronze_table_exists", "PASS", f"Table exists")
        return df
    except Exception as e:
        record("test_bronze_table_exists", "FAIL", str(e))
        return None

df_bronze = test_bronze_table_exists()

# COMMAND ----------

# ── Test 1.2: Bronze row count ─────────────────────────────
def test_bronze_row_count(df):
    try:
        count = df.count()
        assert count > 0, f"Bronze table is empty"
        assert count > 1000, f"Too few rows: {count}"
        record("test_bronze_row_count", "PASS", f"{count:,} rows")
    except AssertionError as e:
        record("test_bronze_row_count", "FAIL", str(e))
    except Exception as e:
        record("test_bronze_row_count", "FAIL", str(e))

if df_bronze:
    test_bronze_row_count(df_bronze)

# COMMAND ----------

# ── Test 1.3: Required columns present ────────────────────
def test_bronze_required_columns(df):
    required = [
        "weather_record_id", "country", "location_name",
        "last_updated", "temperature_celsius", "humidity",
        "wind_kph", "pressure_mb", "uv_index",
        "air_quality_pm2_5", "air_quality_pm10",
        "_batch_date", "_ingestion_ts", "_source_path", "_stream_name"
    ]
    try:
        missing = [c for c in required if c not in df.columns]
        assert len(missing) == 0, f"Missing columns: {missing}"
        record("test_bronze_required_columns", "PASS", f"All {len(required)} columns present")
    except AssertionError as e:
        record("test_bronze_required_columns", "FAIL", str(e))

if df_bronze:
    test_bronze_required_columns(df_bronze)

# COMMAND ----------

# ── Test 1.4: Special character columns renamed ────────────
def test_bronze_column_renaming(df):
    bad_cols = [
        "air_quality_PM2.5", "air_quality_PM10",
        "air_quality_us-epa-index", "air_quality_gb-defra-index"
    ]
    good_cols = [
        "air_quality_pm2_5", "air_quality_pm10",
        "air_quality_us_epa_index", "air_quality_gb_defra_index"
    ]
    try:
        # Bad cols should NOT exist
        bad_present = [c for c in bad_cols if c in df.columns]
        assert len(bad_present) == 0, f"Special char columns still present: {bad_present}"
        # Good cols SHOULD exist
        good_missing = [c for c in good_cols if c not in df.columns]
        assert len(good_missing) == 0, f"Renamed columns missing: {good_missing}"
        record("test_bronze_column_renaming", "PASS", "All special char columns renamed correctly")
    except AssertionError as e:
        record("test_bronze_column_renaming", "FAIL", str(e))

if df_bronze:
    test_bronze_column_renaming(df_bronze)

# COMMAND ----------

# ── Test 1.5: Column names are lowercase ──────────────────
def test_bronze_lowercase_columns(df):
    try:
        non_lower = [c for c in df.columns if c != c.lower() and not c.startswith("_")]
        assert len(non_lower) == 0, f"Non-lowercase columns: {non_lower}"
        record("test_bronze_lowercase_columns", "PASS", f"All {len(df.columns)} columns are lowercase")
    except AssertionError as e:
        record("test_bronze_lowercase_columns", "FAIL", str(e))

if df_bronze:
    test_bronze_lowercase_columns(df_bronze)

# COMMAND ----------

# ── Test 1.6: Audit columns have valid values ──────────────
def test_bronze_audit_columns(df):
    try:
        sample = df.select(
            "_batch_date", "_ingestion_ts",
            "_source_path", "_stream_name"
        ).limit(1).collect()[0]

        assert sample["_batch_date"] is not None, "_batch_date is null"
        assert sample["_ingestion_ts"] is not None, "_ingestion_ts is null"
        assert sample["_source_path"] is not None, "_source_path is null"
        assert sample["_stream_name"] == "weather", f"_stream_name should be 'weather', got '{sample['_stream_name']}'"
        record("test_bronze_audit_columns", "PASS", "All audit columns valid")
    except AssertionError as e:
        record("test_bronze_audit_columns", "FAIL", str(e))
    except Exception as e:
        record("test_bronze_audit_columns", "FAIL", str(e))

if df_bronze:
    test_bronze_audit_columns(df_bronze)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TEST SUITE 2 — Silver Layer Tests (My Pipeline)

# COMMAND ----------

print("\n" + "=" * 60)
print("TEST SUITE 2 — Silver Layer (silver_v2)")
print("=" * 60)

# ── Test 2.1: Silver table exists ─────────────────────────
def test_silver_table_exists():
    try:
        df = spark.table(f"{CATALOG}.silver_v2.weather_silver")
        assert df is not None
        record("test_silver_table_exists", "PASS", "silver_v2.weather_silver exists")
        return df
    except Exception as e:
        record("test_silver_table_exists", "FAIL", str(e))
        return None

df_silver = test_silver_table_exists()

# COMMAND ----------

# ── Test 2.2: Silver has more cols than Bronze ─────────────
def test_silver_column_enrichment(df_s, df_b):
    try:
        assert len(df_s.columns) > len(df_b.columns), \
            f"Silver ({len(df_s.columns)} cols) should have more columns than Bronze ({len(df_b.columns)} cols)"
        record("test_silver_column_enrichment", "PASS",
               f"Silver has {len(df_s.columns)} cols vs Bronze {len(df_b.columns)} cols")
    except AssertionError as e:
        record("test_silver_column_enrichment", "FAIL", str(e))

if df_silver and df_bronze:
    test_silver_column_enrichment(df_silver, df_bronze)

# COMMAND ----------

# ── Test 2.3: Data type casting ───────────────────────────
def test_silver_data_types(df):
    expected_types = {
        "weather_record_id"   : "IntegerType",
        "latitude"            : "DoubleType",
        "longitude"           : "DoubleType",
        "temperature_celsius" : "DoubleType",
        "humidity"            : "IntegerType",
        "wind_kph"            : "DoubleType",
        "pressure_mb"         : "DoubleType",
        "uv_index"            : "DoubleType",
        "air_quality_pm2_5"   : "DoubleType",
    }
    try:
        schema_map = {f.name: type(f.dataType).__name__ for f in df.schema.fields}
        wrong_types = {
            col: f"expected {exp}, got {schema_map.get(col, 'MISSING')}"
            for col, exp in expected_types.items()
            if schema_map.get(col) != exp
        }
        assert len(wrong_types) == 0, f"Wrong data types: {wrong_types}"
        record("test_silver_data_types", "PASS", f"All {len(expected_types)} types correct")
    except AssertionError as e:
        record("test_silver_data_types", "FAIL", str(e))

if df_silver:
    test_silver_data_types(df_silver)

# COMMAND ----------

# ── Test 2.4: No critical nulls ───────────────────────────
def test_silver_no_critical_nulls(df):
    critical = [
        "country", "location_name", "temperature_celsius",
        "humidity", "wind_kph", "condition_text"
    ]
    try:
        from pyspark.sql.functions import sum as spark_sum
        null_counts = df.select([
            spark_sum(col(c).isNull().cast("int")).alias(c)
            for c in critical
        ]).collect()[0].asDict()
        nulls_found = {k: v for k, v in null_counts.items() if v > 0}
        assert len(nulls_found) == 0, f"Nulls found in critical cols: {nulls_found}"
        record("test_silver_no_critical_nulls", "PASS", f"Zero nulls in {len(critical)} critical columns")
    except AssertionError as e:
        record("test_silver_no_critical_nulls", "FAIL", str(e))

if df_silver:
    test_silver_no_critical_nulls(df_silver)

# COMMAND ----------

# ── Test 2.5: is_outlier column exists and is boolean ─────
def test_silver_outlier_flag(df):
    try:
        assert "is_outlier" in df.columns, "is_outlier column missing"
        schema_map = {f.name: type(f.dataType).__name__ for f in df.schema.fields}
        assert schema_map["is_outlier"] == "BooleanType", \
            f"is_outlier should be BooleanType, got {schema_map['is_outlier']}"
        outlier_vals = {r["is_outlier"] for r in df.select("is_outlier").distinct().collect()}
        assert outlier_vals.issubset({True, False}), \
            f"is_outlier has unexpected values: {outlier_vals}"
        record("test_silver_outlier_flag", "PASS", f"is_outlier valid — values: {outlier_vals}")
    except AssertionError as e:
        record("test_silver_outlier_flag", "FAIL", str(e))

if df_silver:
    test_silver_outlier_flag(df_silver)

# COMMAND ----------

# ── Test 2.6: Season column has valid values ───────────────
def test_silver_season_values(df):
    valid_seasons = {"Spring", "Summer", "Autumn", "Winter"}
    try:
        assert "season" in df.columns, "season column missing"
        actual_seasons = {r["season"] for r in df.select("season").distinct().collect()
                         if r["season"] is not None}
        invalid = actual_seasons - valid_seasons
        assert len(invalid) == 0, f"Invalid season values: {invalid}"
        assert len(actual_seasons) == 4, f"Expected 4 seasons, got: {actual_seasons}"
        record("test_silver_season_values", "PASS", f"All 4 seasons valid: {sorted(actual_seasons)}")
    except AssertionError as e:
        record("test_silver_season_values", "FAIL", str(e))

if df_silver:
    test_silver_season_values(df_silver)

# COMMAND ----------

# ── Test 2.7: Temperature range validation ────────────────
def test_silver_temperature_range(df):
    try:
        out_of_range = df.filter(
            (col("temperature_celsius") < -90) |
            (col("temperature_celsius") > 60)
        ).count()
        total = df.count()
        pct   = round((out_of_range / total) * 100, 2)
        assert pct <= 1, f"{pct}% rows have temperature out of range (-90 to 60°C)"
        record("test_silver_temperature_range", "PASS",
               f"Only {out_of_range} rows ({pct}%) outside valid range")
    except AssertionError as e:
        record("test_silver_temperature_range", "FAIL", str(e))

if df_silver:
    test_silver_temperature_range(df_silver)

# COMMAND ----------

# ── Test 2.8: Humidity range 0-100 ───────────────────────
def test_silver_humidity_range(df):
    try:
        invalid = df.filter(
            (col("humidity") < 0) | (col("humidity") > 100)
        ).count()
        assert invalid == 0, f"{invalid} rows have humidity outside 0-100 range"
        record("test_silver_humidity_range", "PASS", "All humidity values in 0-100 range")
    except AssertionError as e:
        record("test_silver_humidity_range", "FAIL", str(e))

if df_silver:
    test_silver_humidity_range(df_silver)

# COMMAND ----------

# ── Test 2.9: Derived columns exist ───────────────────────
def test_silver_derived_columns(df):
    derived = [
        "heat_index", "wind_compass_zone", "pm25_health_risk",
        "daylight_hours", "season", "temperature_category",
        "humidity_level", "wind_intensity_level"
    ]
    try:
        missing = [c for c in derived if c not in df.columns]
        assert len(missing) == 0, f"Missing derived columns: {missing}"
        record("test_silver_derived_columns", "PASS", f"All {len(derived)} derived columns present")
    except AssertionError as e:
        record("test_silver_derived_columns", "FAIL", str(e))

if df_silver:
    test_silver_derived_columns(df_silver)

# COMMAND ----------

# ── Test 2.10: No duplicates on primary key ───────────────
def test_silver_no_pk_duplicates(df):
    try:
        total    = df.count()
        distinct = df.dropDuplicates(["weather_record_id"]).count()
        dupes    = total - distinct
        dup_pct  = round((dupes / total) * 100, 2)
        assert dup_pct <= 1, f"{dupes} duplicate weather_record_ids ({dup_pct}%)"
        record("test_silver_no_pk_duplicates", "PASS", f"{dupes} duplicates ({dup_pct}%)")
    except AssertionError as e:
        record("test_silver_no_pk_duplicates", "FAIL", str(e))

if df_silver:
    test_silver_no_pk_duplicates(df_silver)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TEST SUITE 3 — Gold Layer Tests (My Pipeline)

# COMMAND ----------

print("\n" + "=" * 60)
print("TEST SUITE 3 — Gold Layer (gold_analytics)")
print("=" * 60)

# ── Load all Gold tables ───────────────────────────────────
def load_gold_tables():
    try:
        tables = {
            "fact"  : spark.table(f"{CATALOG}.gold_analytics.fact_weather_events"),
            "loc"   : spark.table(f"{CATALOG}.gold_analytics.dim_location"),
            "date"  : spark.table(f"{CATALOG}.gold_analytics.dim_date"),
            "cond"  : spark.table(f"{CATALOG}.gold_analytics.dim_weather_condition"),
            "aq"    : spark.table(f"{CATALOG}.gold_analytics.dim_air_quality"),
        }
        record("test_gold_tables_exist", "PASS", "All 5 gold_analytics tables loaded")
        return tables
    except Exception as e:
        record("test_gold_tables_exist", "FAIL", str(e))
        return None

gold = load_gold_tables()

# COMMAND ----------

# ── Test 3.1: Fact table row count ────────────────────────
def test_gold_fact_row_count(g):
    try:
        fact_count   = g["fact"].count()
        silver_clean = df_silver.filter(col("is_outlier") == False).count() if df_silver else 0
        assert fact_count > 0, "fact_weather_events is empty"
        if silver_clean > 0:
            retention = round((fact_count / silver_clean) * 100, 2)
            assert retention >= 80, f"Too many records lost: only {retention}% retained"
            record("test_gold_fact_row_count", "PASS",
                   f"{fact_count:,} rows ({retention}% of clean silver)")
        else:
            record("test_gold_fact_row_count", "PASS", f"{fact_count:,} rows")
    except AssertionError as e:
        record("test_gold_fact_row_count", "FAIL", str(e))

if gold:
    test_gold_fact_row_count(gold)

# COMMAND ----------

# ── Test 3.2: No null foreign keys ────────────────────────
def test_gold_no_null_fks(g):
    fk_cols = ["location_id", "date_id", "condition_id", "air_quality_id"]
    try:
        from pyspark.sql.functions import sum as spark_sum
        fk_nulls = g["fact"].select([
            spark_sum(col(c).isNull().cast("int")).alias(c)
            for c in fk_cols
        ]).collect()[0].asDict()
        null_fks = {k: v for k, v in fk_nulls.items() if v > 0}
        assert len(null_fks) == 0, f"Null FKs found: {null_fks}"
        record("test_gold_no_null_fks", "PASS", f"All {len(fk_cols)} FKs are non-null")
    except AssertionError as e:
        record("test_gold_no_null_fks", "FAIL", str(e))

if gold:
    test_gold_no_null_fks(gold)

# COMMAND ----------

# ── Test 3.3: Dimension PK uniqueness ─────────────────────
def test_gold_dim_pk_uniqueness(g):
    dim_pks = {
        "loc"  : "location_id",
        "date" : "date_id",
        "cond" : "condition_id",
        "aq"   : "air_quality_id",
    }
    try:
        all_unique = True
        details    = []
        for dim_key, pk_col in dim_pks.items():
            total    = g[dim_key].count()
            distinct = g[dim_key].dropDuplicates([pk_col]).count()
            dupes    = total - distinct
            if dupes > 0:
                all_unique = False
                details.append(f"{dim_key}: {dupes} dupes")
        assert all_unique, f"Duplicate PKs found: {details}"
        record("test_gold_dim_pk_uniqueness", "PASS", "All dimension PKs unique")
    except AssertionError as e:
        record("test_gold_dim_pk_uniqueness", "FAIL", str(e))

if gold:
    test_gold_dim_pk_uniqueness(gold)

# COMMAND ----------

# ── Test 3.4: dim_location coordinate validity ────────────
def test_gold_coordinate_validity(g):
    try:
        invalid = g["loc"].filter(
            (col("latitude")  < -90)  | (col("latitude")  > 90)  |
            (col("longitude") < -180) | (col("longitude") > 180)
        ).count()
        assert invalid == 0, f"{invalid} locations have invalid coordinates"
        record("test_gold_coordinate_validity", "PASS", "All coordinates valid")
    except AssertionError as e:
        record("test_gold_coordinate_validity", "FAIL", str(e))

if gold:
    test_gold_coordinate_validity(gold)

# COMMAND ----------

# ── Test 3.5: dim_date has all 4 seasons ──────────────────
def test_gold_dim_date_seasons(g):
    try:
        seasons = {r["season"] for r in g["date"].select("season").distinct().collect()}
        missing = {"Spring", "Summer", "Autumn", "Winter"} - seasons
        assert len(missing) == 0, f"Missing seasons in dim_date: {missing}"
        record("test_gold_dim_date_seasons", "PASS", f"All 4 seasons in dim_date")
    except AssertionError as e:
        record("test_gold_dim_date_seasons", "FAIL", str(e))

if gold:
    test_gold_dim_date_seasons(gold)

# COMMAND ----------

# ── Test 3.6: Fact measure ranges ─────────────────────────
def test_gold_fact_measure_ranges(g):
    range_checks = {
        "temperature_celsius" : (-90, 60),
        "humidity"            : (0, 100),
        "uv_index"            : (0, 20),
        "pressure_mb"         : (870, 1084),
    }
    try:
        violations = {}
        for col_name, (min_v, max_v) in range_checks.items():
            cnt = g["fact"].filter(
                (col(col_name) < min_v) | (col(col_name) > max_v)
            ).count()
            if cnt > 0:
                violations[col_name] = cnt
        assert len(violations) == 0, f"Range violations found: {violations}"
        record("test_gold_fact_measure_ranges", "PASS",
               f"All {len(range_checks)} measures within valid ranges")
    except AssertionError as e:
        record("test_gold_fact_measure_ranges", "FAIL", str(e))

if gold:
    test_gold_fact_measure_ranges(gold)

# COMMAND ----------

# MAGIC %md
# MAGIC ## TEST SUITE 4 — Teammate's Pipeline Tests

# COMMAND ----------

print("\n" + "=" * 60)
print("TEST SUITE 4 — Teammate's Pipeline (silver + gold)")
print("=" * 60)

# ── Test 4.1: Teammate Silver exists ──────────────────────
def test_tm_silver_exists():
    try:
        df = spark.table(f"{CATALOG}.silver.weather_silver")
        count = df.count()
        assert count > 0, "Teammate Silver is empty"
        record("test_tm_silver_exists", "PASS", f"{count:,} rows")
        return df
    except Exception as e:
        record("test_tm_silver_exists", "FAIL", str(e))
        return None

df_tm_silver = test_tm_silver_exists()

# COMMAND ----------

# ── Test 4.2: Teammate Gold tables exist ──────────────────
def test_tm_gold_tables():
    tables = ["dim_location", "dim_condition", "dim_astronomy",
              "dim_date", "fact_weather"]
    try:
        counts = {}
        for t in tables:
            cnt = spark.sql(
                f"SELECT COUNT(*) AS cnt FROM {CATALOG}.gold.{t}"
            ).collect()[0]["cnt"]
            counts[t] = cnt
            assert cnt > 0, f"gold.{t} is empty"
        record("test_tm_gold_tables", "PASS",
               f"All {len(tables)} tables exist — fact: {counts['fact_weather']:,} rows")
        return counts
    except Exception as e:
        record("test_tm_gold_tables", "FAIL", str(e))
        return None

tm_gold_counts = test_tm_gold_tables()

# COMMAND ----------

# ── Test 4.3: Teammate Silver-Gold row consistency ─────────
def test_tm_silver_gold_consistency():
    try:
        if df_tm_silver and tm_gold_counts:
            silver_count = df_tm_silver.count()
            fact_count   = tm_gold_counts.get("fact_weather", 0)
            retention    = round((fact_count / silver_count) * 100, 2)
            assert retention >= 80, f"Low retention: {retention}% (silver={silver_count:,}, fact={fact_count:,})"
            record("test_tm_silver_gold_consistency", "PASS",
                   f"{retention}% retention (silver={silver_count:,}, fact={fact_count:,})")
    except AssertionError as e:
        record("test_tm_silver_gold_consistency", "FAIL", str(e))

test_tm_silver_gold_consistency()

# COMMAND ----------

# MAGIC %md
# MAGIC ## TEST SUITE 5 — Pipeline Integration Tests

# COMMAND ----------

print("\n" + "=" * 60)
print("TEST SUITE 5 — Pipeline Integration Tests")
print("=" * 60)

# ── Test 5.1: Bronze → Silver row flow ────────────────────
def test_bronze_to_silver_flow():
    try:
        bronze_count = df_bronze.count() if df_bronze else 0
        silver_count = df_silver.count() if df_silver else 0
        assert silver_count > 0, "Silver has no rows"
        assert bronze_count > 0, "Bronze has no rows"
        # Silver should have close to bronze rows (minus deduplication)
        retention = round((silver_count / bronze_count) * 100, 2)
        assert retention >= 80, f"Too many rows lost Bronze→Silver: {retention}%"
        record("test_bronze_to_silver_flow", "PASS",
               f"Bronze={bronze_count:,} → Silver={silver_count:,} ({retention}% retained)")
    except AssertionError as e:
        record("test_bronze_to_silver_flow", "FAIL", str(e))

test_bronze_to_silver_flow()

# COMMAND ----------

# ── Test 5.2: Batch date consistency across layers ─────────
def test_batch_date_consistency():
    try:
        bronze_date = df_bronze.agg({"_batch_date": "max"}).collect()[0][0] if df_bronze else None
        silver_date = df_silver.agg({"_batch_date": "max"}).collect()[0][0] if df_silver else None
        assert bronze_date is not None, "Bronze has no _batch_date"
        assert silver_date is not None, "Silver has no _batch_date"
        record("test_batch_date_consistency", "PASS",
               f"Bronze={bronze_date}, Silver={silver_date}")
    except AssertionError as e:
        record("test_batch_date_consistency", "FAIL", str(e))

test_batch_date_consistency()

# COMMAND ----------

# ── Test 5.3: DQ log table exists and has data ────────────
def test_dq_log_exists():
    try:
        dq_df  = spark.table(f"{CATALOG}.data_quality.dq_check_log")
        count  = dq_df.count()
        assert count > 0, "DQ log table is empty — run 04_data_quality_checks first"
        passed = dq_df.filter(col("status") == "PASS").count()
        failed = dq_df.filter(col("status") == "FAIL").count()
        record("test_dq_log_exists", "PASS",
               f"{count} DQ checks logged (passed={passed}, failed={failed})")
    except Exception as e:
        record("test_dq_log_exists", "FAIL", str(e))

test_dq_log_exists()

# COMMAND ----------

# ── Test 5.4: Pipeline run log exists ─────────────────────
def test_pipeline_run_log_exists():
    try:
        run_df = spark.table(f"{CATALOG}.pipeline_logs.run_log")
        count  = run_df.count()
        assert count > 0, "Run log is empty — run 05_error_handling first"
        success = run_df.filter(col("status") == "SUCCESS").count()
        record("test_pipeline_run_log_exists", "PASS",
               f"{count} run entries ({success} successful layers)")
    except Exception as e:
        record("test_pipeline_run_log_exists", "FAIL", str(e))

test_pipeline_run_log_exists()

# COMMAND ----------

# ── Test 5.5: Both pipelines produce same Bronze source ────
def test_same_bronze_source():
    try:
        your_silver_src = df_silver.select("_source_path").distinct().collect() if df_silver else []
        paths = [r["_source_path"] for r in your_silver_src]
        assert any("bronze_delta" in p for p in paths), \
            f"Silver not reading from bronze_delta: {paths}"
        record("test_same_bronze_source", "PASS",
               f"Silver correctly reads from bronze_delta")
    except AssertionError as e:
        record("test_same_bronze_source", "FAIL", str(e))

test_same_bronze_source()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Test Results Summary

# COMMAND ----------

print("\n" + "=" * 60)
print("PYTEST RESULTS SUMMARY")
print("=" * 60)

total   = len(test_results)
passed  = sum(1 for r in test_results if r["status"] == "PASS")
failed  = sum(1 for r in test_results if r["status"] == "FAIL")
score   = round((passed / total) * 100, 1) if total > 0 else 0

print(f"\n📊 Overall:")
print(f"   Total tests  : {total}")
print(f"   ✅ Passed    : {passed}")
print(f"   ❌ Failed    : {failed}")
print(f"   Test Score   : {score}%")

if failed > 0:
    print(f"\n❌ Failed Tests:")
    for r in test_results:
        if r["status"] == "FAIL":
            print(f"   ❌ {r['test_name']}: {r['message']}")

print(f"\n✅ Passed Tests:")
for r in test_results:
    if r["status"] == "PASS":
        print(f"   ✅ {r['test_name']}: {r['message']}")

# COMMAND ----------

# ── Write test results to Delta ────────────────────────────
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import lit, current_timestamp
import datetime

test_schema = StructType([
    StructField("test_name",  StringType(), True),
    StructField("status",     StringType(), True),
    StructField("message",    StringType(), True),
])

batch_date = datetime.date.today().strftime("%Y-%m-%d")
df_results = spark.createDataFrame(test_results, schema=test_schema) \
    .withColumn("batch_date", lit(batch_date)) \
    .withColumn("run_at", current_timestamp())

TEST_PATH = f"{BUCKET}/pipeline_logs/test_results/"
df_results.write \
    .format("delta") \
    .mode("append") \
    .option("mergeSchema", "true") \
    .partitionBy("batch_date") \
    .save(TEST_PATH)

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.pipeline_logs.test_results
    USING DELTA LOCATION '{TEST_PATH}'
""")

print(f"\n✅ Test results saved → {CATALOG}.pipeline_logs.test_results")
display(df_results.orderBy("status", "test_name"))

# COMMAND ----------

# ── Final test gate ────────────────────────────────────────
if failed > 0:
    raise ValueError(
        f"🚨 PyTest FAILED — {failed}/{total} tests failed. "
        f"Check {CATALOG}.pipeline_logs.test_results for details."
    )
else:
    print(f"\n🎉 All {total} tests PASSED!")
    print(f"   Test Score : {score}%")
    print(f"   Pipeline is validated and ready ✅")
