# Databricks notebook source
# MAGIC %sql
# MAGIC USE CATALOG global_weather_catalog;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS global_weather_catalog.insights
# MAGIC COMMENT 'Business insights layer - pre-aggregated tables for dashboards and reporting';

# COMMAND ----------

# MAGIC %sql
# MAGIC USE CATALOG global_weather_catalog;
# MAGIC USE SCHEMA insights;

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, current_timestamp, lit,
    round as spark_round,
    avg, min as spark_min, max as spark_max,
    sum as spark_sum, count, countDistinct,
    first, dense_rank
)
from pyspark.sql.window import Window
import datetime
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("business_insights")

CATALOG    = "global_weather_catalog"
BUCKET     = "s3://global-weather-pipeline"
INSIGHTS_PATH = f"{BUCKET}/insights/"
batch_date = datetime.date.today().strftime("%Y-%m-%d")

# ── Read from fact + dims ──────────────────────────────────
fact  = spark.table(f"{CATALOG}.gold_analytics.fact_weather_events")
d_loc = spark.table(f"{CATALOG}.gold_analytics.dim_location")
d_dt  = spark.table(f"{CATALOG}.gold_analytics.dim_date")
d_cond= spark.table(f"{CATALOG}.gold_analytics.dim_weather_condition")
d_aq  = spark.table(f"{CATALOG}.gold_analytics.dim_air_quality")

# ── Base joined view for insights ──────────────────────────
df = fact \
    .join(d_loc.select("location_id","country","location_name","region_category","weather_station_name","sensor_type"),
          on="location_id", how="left") \
    .join(d_dt.select("date_id","date","year","month","month_name","day_of_week",
                      "day_name","hour","season","is_day","is_weekend",
                      "time_of_day","date_label","week_of_year","quarter","daylight_hours"),
          on="date_id", how="left") \
    .join(d_cond.select("condition_id","condition_text","temperature_category",
                        "humidity_level","wind_intensity_level","precipitation_level",
                        "visibility_category","uv_risk_level","cloud_cover_level"),
          on="condition_id", how="left") \
    .join(d_aq.select("air_quality_id","air_quality_category",
                      F.col("air_quality_us_epa_index"),
                      F.col("air_quality_gb_defra_index"),
                      d_aq["pm25_health_risk"].alias("pm25_health_risk")),
          on="air_quality_id", how="left") \
    .drop(fact["pm25_health_risk"])

total = df.count()
print(f"✅ Base joined dataset ready!")
print(f"   Total rows : {total}")
print(f"\n🟢 Building 13 business insight tables...")

# COMMAND ----------

# ══════════════════════════════════════════════════════════
# INSIGHT 1 — insight_air_quality
# Daily air quality metrics per country and location
# ══════════════════════════════════════════════════════════

logger.info("Building insight_air_quality...")

insight_air_quality = df.groupBy(
    "country", "location_name", "date", "year", "month", "month_name", "season"
).agg(
    spark_round(avg("air_quality_pm2_5"),            2).alias("avg_pm2_5"),
    spark_round(spark_max("air_quality_pm2_5"),      2).alias("max_pm2_5"),
    spark_round(avg("air_quality_pm10"),             2).alias("avg_pm10"),
    spark_round(spark_max("air_quality_pm10"),       2).alias("max_pm10"),
    spark_round(avg("air_quality_carbon_monoxide"),  2).alias("avg_carbon_monoxide"),
    spark_round(avg("air_quality_ozone"),            2).alias("avg_ozone"),
    spark_round(avg("air_quality_nitrogen_dioxide"), 2).alias("avg_nitrogen_dioxide"),
    spark_round(avg("air_quality_sulphur_dioxide"),  2).alias("avg_sulphur_dioxide"),
    first("air_quality_category").alias("air_quality_category"),
    first("pm25_health_risk").alias("pm25_health_risk"),
    first("air_quality_us_epa_index").alias("us_epa_index"),
    first("air_quality_gb_defra_index").alias("gb_defra_index"),
    count("*").alias("record_count")
).withColumn("_gold_ts",    current_timestamp()) \
 .withColumn("_batch_date", lit(batch_date)) \
 .withColumn("_layer",      lit("gold"))

insight_air_quality.write.format("delta").mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(f"{INSIGHTS_PATH}insight_air_quality/")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.insights.insight_air_quality
    USING DELTA LOCATION '{INSIGHTS_PATH}insight_air_quality/'
""")
print(f"✅ insight_air_quality : {insight_air_quality.count()} rows")

# COMMAND ----------

# ══════════════════════════════════════════════════════════
# INSIGHT 2 — insight_best_weather
# Best weather conditions per country per season
# Ranks locations by comfort (low uv, mild temp, low humidity)
# ══════════════════════════════════════════════════════════

logger.info("Building insight_best_weather...")

window_rank = Window.partitionBy("country", "season").orderBy(
    col("avg_temp_celsius").asc(),
    col("avg_humidity").asc(),
    col("avg_uv_index").asc()
)

insight_best_weather = df.groupBy(
    "country", "location_name", "season", "region_category"
).agg(
    spark_round(avg("temperature_celsius"), 2).alias("avg_temp_celsius"),
    spark_round(avg("humidity"),            2).alias("avg_humidity"),
    spark_round(avg("uv_index"),            2).alias("avg_uv_index"),
    spark_round(avg("wind_kph"),            2).alias("avg_wind_kph"),
    spark_round(avg("visibility_km"),       2).alias("avg_visibility_km"),
    spark_round(avg("heat_index"),          2).alias("avg_heat_index"),
    first("temperature_category").alias("temperature_category"),
    first("humidity_level").alias("humidity_level"),
    count("*").alias("record_count")
).withColumn("comfort_rank", dense_rank().over(window_rank)) \
 .withColumn("_gold_ts",    current_timestamp()) \
 .withColumn("_batch_date", lit(batch_date)) \
 .withColumn("_layer",      lit("gold"))

insight_best_weather.write.format("delta").mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(f"{INSIGHTS_PATH}insight_best_weather/")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.insights.insight_best_weather
    USING DELTA LOCATION '{INSIGHTS_PATH}insight_best_weather/'
""")
print(f"✅ insight_best_weather : {insight_best_weather.count()} rows")

# COMMAND ----------

# ══════════════════════════════════════════════════════════
# INSIGHT 3 — insight_cloud_air_quality
# Relationship between cloud cover and air quality
# ══════════════════════════════════════════════════════════

logger.info("Building insight_cloud_air_quality...")

insight_cloud_air_quality = df.groupBy(
    "country", "location_name", "date",
    "cloud_cover_level", "air_quality_category", "season"
).agg(
    spark_round(avg("cloud"),               2).alias("avg_cloud_pct"),
    spark_round(spark_max("cloud"),         2).alias("max_cloud_pct"),
    spark_round(avg("air_quality_pm2_5"),   2).alias("avg_pm2_5"),
    spark_round(avg("air_quality_pm10"),    2).alias("avg_pm10"),
    spark_round(avg("uv_index"),            2).alias("avg_uv_index"),
    first("pm25_health_risk").alias("pm25_health_risk"),
    count("*").alias("record_count")
).withColumn("_gold_ts",    current_timestamp()) \
 .withColumn("_batch_date", lit(batch_date)) \
 .withColumn("_layer",      lit("gold"))

insight_cloud_air_quality.write.format("delta").mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(f"{INSIGHTS_PATH}insight_cloud_air_quality/")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.insights.insight_cloud_air_quality
    USING DELTA LOCATION '{INSIGHTS_PATH}insight_cloud_air_quality/'
""")
print(f"✅ insight_cloud_air_quality : {insight_cloud_air_quality.count()} rows")

# COMMAND ----------

# ══════════════════════════════════════════════════════════
# INSIGHT 4 — insight_conditions
# Weather condition frequency and metrics per country
# ══════════════════════════════════════════════════════════

logger.info("Building insight_conditions...")

insight_conditions = df.groupBy(
    "country", "condition_text", "season", "month_name"
).agg(
    count("*").alias("occurrence_count"),
    spark_round(avg("temperature_celsius"), 2).alias("avg_temp_celsius"),
    spark_round(avg("humidity"),            2).alias("avg_humidity"),
    spark_round(avg("wind_kph"),            2).alias("avg_wind_kph"),
    spark_round(avg("precip_mm"),           2).alias("avg_precip_mm"),
    spark_round(avg("visibility_km"),       2).alias("avg_visibility_km"),
    first("temperature_category").alias("temperature_category"),
    first("precipitation_level").alias("precipitation_level")
).withColumn("_gold_ts",    current_timestamp()) \
 .withColumn("_batch_date", lit(batch_date)) \
 .withColumn("_layer",      lit("gold"))

insight_conditions.write.format("delta").mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(f"{INSIGHTS_PATH}insight_conditions/")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.insights.insight_conditions
    USING DELTA LOCATION '{INSIGHTS_PATH}insight_conditions/'
""")
print(f"✅ insight_conditions : {insight_conditions.count()} rows")

# COMMAND ----------

# ══════════════════════════════════════════════════════════
# INSIGHT 5 — insight_day_night_temp
# Temperature comparison between day and night per location
# ══════════════════════════════════════════════════════════

logger.info("Building insight_day_night_temp...")

insight_day_night_temp = df.groupBy(
    "country", "location_name", "date", "is_day", "season", "month_name"
).agg(
    spark_round(avg("temperature_celsius"),  2).alias("avg_temp_celsius"),
    spark_round(spark_min("temperature_celsius"), 2).alias("min_temp_celsius"),
    spark_round(spark_max("temperature_celsius"), 2).alias("max_temp_celsius"),
    spark_round(avg("feels_like_celsius"),   2).alias("avg_feels_like"),
    spark_round(avg("heat_index"),           2).alias("avg_heat_index"),
    spark_round(avg("humidity"),             2).alias("avg_humidity"),
    spark_round(avg("uv_index"),             2).alias("avg_uv_index"),
    spark_round(avg("daylight_hours"),       2).alias("avg_daylight_hours"),
    count("*").alias("record_count")
).withColumn("_gold_ts",    current_timestamp()) \
 .withColumn("_batch_date", lit(batch_date)) \
 .withColumn("_layer",      lit("gold"))

insight_day_night_temp.write.format("delta").mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(f"{INSIGHTS_PATH}insight_day_night_temp/")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.insights.insight_day_night_temp
    USING DELTA LOCATION '{INSIGHTS_PATH}insight_day_night_temp/'
""")
print(f"✅ insight_day_night_temp : {insight_day_night_temp.count()} rows")

# COMMAND ----------

# ══════════════════════════════════════════════════════════
# INSIGHT 6 — insight_extreme_weather
# Extreme weather events — hottest, coldest, windiest,
# most humid, worst air quality per country
# ══════════════════════════════════════════════════════════

logger.info("Building insight_extreme_weather...")

insight_extreme_weather = df.groupBy(
    "country", "location_name", "date", "season", "region_category"
).agg(
    spark_round(spark_max("temperature_celsius"), 2).alias("max_temp_celsius"),
    spark_round(spark_min("temperature_celsius"), 2).alias("min_temp_celsius"),
    spark_round(spark_max("wind_kph"),            2).alias("max_wind_kph"),
    spark_round(spark_max("gust_kph"),            2).alias("max_gust_kph"),
    spark_round(spark_max("humidity"),            2).alias("max_humidity"),
    spark_round(spark_max("precip_mm"),           2).alias("max_precip_mm"),
    spark_round(spark_max("uv_index"),            2).alias("max_uv_index"),
    spark_round(spark_max("air_quality_pm2_5"),   2).alias("max_pm2_5"),
    spark_round(spark_min("visibility_km"),       2).alias("min_visibility_km"),
    spark_round(spark_max("heat_index"),          2).alias("max_heat_index"),
    first("condition_text").alias("condition_text"),
    first("uv_risk_level").alias("uv_risk_level"),
    first("wind_intensity_level").alias("wind_intensity_level"),
    count("*").alias("record_count")
).withColumn("_gold_ts",    current_timestamp()) \
 .withColumn("_batch_date", lit(batch_date)) \
 .withColumn("_layer",      lit("gold"))

insight_extreme_weather.write.format("delta").mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(f"{INSIGHTS_PATH}insight_extreme_weather/")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.insights.insight_extreme_weather
    USING DELTA LOCATION '{INSIGHTS_PATH}insight_extreme_weather/'
""")
print(f"✅ insight_extreme_weather : {insight_extreme_weather.count()} rows")

# COMMAND ----------

# ══════════════════════════════════════════════════════════
# INSIGHT 7 — insight_humidity
# Humidity patterns by location, season and time of day
# ══════════════════════════════════════════════════════════

logger.info("Building insight_humidity...")

insight_humidity = df.groupBy(
    "country", "location_name", "season", "time_of_day", "month_name"
).agg(
    spark_round(avg("humidity"),            2).alias("avg_humidity"),
    spark_round(spark_min("humidity"),      2).alias("min_humidity"),
    spark_round(spark_max("humidity"),      2).alias("max_humidity"),
    spark_round(avg("temperature_celsius"), 2).alias("avg_temp_celsius"),
    spark_round(avg("heat_index"),          2).alias("avg_heat_index"),
    spark_round(avg("precip_mm"),           2).alias("avg_precip_mm"),
    first("humidity_level").alias("humidity_level"),
    count("*").alias("record_count")
).withColumn("_gold_ts",    current_timestamp()) \
 .withColumn("_batch_date", lit(batch_date)) \
 .withColumn("_layer",      lit("gold"))

insight_humidity.write.format("delta").mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(f"{INSIGHTS_PATH}insight_humidity/")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.insights.insight_humidity
    USING DELTA LOCATION '{INSIGHTS_PATH}insight_humidity/'
""")
print(f"✅ insight_humidity : {insight_humidity.count()} rows")

# COMMAND ----------

# ══════════════════════════════════════════════════════════
# INSIGHT 8 — insight_pressure
# Atmospheric pressure patterns and weather forecast signals
# ══════════════════════════════════════════════════════════

logger.info("Building insight_pressure...")

insight_pressure = df.groupBy(
    "country", "location_name", "date", "season", "month_name"
).agg(
    spark_round(avg("pressure_mb"),         2).alias("avg_pressure_mb"),
    spark_round(spark_min("pressure_mb"),   2).alias("min_pressure_mb"),
    spark_round(spark_max("pressure_mb"),   2).alias("max_pressure_mb"),
    spark_round(avg("temperature_celsius"), 2).alias("avg_temp_celsius"),
    spark_round(avg("wind_kph"),            2).alias("avg_wind_kph"),
    spark_round(avg("precip_mm"),           2).alias("avg_precip_mm"),
    first("condition_text").alias("condition_text"),
    count("*").alias("record_count")
).withColumn("pressure_category",
    F.when(col("avg_pressure_mb") < 1000, "Low Pressure - Storm Risk")
    .when(col("avg_pressure_mb") <= 1013, "Normal Pressure")
    .otherwise("High Pressure - Clear Weather")
).withColumn("_gold_ts",    current_timestamp()) \
 .withColumn("_batch_date", lit(batch_date)) \
 .withColumn("_layer",      lit("gold"))

insight_pressure.write.format("delta").mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(f"{INSIGHTS_PATH}insight_pressure/")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.insights.insight_pressure
    USING DELTA LOCATION '{INSIGHTS_PATH}insight_pressure/'
""")
print(f"✅ insight_pressure : {insight_pressure.count()} rows")

# COMMAND ----------

# ══════════════════════════════════════════════════════════
# INSIGHT 9 — insight_rainfall
# Precipitation analysis per country and season
# ══════════════════════════════════════════════════════════

logger.info("Building insight_rainfall...")

insight_rainfall = df.groupBy(
    "country", "location_name", "date", "season",
    "month_name", "precipitation_level"
).agg(
    spark_round(spark_sum("precip_mm"),     2).alias("total_precip_mm"),
    spark_round(avg("precip_mm"),           2).alias("avg_precip_mm"),
    spark_round(spark_max("precip_mm"),     2).alias("max_precip_mm"),
    spark_round(avg("humidity"),            2).alias("avg_humidity"),
    spark_round(avg("cloud"),               2).alias("avg_cloud_pct"),
    spark_round(avg("temperature_celsius"), 2).alias("avg_temp_celsius"),
    first("condition_text").alias("condition_text"),
    first("cloud_cover_level").alias("cloud_cover_level"),
    count("*").alias("record_count")
).withColumn("_gold_ts",    current_timestamp()) \
 .withColumn("_batch_date", lit(batch_date)) \
 .withColumn("_layer",      lit("gold"))

insight_rainfall.write.format("delta").mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(f"{INSIGHTS_PATH}insight_rainfall/")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.insights.insight_rainfall
    USING DELTA LOCATION '{INSIGHTS_PATH}insight_rainfall/'
""")
print(f"✅ insight_rainfall : {insight_rainfall.count()} rows")

# COMMAND ----------

# ══════════════════════════════════════════════════════════
# INSIGHT 10 — insight_top_temperatures
# Top hottest and coldest locations per season
# ══════════════════════════════════════════════════════════

logger.info("Building insight_top_temperatures...")

window_hot  = Window.partitionBy("season").orderBy(col("avg_temp_celsius").desc())
window_cold = Window.partitionBy("season").orderBy(col("avg_temp_celsius").asc())

temp_base = df.groupBy(
    "country", "location_name", "season", "region_category"
).agg(
    spark_round(avg("temperature_celsius"),      2).alias("avg_temp_celsius"),
    spark_round(spark_max("temperature_celsius"),2).alias("max_temp_celsius"),
    spark_round(spark_min("temperature_celsius"),2).alias("min_temp_celsius"),
    spark_round(avg("feels_like_celsius"),       2).alias("avg_feels_like"),
    spark_round(avg("heat_index"),               2).alias("avg_heat_index"),
    spark_round(avg("humidity"),                 2).alias("avg_humidity"),
    first("temperature_category").alias("temperature_category"),
    count("*").alias("record_count")
)

insight_top_temperatures = temp_base \
    .withColumn("hot_rank",  dense_rank().over(window_hot)) \
    .withColumn("cold_rank", dense_rank().over(window_cold)) \
    .withColumn("_gold_ts",    current_timestamp()) \
    .withColumn("_batch_date", lit(batch_date)) \
    .withColumn("_layer",      lit("gold"))

insight_top_temperatures.write.format("delta").mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(f"{INSIGHTS_PATH}insight_top_temperatures/")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.insights.insight_top_temperatures
    USING DELTA LOCATION '{INSIGHTS_PATH}insight_top_temperatures/'
""")
print(f"✅ insight_top_temperatures : {insight_top_temperatures.count()} rows")

# COMMAND ----------

# ══════════════════════════════════════════════════════════
# INSIGHT 11 — insight_uv_risk
# UV risk analysis by location, season and time of day
# ══════════════════════════════════════════════════════════

logger.info("Building insight_uv_risk...")

insight_uv_risk = df.groupBy(
    "country", "location_name", "season",
    "time_of_day", "month_name", "uv_risk_level"
).agg(
    spark_round(avg("uv_index"),            2).alias("avg_uv_index"),
    spark_round(spark_max("uv_index"),      2).alias("max_uv_index"),
    spark_round(avg("cloud"),               2).alias("avg_cloud_pct"),
    spark_round(avg("temperature_celsius"), 2).alias("avg_temp_celsius"),
    spark_round(avg("daylight_hours"),      2).alias("avg_daylight_hours"),
    first("is_day").alias("is_day"),
    count("*").alias("record_count")
).withColumn("_gold_ts",    current_timestamp()) \
 .withColumn("_batch_date", lit(batch_date)) \
 .withColumn("_layer",      lit("gold"))

insight_uv_risk.write.format("delta").mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(f"{INSIGHTS_PATH}insight_uv_risk/")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.insights.insight_uv_risk
    USING DELTA LOCATION '{INSIGHTS_PATH}insight_uv_risk/'
""")
print(f"✅ insight_uv_risk : {insight_uv_risk.count()} rows")

# COMMAND ----------

# ══════════════════════════════════════════════════════════
# INSIGHT 12 — insight_visibility
# Visibility patterns by location and weather condition
# ══════════════════════════════════════════════════════════

logger.info("Building insight_visibility...")

insight_visibility = df.groupBy(
    "country", "location_name", "season",
    "condition_text", "visibility_category", "month_name"
).agg(
    spark_round(avg("visibility_km"),       2).alias("avg_visibility_km"),
    spark_round(spark_min("visibility_km"), 2).alias("min_visibility_km"),
    spark_round(spark_max("visibility_km"), 2).alias("max_visibility_km"),
    spark_round(avg("humidity"),            2).alias("avg_humidity"),
    spark_round(avg("cloud"),               2).alias("avg_cloud_pct"),
    spark_round(avg("precip_mm"),           2).alias("avg_precip_mm"),
    first("cloud_cover_level").alias("cloud_cover_level"),
    count("*").alias("record_count")
).withColumn("_gold_ts",    current_timestamp()) \
 .withColumn("_batch_date", lit(batch_date)) \
 .withColumn("_layer",      lit("gold"))

insight_visibility.write.format("delta").mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(f"{INSIGHTS_PATH}insight_visibility/")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.insights.insight_visibility
    USING DELTA LOCATION '{INSIGHTS_PATH}insight_visibility/'
""")
print(f"✅ insight_visibility : {insight_visibility.count()} rows")

# COMMAND ----------

# ══════════════════════════════════════════════════════════
# INSIGHT 13 — insight_wind
# Wind patterns and gust analysis per location and season
# ══════════════════════════════════════════════════════════

logger.info("Building insight_wind...")

insight_wind = df.groupBy(
    "country", "location_name", "season",
    "month_name", "wind_intensity_level", "wind_compass_zone"
).agg(
    spark_round(avg("wind_kph"),            2).alias("avg_wind_kph"),
    spark_round(spark_max("wind_kph"),      2).alias("max_wind_kph"),
    spark_round(spark_min("wind_kph"),      2).alias("min_wind_kph"),
    spark_round(avg("gust_kph"),            2).alias("avg_gust_kph"),
    spark_round(spark_max("gust_kph"),      2).alias("max_gust_kph"),
    spark_round(avg("wind_degree"),         2).alias("avg_wind_degree"),
    first("wind_direction").alias("dominant_wind_direction"),
    spark_round(avg("temperature_celsius"), 2).alias("avg_temp_celsius"),
    count("*").alias("record_count")
).withColumn("_gold_ts",    current_timestamp()) \
 .withColumn("_batch_date", lit(batch_date)) \
 .withColumn("_layer",      lit("gold"))

insight_wind.write.format("delta").mode("overwrite") \
    .option("overwriteSchema","true") \
    .save(f"{INSIGHTS_PATH}insight_wind/")

spark.sql(f"""
    CREATE TABLE IF NOT EXISTS {CATALOG}.insights.insight_wind
    USING DELTA LOCATION '{INSIGHTS_PATH}insight_wind/'
""")
print(f"✅ insight_wind : {insight_wind.count()} rows")

# COMMAND ----------

# ── Optimize all insight tables ────────────────────────────
print("📊 Optimizing all insight tables...")

insight_tables = [
    ("insight_air_quality",       "country, date"),
    ("insight_best_weather",      "country, season"),
    ("insight_cloud_air_quality", "country, date"),
    ("insight_conditions",        "country, condition_text"),
    ("insight_day_night_temp",    "country, is_day"),
    ("insight_extreme_weather",   "country, date"),
    ("insight_humidity",          "country, season"),
    ("insight_pressure",          "country, date"),
    ("insight_rainfall",          "country, season"),
    ("insight_top_temperatures",  "season"),
    ("insight_uv_risk",           "country, season"),
    ("insight_visibility",        "country, condition_text"),
    ("insight_wind",              "country, season"),
]

for table, zorder in insight_tables:
    spark.sql(f"OPTIMIZE {CATALOG}.insights.{table} ZORDER BY ({zorder})")
    print(f"   ✅ {table} optimized")

print(f"\n✅ All insight tables optimized!")

# COMMAND ----------

# ── Final Verification ─────────────────────────────────────
print("🎉 Business Insights — Final Verification Report")
print("=" * 55)
print(f"\n📊 Row Counts:")

total_rows = 0
for table, _ in insight_tables:
    cnt = spark.sql(
        f"SELECT COUNT(*) AS cnt FROM {CATALOG}.insights.{table}"
    ).collect()[0]["cnt"]
    total_rows += cnt
    print(f"   💡 {table}: {cnt} rows")

print(f"\n   Total insight rows : {total_rows}")
print(f"   Total tables       : {len(insight_tables)}")
print(f"   Schema             : insights")
print(f"   Source             : fact_weather_events + 4 dims")
print(f"   Location           : {INSIGHTS_PATH}")
print(f"\n✅ Business insights build SUCCESSFUL ✓")
