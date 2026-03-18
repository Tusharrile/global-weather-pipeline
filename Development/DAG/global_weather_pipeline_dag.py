"""
Global Weather Pipeline DAG
============================
Airflow Version : 2.9.2
Owner           : global-weather-team
Schedule        : Daily at 6:00 AM UTC
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.glue_crawler import GlueCrawlerOperator
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta
import logging

# ── Config ─────────────────────────────────────────────────────────────────────
# S3 Bucket
S3_BUCKET        = "global-weather-pipeline"
S3_RAW_PREFIX    = "raw/GlobalWeather.csv"
S3_REGION        = "ap-south-1"

# AWS / Glue
GLUE_CRAWLER     = "global-weather-glue-crawler"

# Databricks
DATABRICKS_CONN  = "databricks_default"

# Databricks Job ID — Weather-DAG (Bronze > Silver > Gold)
# Found at: Databricks → Jobs & Pipelines → Weather-DAG → Job ID
WEATHER_DAG_JOB_ID = 452814326715663

# ── Default args ───────────────────────────────────────────────────────────────
default_args = {
    "owner":            "global-weather-team",
    "depends_on_past":  False,
    "email_on_failure": False,
    "email_on_retry":   False,
    "retries":          1,
    "retry_delay":      timedelta(minutes=5),
}

# ── Notification helpers ───────────────────────────────────────────────────────
def send_slack_notification(message, **context):
    import requests, os
    webhook_url = os.environ.get("slack_webhook_url", "")
    if not webhook_url:
        logging.info("No Slack webhook configured, skipping notification")
        return
    try:
        response = requests.post(webhook_url, json={"text": message})
        logging.info(f"Slack notification sent: {response.status_code}")
    except Exception as e:
        logging.warning(f"Slack notification failed: {e}")


def notify_start(**context):
    send_slack_notification(
        f"Global Weather Pipeline STARTED\nRun date: {context['ds']}",
        **context
    )


def notify_complete(**context):
    send_slack_notification(
        f"Global Weather Pipeline COMPLETE\nRun date: {context['ds']}\nBronze > Silver > Gold all done!",
        **context
    )


def notify_failure(context):
    task_id = context["task_instance"].task_id
    send_slack_notification(
        f"FAILED: Task {task_id} failed!\nRun date: {context['ds']}"
    )


# ── DAG ────────────────────────────────────────────────────────────────────────
with DAG(
    dag_id            = "global_weather_pipeline",
    default_args      = default_args,
    description       = "End-to-end weather pipeline: S3 > Glue > Bronze > Silver > Gold",
    schedule_interval = "0 6 * * *",
    start_date        = datetime(2024, 1, 1),
    catchup           = False,
    max_active_runs   = 1,
    tags              = ["weather", "medallion", "databricks"],
) as dag:

    # ── Task 1: Notify Start ──────────────────────────────────────────────────
    task_notify_start = PythonOperator(
        task_id         = "notify_pipeline_started",
        python_callable = notify_start,
        provide_context = True,
    )

    # ── Task 2: S3 Sensor ────────────────────────────────────────────────────
    task_wait_for_file = S3KeySensor(
        task_id             = "wait_for_raw_csv",
        bucket_name         = S3_BUCKET,
        bucket_key          = S3_RAW_PREFIX,
        wildcard_match      = False,
        aws_conn_id         = "aws_default",
        poke_interval       = 30,
        timeout             = 300,
        mode                = "reschedule",
        on_failure_callback = notify_failure,
    )

    # ── Task 3: Glue Crawler ─────────────────────────────────────────────────
    task_glue_crawler = GlueCrawlerOperator(
        task_id             = "run_glue_crawler",
        config              = {"Name": GLUE_CRAWLER},
        aws_conn_id         = "aws_default",
        on_failure_callback = notify_failure,
    )

    # ── Task 4: Run Databricks Weather-DAG Job (Bronze > Silver > Gold) ───────
    # Uses DatabricksRunNowOperator — triggers existing job, supports serverless
    task_databricks_pipeline = DatabricksRunNowOperator(
        task_id             = "run_databricks_pipeline",
        databricks_conn_id  = DATABRICKS_CONN,
        job_id              = WEATHER_DAG_JOB_ID,
        notebook_params     = {"run_date": "{{ ds }}"},
        on_failure_callback = notify_failure,
    )

    # ── Task 5: Notify Complete ───────────────────────────────────────────────
    task_notify_complete = PythonOperator(
        task_id         = "notify_pipeline_complete",
        python_callable = notify_complete,
        provide_context = True,
    )

    # ── Pipeline Flow ─────────────────────────────────────────────────────────
    (
        task_notify_start
        >> task_wait_for_file
        >> task_glue_crawler
        >> task_databricks_pipeline
        >> task_notify_complete
    )
