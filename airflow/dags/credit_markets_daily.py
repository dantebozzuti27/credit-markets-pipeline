# Credit Markets Daily Pipeline DAG

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="credit_markets_daily",
    default_args=default_args,
    description="Daily pipeline for FRED and SEC data ingestion",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["credit-markets", "daily"],
) as dag:
    
    @task
    def run_daily_pipeline(**context):
        from credit_markets.pipeline.daily import DailyPipeline
        from datetime import date

        execution_date = context["logical_date"].date()

        pipeline = DailyPipeline()
        results = pipeline.run(execution_date)

        return results
    
    run_daily_pipeline()