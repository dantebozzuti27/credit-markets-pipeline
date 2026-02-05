# Credit Markets Daily Pipeline DAG

from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

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
    
    wait_for_fred_data = S3KeySensor(
        task_id="wait_for_fred_data",
        bucket_name="credit-markets-data",
        bucket_key="bronze/fred/{{ ds }}/*",
        wildcard_match=True,
        aws_conn_id="aws_default",
        poke_interval=60,
        timeout=3600,
        mode="reschedule",
        soft_fail=True,
    )

    @task(trigger_rule="none_failed_min_one_success")
    def run_daily_pipeline(**context):
        from credit_markets.pipeline.daily import DailyPipeline
        from datetime import date

        execution_date = context["logical_date"].date()

        pipeline = DailyPipeline()
        results = pipeline.run(execution_date)

        return results
    
    @task
    def trigger_lambda(pipeline_results, **context):
        import json
        import boto3

        lambda_client = boto3.client(
            'lambda',
            endpoint_url='http://localstack:4566',
            region_name='us-east-1',
            aws_access_key_id='test',
            aws_secret_access_key='test'
        )

        payload = {
            "source": "airflow",
            "date": str(context["logical_date"].date()),
            "pipeline_results": pipeline_results
        }

        response = lambda_client.invoke(
            FunctionName='credit-markets-processor',
            InvocationType='Event',
            Payload=json.dumps(payload)
        )

        return response['StatusCode']

    pipeline_result = run_daily_pipeline()
    wait_for_fred_data >> pipeline_result >> trigger_lambda(pipeline_result)

   