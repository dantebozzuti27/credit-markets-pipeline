# Credit Markets Daily Pipeline DAG

from datetime import datetime, timedelta

import boto3
from airflow import DAG
from airflow.decorators import task
from airflow.providers.amazon.aws.sensors.s3 import S3KeySensor
from airflow.providers.amazon.aws.operators.lambda_function import LambdaInvokeFunctionOperator

def send_slack_notification(context, status="failed"):
    """Send Slack notification on task/DAG events."""
    import os
    import requests

    webhook_url = os.environ.get("SLACK_WEBHOOK_URL")
    if not webhook_url:
        print("SLACK_WEBHOOK_URL not set, skipping alert")
        return

    dag_id = context.get("dag").dag_id
    execution_date = context.get("logical_date")

    if status == "failed":
        task_instance = context.get("task_instance")
        exception = context.get("exception")
        message = {
        "text": f":x: *Task Failed*",
        "attachments": [
            {
                "color": "danger",
                "fields": [
                    {"title": "DAG", "value": dag_id, "short": True},
                    {"title": "Task", "value": task_instance.task_id, "short": True},
                    {"title": "Execution Date", "value": str(execution_date), "short": False},
                    {"title": "Error", "value": str(exception)[:500], "short": False},
                ],
            }],
        }
    else:
        message = {
            "text": ":white_check_mark: *DAG Completed Successfully*",
            "attachments": [{
                "color": "good",
                "fields": [
                    {"title": "DAG", "value": dag_id, "short": True},
                    {"title": "Execution Date", "value": str(execution_date), "short": True},
                ],
            }],
        }

    requests.post(webhook_url, json=message)

def on_failure(context):
    send_slack_notification(context, status="failed")
def on_success(context):
    send_slack_notification(context, status="success")

def publish_cloudwatch_metric(metric_name, value, unit="Count"):
    """Publish custom metric to CloudWatch LocalStack."""
    import boto3

    try:
        cloudwatch = boto3.client(
            'cloudwatch',
            endpoint_url='http://localstack:4566',
            region_name='us-east-1',
            aws_access_key_id='test',
            aws_secret_access_key='test'
        )

        cloudwatch.put_metric_data(
            Namespace='CreditMarketsPipeline',
            MetricData=[{
                'MetricName': metric_name,
                'Value': value,
                'Unit': unit,
            }]
        )
    except Exception as e:
        print(f"failed to publish metric {metric_name}: {e}")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure,
}

with DAG(
    dag_id="credit_markets_daily",
    default_args=default_args,
    description="Daily pipeline for FRED and SEC data ingestion",
    schedule="0 6 * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["credit-markets", "daily"],
    on_success_callback=on_success,
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
        import time

        execution_date = context["logical_date"].date()

        start_time = time.time()

        pipeline = DailyPipeline()
        results = pipeline.run(execution_date)
        duration = time.time() - start_time

        publish_cloudwatch_metric("PipelineDuration", duration, "Seconds")
        publish_cloudwatch_metric("PipelineSuccess", 1, "Count")
        
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

   