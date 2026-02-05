# Post-processing Lambda handler

import json

def handler(event, context):
    """Post-processing Lambda triggered by Airflow."""
    print(f"received event: {json.dumps(event)}")

    source = event.get("source", "unknown")
    date = event.get("date", "unknown")
    pipeline_results = event.get("pipeline_results", {})

    print(f"Processing data from {source} for date {date}")
    print(f"Pipeline results: {pipeline_results}")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "message": "Post-processing complete",
            "date": date
        })
    }