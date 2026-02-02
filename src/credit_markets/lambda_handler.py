#Lambda Handler

from credit_markets.pipeline.daily import DailyPipeline
from datetime import date, datetime
import json

def handler(event, context):
    """    AWS Lambda entry point.
    
    Args:
        event: Dict containing input data (from EventBridge, API Gateway, etc.)
               Example: {"target_date": "2026-01-26"} or {} for today
        
        context: Lambda runtime info (request ID, time remaining, memory limit)
                 You rarely use this directly, but it's always passed
    
    Returns:
        Dict with statusCode and body (API Gateway format)
        Example: {"statusCode": 200, "body": '{"fred_rows": 100}'}
    """

    target_date = event.get("target_date")
    if target_date is None:
        target_date = date.today()
    else:
        target_date = datetime.strptime(target_date, "%Y-%m-%d").date()
    
    try:
        pipeline = DailyPipeline()
        results = pipeline.run(target_date)

        return {
            "statusCode": 200,
            "body": json.dumps(results)
        }
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }
