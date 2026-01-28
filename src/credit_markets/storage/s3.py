# S3 Storage Client

import json
from datetime import datetime
import boto3
from credit_markets.config.settings import get_settings

class S3Client:
    def __init__(self):
        settings = get_settings()
        self.bucket = settings.s3_bucket

        if settings.aws_endpoint_url:
            self.client = boto3.client(
                "s3",
                endpoint_url=settings.aws_endpoint_url,
                region_name=settings.aws_region,
            )
        else:
            self.client = boto3.client("s3", region_name=settings.aws_region)
    
    def write_json(self, data: dict, key: str) -> None:
        """Write JSON data to s3"""
        body = json.dumps(data)
        self.client.put_object(
            Bucket=self.bucket,
            Key=key,
            Body=body,
            ContentType="application/json"
        )

if __name__ == "__main__":
    client = S3Client()
    client.write_json({"test": "data"}, "test/example.json")
    print("Written to S3")