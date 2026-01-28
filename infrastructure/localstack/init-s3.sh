#!/bin/bash
# Initialize LocalStack S3 buckets

echo "Creating S3 bucket for credit-markets-data..."
awslocal s3 mb s3://credit-markets-data

echo "Listing buckets..."
awslocal s3 ls

echo "LocalStack S3 initialization complete!"
