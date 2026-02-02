#!/bin/bash
#Deploy Lambda to LocalStack

set -e

echo "Packaging Lambda..."

#Create temporary build directory
rm -rf build/lambda
mkdir -p build/lambda

#Copy source code
cp -r src/credit_markets build/lambda/

#Install Dependencies
pip install -r requirements-lambda.txt -t build/lambda/ \
    --platform manylinux2014_x86_64 \
    --only-binary=:all: \
    --quiet

#Create zip
cd build/lambda
zip -r ../lambda.zip . -q
cd ../..

#Create or update lambda function
aws --endpoint-url=http://localhost:4566 lambda create-function \
    --function-name credit-markets-pipeline \
    --runtime python3.11 \
    --handler credit_markets.lambda_handler.handler \
    --zip-file fileb://build/lambda.zip \
    --role arn:aws:iam::000000000000:role/lambda-role \
    2>/dev/null || \
aws --endpoint-url=http://localhost:4566 lambda update-function-code \
    --function-name credit-markets-pipeline \
    --zip-file fileb://build/lambda.zip

echo "Done. Test with:"
echo "aws --endpoint-url=http://localhost:4566 lambda invoke --function-name credit-markets-pipeline output.json && cat output.json"