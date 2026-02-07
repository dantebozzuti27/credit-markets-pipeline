# Credit Markets Pipeline - Runbook

Operational guide for deploying, monitoring, and troubleshooting the pipeline.

## Quick Start

```bash
# Start all services
docker compose up -d

# Check service health
docker ps

# Access Airflow UI
open http://localhost:8080
```

## Daily Operations

### Manual Pipeline Trigger

```bash
docker exec -it credit-markets-airflow airflow dags trigger credit_markets_daily
```

### Check DAG Status

```bash
docker exec -it credit-markets-airflow airflow dags list-runs -d credit_markets_daily --limit 5
```

### View Task Logs

```bash
# Via Airflow UI: Click task → Logs

# Or via CLI
docker exec -it credit-markets-airflow airflow tasks logs credit_markets_daily run_daily_pipeline <execution_date>
```

## Monitoring

### Slack Alerts

Alerts are sent on:
- **DAG failure:** Task-level failures trigger immediate alerts
- **DAG success:** Completion notification with execution date

Configure webhook in `.env`:
```
SLACK_WEBHOOK_URL=https://hooks.slack.com/services/...
```

### CloudWatch Metrics

Metrics published (if CloudWatch available):
- `PipelineDuration` (Seconds)
- `PipelineSuccess` (Count)

Namespace: `CreditMarketsPipeline`

### Health Checks

```bash
# Postgres
docker exec -it credit-markets-postgres pg_isready -U postgres

# LocalStack S3
docker exec -it credit-markets-localstack awslocal s3 ls

# Airflow
docker exec -it credit-markets-airflow airflow version
```

## Troubleshooting

### DAG Not Appearing in UI

```bash
# Check for import errors
docker exec -it credit-markets-airflow python /opt/airflow/dags/credit_markets_daily.py

# Force DAG refresh
docker exec -it credit-markets-airflow airflow dags reserialize
```

### S3 Sensor Stuck

The sensor waits for files matching `bronze/fred/{{ ds }}/*`.

```bash
# Check if file exists
docker exec -it credit-markets-localstack awslocal s3 ls s3://credit-markets-data/bronze/fred/2026-02-07/ --recursive

# Create test file manually
docker exec -it credit-markets-localstack sh -c 'echo "{}" | awslocal s3 cp - s3://credit-markets-data/bronze/fred/2026-02-07/test.json'
```

Sensor settings:
- `poke_interval`: 60 seconds
- `timeout`: 3600 seconds (1 hour)
- `mode`: reschedule (releases worker between pokes)
- `soft_fail`: True (skips instead of failing on timeout)

### Data Quality Validation Failed

Check GX validation results:

```bash
# Run validation manually
cd /path/to/credit-markets-pipeline
python -m credit_markets.quality.expectations
```

Common issues:
- **Column not found:** Schema mismatch between expectations and actual table
- **Connection refused:** Postgres not running or wrong host (use container name in Docker)

### Lambda Not Invoking

```bash
# Check Lambda exists
docker exec -it credit-markets-localstack awslocal lambda list-functions

# Recreate Lambda
cd infrastructure/lambda
zip post_processor.zip post_processor.py
docker exec -i credit-markets-localstack awslocal lambda create-function \
  --function-name credit-markets-processor \
  --runtime python3.11 \
  --handler post_processor.handler \
  --zip-file fileb:///dev/stdin \
  --role arn:aws:iam::000000000000:role/lambda-role \
  < post_processor.zip
```

### Container Unhealthy

```bash
# Check logs
docker logs credit-markets-airflow --tail 100

# Restart container
docker restart credit-markets-airflow

# Full restart
docker compose down && docker compose up -d
```

## Deployment

### Local Development

```bash
# Install dependencies
pip install -e ".[dev]"

# Start infrastructure
docker compose up -d

# Run pipeline manually
python -m credit_markets.cli run
```

### Production Checklist

- [ ] Set real AWS credentials (not LocalStack)
- [ ] Configure production Postgres connection
- [ ] Set up Slack webhook for alerts
- [ ] Enable CloudWatch metrics
- [ ] Configure proper Airflow executor (Celery/Kubernetes)
- [ ] Set up log aggregation
- [ ] Configure backup strategy for Postgres

## Backfill

Run pipeline for a specific date:

```bash
# Via CLI
python -m credit_markets.cli run --target-date 2026-01-15

# Via Airflow (backfill range)
docker exec -it credit-markets-airflow airflow dags backfill \
  credit_markets_daily \
  --start-date 2026-01-01 \
  --end-date 2026-01-31
```

## Database Maintenance

### Check Row Counts

```bash
docker exec -it credit-markets-postgres psql -U postgres -d credit_markets -c "
SELECT 
  'treasury_yields' as table_name, COUNT(*) as rows FROM silver.treasury_yields
UNION ALL
SELECT 
  'sec_filings', COUNT(*) FROM silver.sec_filings;
"
```

### Vacuum/Analyze

```bash
docker exec -it credit-markets-postgres psql -U postgres -d credit_markets -c "
VACUUM ANALYZE silver.treasury_yields;
VACUUM ANALYZE silver.sec_filings;
"
```

## Contacts

- **Pipeline Owner:** [Your Name]
- **On-Call:** Check PagerDuty/Slack
