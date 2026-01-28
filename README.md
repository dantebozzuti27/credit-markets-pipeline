# Credit Markets Pipeline

A data pipeline for monitoring credit market conditions. Ingests treasury yield data from FRED and company filings from SEC EDGAR, stores raw data in S3, transforms it into PostgreSQL, and provides analytics views for yield curve analysis.

## What It Does

- Pulls treasury yields (2Y, 10Y, 30Y) from the Federal Reserve Economic Data API
- Pulls SEC filings from EDGAR for specified companies
- Stores raw JSON in S3 (bronze layer)
- Transforms and loads into PostgreSQL (silver layer)
- Provides a yield curve view with 2s10s spread calculation (gold layer)

The 2s10s spread (10-year yield minus 2-year yield) is a key recession indicator. When it goes negative, the yield curve is inverted.

## Architecture

```
FRED API ──┐
           ├──> S3 (bronze) ──> PostgreSQL (silver) ──> Views (gold)
SEC API  ──┘
```

**Bronze layer:** Raw JSON files partitioned by date and source

**Silver layer:** Validated, typed, deduplicated tables with proper indexes

**Gold layer:** Analytical views (yield curve with spread calculations)

## Project Structure

```
credit-markets-pipeline/
├── src/credit_markets/
│   ├── config/
│   │   └── settings.py          # Pydantic settings from .env
│   ├── ingestion/
│   │   ├── fred.py              # FRED API client
│   │   └── sec.py               # SEC EDGAR client
│   ├── storage/
│   │   ├── s3.py                # S3 client (LocalStack compatible)
│   │   └── postgres.py          # PostgreSQL client with batch inserts
│   ├── transform/
│   │   ├── fred.py              # FRED data transformer
│   │   └── sec.py               # SEC data transformer
│   ├── pipeline/
│   │   └── daily.py             # Pipeline orchestrator
│   └── observability/
│       └── logging.py           # Structured JSON logging
├── infrastructure/
│   └── sql/init/
│       └── 01_schemas.sql       # Silver tables and gold views
├── docker-compose.yml           # PostgreSQL, LocalStack, Redis
├── pyproject.toml
└── .env.example
```

## Setup

### Prerequisites

- Python 3.11+
- Docker

### Installation

```bash
# Clone the repo
git clone https://github.com/dantebozzuti27/credit-markets-pipeline.git
cd credit-markets-pipeline

# Create virtual environment
python3.11 -m venv .venv
source .venv/bin/activate

# Install dependencies
pip install -e ".[dev]"

# Copy and configure environment
cp .env.example .env
# Edit .env with your FRED API key and email for SEC User-Agent
```

### Start Infrastructure

```bash
docker-compose up -d
```

This starts:
- PostgreSQL on port 5432
- LocalStack (S3) on port 4566
- Redis on port 6379

### Initialize Database

```bash
docker exec -i credit-markets-postgres psql -U postgres -d credit_markets < infrastructure/sql/init/01_schemas.sql
```

### Run the Pipeline

```bash
python src/credit_markets/pipeline/daily.py
```

Output:
```json
{"timestamp": "2026-01-27T20:32:42", "level": "INFO", "message": "Starting pipeline for 2026-01-27", "module": "daily"}
{"timestamp": "2026-01-27T20:32:53", "level": "INFO", "message": "Pipeline complete: 3 FRED rows, 0 SEC rows", "module": "daily"}
```

### Query the Data

```bash
# Check treasury yields
docker exec -it credit-markets-postgres psql -U postgres -d credit_markets \
  -c "SELECT * FROM silver.treasury_yields ORDER BY observation_date DESC LIMIT 5;"

# Check yield curve with spread
docker exec -it credit-markets-postgres psql -U postgres -d credit_markets \
  -c "SELECT * FROM gold.yield_curve LIMIT 10;"
```

Sample output:
```
observation_date | yield_2y | yield_10y | yield_30y | spread_2s10s
------------------+----------+-----------+-----------+--------------
2026-01-23       |   3.6000 |    4.2400 |    4.8200 |       0.6400
2026-01-22       |   3.6100 |    4.2600 |    4.8400 |       0.6500
```

## Configuration

Environment variables (see `.env.example`):

| Variable | Description |
|----------|-------------|
| FRED_API_KEY | Your FRED API key (free from fred.stlouisfed.org) |
| SEC_USER_AGENT | Your email (required by SEC for API access) |
| DATABASE_HOST | PostgreSQL host (default: localhost) |
| DATABASE_PORT | PostgreSQL port (default: 5432) |
| DATABASE_NAME | Database name (default: credit_markets) |
| DATABASE_USER | Database user (default: postgres) |
| DATABASE_PASSWORD | Database password |
| S3_BUCKET | S3 bucket name |
| AWS_REGION | AWS region |
| AWS_ENDPOINT_URL | LocalStack endpoint for local dev |

## Data Sources

**FRED (Federal Reserve Economic Data)**
- DGS2: 2-Year Treasury Constant Maturity Rate
- DGS10: 10-Year Treasury Constant Maturity Rate
- DGS30: 30-Year Treasury Constant Maturity Rate

**SEC EDGAR**
- Company filings (10-K, 10-Q, 8-K)
- Currently configured for Apple (CIK 320193) as example

## Database Schema

### Silver Layer

```sql
silver.treasury_yields
  - observation_date (DATE, PK)
  - series_id (VARCHAR, PK)
  - value (DECIMAL)
  - ingested_at (TIMESTAMP)

silver.sec_filings
  - accession_number (VARCHAR, PK)
  - cik (VARCHAR)
  - company_name (VARCHAR)
  - filing_type (VARCHAR)
  - filing_date (DATE)
  - ingested_at (TIMESTAMP)
```

### Gold Layer

```sql
gold.yield_curve (VIEW)
  - observation_date
  - yield_2y
  - yield_10y
  - yield_30y
  - spread_2s10s (10Y - 2Y)
```

## Tech Stack

- Python 3.11
- PostgreSQL 15
- LocalStack (S3)
- Docker Compose
- Pydantic (settings)
- httpx (HTTP client)
- psycopg2 (PostgreSQL driver)

## Future Work

- CLI interface
- Error handling with retries
- Unit and integration tests
- Airflow DAG for scheduling
- Additional treasury series and credit spreads
- Streamlit dashboard
- CI/CD pipeline

## License

MIT
