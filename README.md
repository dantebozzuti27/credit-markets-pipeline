# Credit Markets Pipeline

A data pipeline for monitoring credit market conditions. Ingests treasury yield data from FRED and company filings from SEC EDGAR, stores raw data in S3, transforms it into PostgreSQL, and provides analytics views for yield curve analysis.

## What It Does

- Pulls 18 FRED series: full treasury curve (1M to 30Y), credit spreads (IG/HY), VIX, Fed Funds, SOFR
- Pulls SEC filings from EDGAR for 33 major companies (banks, tech, utilities)
- Stores raw JSON in S3 (bronze layer)
- Transforms and loads into PostgreSQL (silver layer)
- Provides a yield curve view with 2s10s spread calculation (gold layer)

Configuration is database-driven. Add or remove tracked series/companies via SQL, not code changes.

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
│   │   ├── fred.py              # FRED API client with retry
│   │   └── sec.py               # SEC EDGAR client with retry
│   ├── storage/
│   │   ├── s3.py                # S3 client (LocalStack compatible)
│   │   └── postgres.py          # PostgreSQL client with batch inserts
│   ├── transform/
│   │   ├── fred.py              # FRED data transformer
│   │   └── sec.py               # SEC data transformer
│   ├── pipeline/
│   │   └── daily.py             # Pipeline orchestrator (database-driven)
│   ├── utils/
│   │   └── retry.py             # Retry decorator with exponential backoff
│   ├── observability/
│   │   └── logging.py           # Structured JSON logging
│   └── cli.py                   # Click CLI interface
├── scripts/
│   ├── fetch_companies.py       # Populate SEC companies from API
│   └── seed_fred_series.py      # Seed FRED series reference data
├── infrastructure/
│   └── sql/init/
│       ├── 00_reference.sql     # Reference tables (fred_series, sec_companies)
│       └── 01_schemas.sql       # Silver tables and gold views
├── tests/
│   └── unit/
│       └── test_retry.py        # Unit tests for retry decorator
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
docker exec -i credit-markets-postgres psql -U postgres -d credit_markets < infrastructure/sql/init/00_reference.sql
docker exec -i credit-markets-postgres psql -U postgres -d credit_markets < infrastructure/sql/init/01_schemas.sql
```

### Populate Reference Data

```bash
# Fetch SEC company CIKs from SEC API
python scripts/fetch_companies.py

# Seed FRED series configuration
python scripts/seed_fred_series.py

# Mark specific companies as active (example: major banks and tech)
docker exec -it credit-markets-postgres psql -U postgres -d credit_markets -c "
UPDATE reference.sec_companies SET is_active = FALSE;
UPDATE reference.sec_companies SET is_active = TRUE 
WHERE ticker IN ('JPM', 'BAC', 'GS', 'AAPL', 'MSFT', 'NVDA');
"
```

### Run the Pipeline

```bash
# Using CLI
python -m credit_markets.cli run

# With specific date
python -m credit_markets.cli run --target-date 2026-01-15
```

Output:
```json
{"timestamp": "2026-01-29T02:17:41", "level": "INFO", "message": "Starting pipeline for 2026-01-28", "module": "daily"}
{"timestamp": "2026-01-29T02:20:55", "level": "INFO", "message": "Pipeline complete: 138079 FRED rows, 32125 SEC rows", "module": "daily"}
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

**FRED (Federal Reserve Economic Data)** - 18 series configured in `reference.fred_series`:

| Category | Series |
|----------|--------|
| Treasury Curve | DGS1MO, DGS3MO, DGS6MO, DGS1, DGS2, DGS5, DGS7, DGS10, DGS20, DGS30 |
| Credit Spreads | BAMLC0A0CM (IG), BAMLC0A4CBBB (BBB), BAMLH0A0HYM2 (HY) |
| Policy Rates | FEDFUNDS, SOFR |
| Risk Indicators | VIXCLS, TEDRATE |
| Inflation | T10YIE |

**SEC EDGAR** - 33 companies configured in `reference.sec_companies`:
- Major banks: JPM, BAC, C, WFC, GS, MS
- Big tech: AAPL, MSFT, AMZN, GOOG, META, NVDA
- Utilities, industrials, healthcare

Add or remove tracked entities via SQL:
```sql
-- Add a new FRED series
INSERT INTO reference.fred_series (series_id, name, category) 
VALUES ('DFF', 'Federal Funds Rate Daily', 'policy');

-- Stop tracking a company
UPDATE reference.sec_companies SET is_active = FALSE WHERE ticker = 'GE';
```

## Database Schema

### Reference Layer (Configuration)

```sql
reference.fred_series
  - series_id (VARCHAR, PK)
  - name (VARCHAR)
  - category (VARCHAR)
  - is_active (BOOLEAN)
  - created_at (TIMESTAMP)

reference.sec_companies
  - cik (VARCHAR, PK)
  - ticker (VARCHAR)
  - name (VARCHAR)
  - is_active (BOOLEAN)
  - created_at (TIMESTAMP)
```

### Silver Layer (Cleaned Data)

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

### Gold Layer (Analytics)

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
- Click (CLI)
- pytest (testing)

## Orchestration

The pipeline is orchestrated with Apache Airflow, running locally via Docker Compose.

```bash
# Start the full stack (Postgres, LocalStack, Redis, Airflow)
docker compose up -d

# Access Airflow UI
open http://localhost:8080
# Login: admin / (check standalone_admin_password.txt in container)
```

The DAG `credit_markets_daily` runs at 6 AM UTC daily. Manual triggers available via the Airflow UI.

**DAG location:** `airflow/dags/credit_markets_daily.py`

## CI/CD

GitHub Actions runs on every push:
- Linting with ruff
- Unit tests with pytest

See `.github/workflows/ci.yml`.

## Future Work

- S3 sensors to trigger DAG on file arrival
- Backfill CLI command for historical date ranges
- Enhanced gold layer views (credit stress indicators, yield curve analysis)
- Streamlit dashboard
- AWS Lambda integration with Airflow

## License

MIT
