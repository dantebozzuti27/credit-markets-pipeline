# Phase 1 Summary (Month 1)

**What it does:** Daily batch pipeline: FRED + SEC EDGAR → S3 bronze → PostgreSQL silver/gold. Airflow DAG with S3 sensor; optional Lambda post-process; Great Expectations validation; Slack on failure.

**Tradeoffs:**
- Single-region, single Postgres (CP-style under failure; we prefer consistency over availability).
- Idempotent/upsert-friendly loads where designed (natural keys).
- Batch daily, not real-time.

**Interview sound bite:** "Daily batch pipeline: we ingest FRED and SEC data into S3, load into Postgres with silver and gold layers. Airflow runs on a schedule with an S3 sensor so we only run when data lands. Great Expectations for validation, Slack on failure. Single-region, CP-oriented. For exactly-once we use idempotent writes and natural keys where it matters."
