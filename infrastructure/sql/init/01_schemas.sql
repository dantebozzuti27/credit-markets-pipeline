-- Credit Markets Pipeline - Database Schema

CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;

CREATE TABLE IF NOT EXISTS silver.treasury_yields (
     observation_date DATE NOT NULL
    ,series_id VARCHAR(20) NOT NULL
    ,value DECIMAL(10, 4)
    ,ingested_at TIMESTAMP NOT NULL DEFAULT NOW()
    ,PRIMARY KEY (observation_date, series_id)
);

CREATE TABLE IF NOT EXISTS silver.sec_filings (
     accession_number VARCHAR(25) PRIMARY KEY
    ,cik VARCHAR(10) NOT NULL
    ,company_name VARCHAR(255)
    ,filing_type VARCHAR(20)
    ,filing_date DATE
    ,ingested_at TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_treasury_yields_date
    ON silver.treasury_yields (observation_date DESC);

CREATE INDEX IF NOT EXISTS idx_sec_filings_date
    ON silver.sec_filings (filing_date DESC);

CREATE INDEX IF NOT EXISTS idx_sec_filings_cik
    ON silver.sec_filings (cik);

CREATE OR REPLACE VIEW gold.yield_curve AS
SELECT
    observation_date
    ,MAX(CASE WHEN series_id = 'DGS2' THEN value END)           AS yield_2y
    ,MAX(CASE WHEN series_id = 'DGS10' THEN value END)          AS yield_10y
    ,MAX(CASE WHEN series_id = 'DGS30' THEN value END)          AS yield_30y
    ,MAX(CASE WHEN series_id = 'DGS10' THEN value END)          -
        MAX(CASE WHEN series_id = 'DGS2' THEN value END)        AS spread_2s10s
    FROM silver.treasury_yields
    GROUP BY observation_date
    ORDER BY observation_date DESC;