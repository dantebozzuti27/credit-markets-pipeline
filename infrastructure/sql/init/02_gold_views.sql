-- Enhanced Gold Layer Views

CREATE OR REPLACE VIEW gold.full_yield_curve AS
SELECT
     observation_date
    ,MAX(CASE WHEN series_id = 'DGS1MO' THEN value END)         AS yield_1m
    ,MAX(CASE WHEN series_id = 'DGS3MO' THEN value END)         AS yield_3m
    ,MAX(CASE WHEN series_id = 'DGS6MO' THEN value END)         AS yield_6m
    ,MAX(CASE WHEN series_id = 'DGS1' THEN value END)           AS yield_1y
    ,MAX(CASE WHEN series_id = 'DGS2' THEN value END)           AS yield_2y
    ,MAX(CASE WHEN series_id = 'DGS5' THEN value END)           AS yield_5y
    ,MAX(CASE WHEN series_id = 'DGS7' THEN value END)           AS yield_7y
    ,MAX(CASE WHEN series_id = 'DGS10' THEN value END)          AS yield_10y
    ,MAX(CASE WHEN series_id = 'DGS20' THEN value END)          AS yield_20y
    ,MAX(CASE WHEN series_id = 'DGS30' THEN value END)          AS yield_30y
    --Key Spreads
    ,MAX(CASE WHEN series_id = 'DGS10' THEN value END) -
        MAX(CASE WHEN series_id = 'DGS2' THEN value END)        AS spread_2s10s
    ,MAX(CASE WHEN series_id = 'DGS10' THEN value END) -
        MAX(CASE WHEN series_id = 'DGS3MO' THEN value END)      AS spread_3m10y
    FROM silver.treasury_yields
    WHERE series_id LIKE 'DGS%'
    GROUP BY observation_date
    ORDER BY observation_date DESC;

CREATE OR REPLACE VIEW gold.credit_stress AS
SELECT
     observation_date
    ,MAX(CASE WHEN series_id = 'BAMLH0A0HYM2' THEN value END)    AS hy_spread
    ,MAX(CASE WHEN series_id = 'VIXCLS' THEN value END)         AS vix
    ,MAX(CASE WHEN series_id = 'TEDRATE' THEN value END)       AS ted_spread
    ,MAX(CASE WHEN series_id = 'FEDFUNDS' THEN value END)      AS fed_funds
    -- Stress indicator: normalized combination
    ,ROUND(
        (COALESCE(MAX(CASE WHEN series_id = 'BAMLH0A0HYM2' THEN value END), 0) / 10.0) +
        (COALESCE(MAX(CASE WHEN series_id = 'VIXCLS' THEN value END), 0) / 40.0)
    , 2) AS stress_score
FROM silver.treasury_yields
GROUP BY observation_date
ORDER BY observation_date DESC;

CREATE OR REPLACE VIEW gold.company_filings_summary AS
SELECT
    c.ticker
    ,c.name AS company_name
    ,f.cik
    ,COUNT(*) AS total_filings
    ,COUNT(CASE WHEN f.filing_type = '10-K' THEN 1 END) AS annual_reports
    ,COUNT(CASE WHEN f.filing_type = '10-Q' THEN 1 END) AS quarterly_reports
    ,COUNT(CASE WHEN f.filing_type = '8-K' THEN 1 END) AS current_reports
    ,MIN(f.filing_date) AS earliest_filing
    ,MAX(f.filing_date) AS latest_filing
FROM silver.sec_filings f
JOIN reference.sec_companies c ON LTRIM(f.cik, '0') = c.cik
GROUP BY c.ticker, c.name, f.cik
ORDER BY total_filings DESC;

CREATE OR REPLACE VIEW gold.daily_snapshot AS
SELECT
    y.observation_date
    ,y.yield_2y
    ,y.yield_10y
    ,y.spread_2s10s
    ,s.hy_spread
    ,s.vix
    ,s.stress_score
    ,CASE 
        WHEN y.spread_2s10s < 0 THEN 'INVERTED'
        WHEN y.spread_2s10s < 0.25 THEN 'FLAT'
        ELSE 'NORMAL'
    END AS curve_status
    ,CASE
        WHEN s.stress_score > 1.5 THEN 'HIGH'
        WHEN s.stress_score > 0.8 THEN 'ELEVATED'
        ELSE 'LOW'
    END AS stress_level
FROM gold.full_yield_curve y
LEFT JOIN gold.credit_stress s ON y.observation_date = s.observation_date
ORDER BY y.observation_date DESC;