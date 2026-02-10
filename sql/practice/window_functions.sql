-- Day 29: Window Functions Practice
-- Run with: docker exec credit-markets-postgres psql -U postgres -d credit_markets -f /path/to/this/file.sql

-- Practice Problem 1:
-- Show each day's 10-year yield (DGS10) with:
--   1. The previous day's yield
--   2. The day-over-day change
--   3. Whether the change was positive, negative, or zero

-- Write your query below:

-- Optimized: compute LAG() once in CTE, reference prev_value everywhere
WITH lagged AS (
    SELECT
        observation_date,
        value,
        LAG(value) OVER (PARTITION BY series_id ORDER BY observation_date) AS prev_value
    FROM silver.treasury_yields
    WHERE series_id = 'DGS10'
)
SELECT
    observation_date,
    value,
    prev_value,
    value - prev_value AS daily_change,
    CASE 
        WHEN prev_value IS NULL THEN 'N/A'
        WHEN value > prev_value THEN 'positive'
        WHEN value < prev_value THEN 'negative'
        ELSE 'zero'
    END AS direction
FROM lagged
ORDER BY observation_date;

-- Problem 2: Running total of yields by series
-- Show observation_date, series_id, value, and running_total
-- running_total = sum of all values from start up to current row

-- Optimized: explicit ROWS frame for clarity, final ORDER BY for deterministic output
SELECT
    observation_date,
    series_id,
    value,
    SUM(value) OVER (
        PARTITION BY series_id
        ORDER BY observation_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS running_total
FROM silver.treasury_yields
ORDER BY series_id, observation_date;

-- Problem 3: 7-day moving average
-- For DGS10, show:
--   1. observation_date
--   2. value (current day's yield)
--   3. avg_7d (average of current row + 6 preceding rows)
-- Only include rows where we have a full 7-day window (no partial averages)

-- Write your query below:

WITH avg_7d AS (
    SELECT
        observation_date
        ,value
        ,AVG(value) OVER (
            PARTITION BY series_id
            ORDER BY observation_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as avg_7d
        ,COUNT(*) OVER (
            PARTITION BY series_id
            ORDER BY observation_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as window_size
    FROM silver.treasury_yields
    WHERE series_id = 'DGS10'
)
SELECT
     observation_date
    ,value
    ,avg_7d
FROM avg_7d
WHERE window_size = 7
ORDER BY observation_date;

-- Problem 4: RANK vs DENSE_RANK
-- Rank all treasury series by their average yield (highest = rank 1)
-- Show: series_id, avg_yield, rank_with_gaps, rank_without_gaps
-- Use the appropriate ranking function for each column

-- Write your query below:

WITH avgs AS (
    SELECT
         series_id
        ,AVG(value) AS avg_yield
    FROM silver.treasury_yields
    GROUP BY series_id
)
SELECT
     series_id
    ,avg_yield
    ,RANK() OVER (ORDER BY avg_yield DESC) AS rank_with_gaps
    ,DENSE_RANK() OVER (ORDER BY avg_yield DESC) AS rank_without_gaps
FROM avgs;

-- Problem 5: NTILE - Percentile Buckets
-- For DGS10, divide all observations into 4 quartiles by yield value
-- Show: observation_date, value, quartile (1=lowest 25%, 4=highest 25%)
-- Order by observation_date

-- Write your query below:


SELECT
     value
    ,observation_date
    ,NTILE(4) OVER (ORDER BY value ASC) AS quartile
FROM silver.treasury_yields
WHERE series_id = 'DGS10'
ORDER BY observation_date ASC