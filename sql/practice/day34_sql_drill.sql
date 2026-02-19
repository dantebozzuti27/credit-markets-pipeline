-- Day 34: SQL Drill — 10 Problems
-- All use silver.treasury_yields (observation_date, series_id, value).
-- Run from project root: docker cp sql/practice/day34_sql_drill.sql credit-markets-postgres:/tmp/
--                        docker exec -it credit-markets-postgres psql -U postgres -d credit_markets -f /tmp/day34_sql_drill.sql

-- ========== Problem 1: Lead — next day's value (DGS10) ==========
-- For DGS10, show observation_date, value, and the *next* day's value (LEAD). Order by observation_date.

-- Write your query below:

SELECT
     observation_date
    ,value
    ,LEAD(value) OVER (ORDER BY observation_date ASC) as next_val
FROM silver.treasury_yields
WHERE series_id = 'DGS10'
ORDER BY observation_date ASC

-- ========== Problem 2: First and last value per series (by date) ==========
-- For each series_id, show one row: series_id, first_date (earliest observation_date), last_date (latest), first_value, last_value. Use FIRST_VALUE/LAST_VALUE or MIN/MAX with GROUP BY.

-- Write your query below:

SELECT
     series_id
    ,MAX(observation_date) AS latest_date
    ,MIN(observation_date) AS first_date
    ,(array_agg(value ORDER BY observation_date))[1] AS first_value
    ,(array_agg(value ORDER BY observation_date DESC))[1] AS last_value
FROM silver.treasury_yields
GROUP BY
    series_id

-- ========== Problem 3: Series that have at least one value above 5.0 ==========
-- List distinct series_id where there exists at least one row with value > 5.0. Use EXISTS or IN or GROUP BY + HAVING.

-- Write your query below:

SELECT distinct
    series_id
FROM silver.treasury_yields
WHERE value > 5.0

-- ========== Problem 4: Rolling 7-day average (DGS10) ==========
-- For DGS10, show observation_date, value, and the 7-day backward-looking average (current + 6 preceding rows by date). Use AVG(value) OVER (ORDER BY observation_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW).

-- Write your query below:

SELECT
     observation_date
    ,value
    ,AVG(value) OVER (ORDER BY observation_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS seven_day_ma
FROM silver.treasury_yields
WHERE series_id = 'DGS10'
ORDER BY observation_date DESC

-- ========== Problem 5: Count of observations per series ==========
-- One row per series_id: series_id, obs_count (number of rows). Use GROUP BY series_id, COUNT(*).

-- Write your query below:

SELECT
     series_id
    ,COUNT(*) AS obs_count
FROM silver.treasury_yields
GROUP BY series_id

-- ========== Problem 6: Dates where DGS10 was below its own 7-day moving average ==========
-- For DGS10 only: observation_date, value, and a flag 1 when value < that day's 7-day backward-moving average, else 0. (Compute the 7d avg in the same query, then CASE.)

-- Write your query below:

SELECT
     observation_date
    ,value
    ,AVG(value) OVER (ORDER BY observation_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS seven_day_ma
    ,CASE WHEN value < AVG(value) OVER (ORDER BY observation_date ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) THEN 1 ELSE 0 END AS flag
FROM silver.treasury_yields
WHERE series_id = 'DGS10'
ORDER BY observation_date DESC

-- ========== Problem 7: Third-highest value per series ==========
-- One row per series: series_id, observation_date, value for the *third*-highest value in that series. Same pattern as “second-highest” but rank = 3.

-- Write your query below:

WITH 
cte AS (
    SELECT
         series_id
        ,observation_date
        ,value
        ,ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY value DESC) AS rn
    FROM silver.treasury_yields
)
    SELECT
         series_id
        ,observation_date
        ,value
    FROM cte
    WHERE rn = 3


-- ========== Problem 8: Month with highest average yield (over all series) ==========
-- One row: year, month (e.g. 2024, 1), and avg_yield for that month (avg of value across all series and days in that month). Then keep only the (year, month) with the highest avg_yield. Use a CTE to get monthly averages, then filter to max.

-- Write your query below:

WITH
cte AS (
    SELECT
         EXTRACT(YEAR FROM observation_date) AS year
        ,EXTRACT(MONTH FROM observation_date) AS month
        ,AVG(value) AS avg_yield
    FROM silver.treasury_yields
    GROUP BY year, month
)
    SELECT
         year
        ,month
        ,avg_yield
    FROM cte
    WHERE avg_yield = (SELECT MAX(avg_yield) FROM cte)


-- ========== Problem 9: Percent rank of value within each series ==========
-- For each row, show series_id, observation_date, value, and pct_rank: the percent_rank of value within that series (ORDER BY value). So 0 = smallest, 1 = largest in the series.

-- Write your query below:

SELECT
     series_id
    ,observation_date
    ,value
    ,PERCENT_RANK() OVER (PARTITION BY series_id ORDER BY value ASC) AS pct_rank
FROM silver.treasury_yields


-- ========== Problem 10: Consecutive dates with same value (DGS10) ==========
-- For DGS10, find pairs of consecutive observation_dates where the value was the same. Output: prev_date, next_date, value (the repeated value). Use LAG(value) and LAG(observation_date); filter where value = LAG(value).

-- Write your query below:

WITH
cte AS (
    SELECT
         observation_date
        ,value
        ,LAG(value) OVER (ORDER BY observation_date) AS prev_value
        ,LAG(observation_date) OVER (ORDER BY observation_date)AS prev_date    
    FROM silver.treasury_yields
    WHERE series_id = 'DGS10'
)
SELECT
     observation_date AS next_date
    ,prev_date
    ,value
FROM cte
WHERE value = prev_value