-- Day 30: Advanced Aggregations Practice
-- GROUPING SETS, CUBE, ROLLUP

-- Background:
-- These are extensions to GROUP BY that let you compute multiple grouping levels in one query.
-- Without them, you'd need UNION ALL to combine different aggregation levels.

-- GROUPING SETS: explicitly define which groupings you want
-- ROLLUP: hierarchical subtotals (year → month → day → grand total)
-- CUBE: all possible combinations (2^n groupings for n columns)

-- Problem 1: GROUPING SETS
-- Show average yield for DGS10 grouped by:
--   1. Year only
--   2. Year + Month
--   3. Grand total (no grouping)
-- All in one query.

-- Hint: EXTRACT(YEAR FROM date) and EXTRACT(MONTH FROM date)

-- Write your query below:

SELECT
     EXTRACT(YEAR FROM observation_date) AS year
    ,EXTRACT(MONTH FROM observation_date) AS month
    ,AVG(value) as avg_yield
FROM
    silver.treasury_yields
WHERE
    series_id = 'DGS10'
GROUP BY GROUPING SETS (
     (year, month)
    ,(year)
    ,()
);

-- Problem 2: ROLLUP
-- Same data (DGS10): show avg yield with hierarchical subtotals.
-- Levels: (series_id, year, month) → (series_id, year) → (series_id) → grand total.
-- Use ROLLUP(series_id, year, month). Filter to DGS10 in WHERE.
-- Include GROUPING(series_id), GROUPING(year), GROUPING(month) so you can see which level each row is.
-- Note: With WHERE series_id = 'DGS10', series_id is constant; ROLLUP still produces 4 levels (detail, year subtotal, series subtotal, grand total).

-- Write your query below:

SELECT
     series_id
    ,EXTRACT(YEAR FROM observation_date) AS year
    ,EXTRACT(MONTH FROM observation_date) AS month
    ,AVG(value) AS avg_value
    ,GROUPING(series_id)
    ,GROUPING(EXTRACT(YEAR FROM observation_date))
    ,GROUPING(EXTRACT(MONTH FROM observation_date))
FROM
    silver.treasury_yields
WHERE 
    series_id = 'DGS10'
GROUP BY ROLLUP(series_id, year, month)


-- Problem 3: CUBE
-- All series (no filter): show count of observations for every combination of (series_id, year).
-- CUBE gives you: (series_id, year), (series_id), (year), ().
-- Show: series_id, year, observation_count, and GROUPING(series_id), GROUPING(year) to label the level.

-- Write your query below:

SELECT COUNT(*)
     series_id
    ,EXTRACT(YEAR FROM observation_date) AS year
FROM
    silver.treasury_yields
GROUP BY CUBE(series_id, year)
