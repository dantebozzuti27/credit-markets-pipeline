-- Day 33: SQL Drill — 10 Problems
-- All use silver.treasury_yields (observation_date, series_id, value).
-- Run via: docker cp sql/practice/day33_sql_drill.sql credit-markets-postgres:/tmp/ (from project root)
--          docker exec -it credit-markets-postgres psql -U postgres -d credit_markets -f /tmp/day33_sql_drill.sql

-- =============================================================================
-- WALKTHROUGH: Problem 1 (Top N per group)
-- =============================================================================
-- 1. GOAL: "For each series_id, the 3 rows with highest value."
--    That's "per group" (per series_id) + "ordered by value" + "take top 3."
--
-- 2. WHY A WINDOW: We need a rank *within* each series. So we PARTITION BY series_id
--    (reset the counter for each series) and ORDER BY value DESC (biggest first).
--    ROW_NUMBER() gives 1, 2, 3, ... per series. Then we keep only rank <= 3.
--
-- 3. ROW_NUMBER vs RANK vs DENSE_RANK:
--    - ROW_NUMBER: 1,2,3,4,... even if two rows tie on value (arbitrary order between ties).
--    - RANK: ties get same number, then skip (e.g. 1,2,2,4). "Top 3" could be 4 rows.
--    - DENSE_RANK: ties same number, no skip (1,2,2,3). "Top 3" could be 4+ rows.
--    For "exactly 3 rows per series" use ROW_NUMBER. For "top 3 distinct values" use DENSE_RANK.
--
-- 4. PATTERN: Compute rank in a subquery/CTE, then outer query WHERE rn <= 3.
-- =============================================================================

-- ========== Problem 1: Top 3 highest yields per series ==========
-- For each series_id, show the 3 rows with the highest value.
-- Output: series_id, observation_date, value (and optionally rank).
-- Hint: ROW_NUMBER() or DENSE_RANK() partitioned by series_id, ordered by value DESC; filter rank <= 3.

-- Write your query below:

WITH cte AS (
    SELECT
        ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY value DESC) AS rn
        ,series_id
        ,observation_date
        ,value
    FROM silver.treasury_yields
)
    SELECT
        rn
        ,series_id
        ,value
        ,observation_date
    FROM cte
    WHERE rn <= 3

-- ========== Problem 2: Series with above-average yield on any day ==========
-- STEP 1: We need "overall average" — one number for the whole table. That's AVG(value) with no GROUP BY.
-- STEP 2: We need every row where value > that number. So we must either (a) put the avg in a CTE and
--         JOIN to the main table, or (b) use a scalar subquery in the WHERE clause: WHERE value > (SELECT AVG(value) FROM silver.treasury_yields).
-- STEP 3: Output: series_id, observation_date, value (and we're done).
-- CTE version is clearer for reading; subquery version is one less line. Both correct.

-- Write your query below:

SELECT
    value
    ,series_id
    ,observation_date
FROM silver.treasury_yields
WHERE value > (SELECT AVG(value) FROM silver.treasury_yields)


-- ========== Problem 3: Consecutive days with rising yield (DGS10 only) ==========
-- STEP 1: Restrict to DGS10: WHERE series_id = 'DGS10'.
-- STEP 2: For each row we need "previous day's value". Same table, previous row by date = LAG(value) OVER (ORDER BY observation_date). Call it prev_value.
-- STEP 3: Flag = 1 when value > prev_value, else 0. So: CASE WHEN value > LAG(value) OVER (...) THEN 1 ELSE 0 END. First row has prev_value NULL; value > NULL is NULL, so use COALESCE(..., 0) so first row is 0.
-- STEP 4: SELECT observation_date, value, flag. Order by observation_date.

-- Write your query below:

SELECT
     observation_date
    ,value  
    ,COALESCE(CASE
        WHEN value > LAG(value) OVER (ORDER BY observation_date)
        THEN 1
        ELSE 0
        END, 0) AS flag
FROM silver.treasury_yields
WHERE series_id = 'DGS10'
ORDER BY observation_date



-- ========== Problem 4: Running count of observations per series ==========
-- STEP 1: "Running count" = at each row, how many rows so far in that series? That's a window: same partition (series_id), order by observation_date, and we want "count of rows from start of partition to current row". Default window is ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW, so COUNT(*) OVER (PARTITION BY series_id ORDER BY observation_date) does it.
-- STEP 2: ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY observation_date) gives 1,2,3,... and is equivalent for "running count" when there are no ties in observation_date. COUNT(*) is more literal.
-- STEP 3: SELECT series_id, observation_date, value, running_count. Order by series_id, observation_date.

-- Write your query below:

SELECT
     series_id
    ,observation_date
    ,value
    ,ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY observation_date) AS running_count
FROM silver.treasury_yields

-- ========== Problem 5: Year-over-year comparison ==========
-- STEP 1: One row per (series_id, year). So we must aggregate first: GROUP BY series_id, EXTRACT(YEAR FROM observation_date), and AVG(value) AS avg_yield. That gives us one row per series per year.
-- STEP 2: "Previous year's avg_yield" = LAG(avg_yield) but over years, per series. So we need a window: PARTITION BY series_id ORDER BY year. So we put the grouped result in a CTE, then in the outer query add LAG(avg_yield) OVER (PARTITION BY series_id ORDER BY year) AS prev_year_avg_yield.
-- STEP 3: Output: year, series_id, avg_yield, prev_year_avg_yield. First year per series will have prev_year_avg_yield NULL.

-- Write your query below:

WITH yr AS (
    SELECT
         series_id
        ,EXTRACT(YEAR FROM observation_date) AS year
        ,AVG(value) AS avg_yield
    FROM silver.treasury_yields
    GROUP BY series_id, EXTRACT(YEAR FROM observation_date)
)
    SELECT
         year
        ,series_id
        ,avg_yield
        ,LAG(avg_yield) OVER (PARTITION BY series_id ORDER BY year) AS prev_year_avg_yield
    FROM yr
    ORDER BY series_id, year

-- ========== Problem 6: Percent of total yield by series ==========
-- STEP 1: "Sum value per series" = GROUP BY series_id, SUM(value) AS total_value.
-- STEP 2: "Grand total" = SUM of all values. So we need that in the same query. Option A: subquery SELECT SUM(value) FROM silver.treasury_yields. Option B: window with no partition: SUM(value) OVER () gives grand total if we're at row level—but we're at series level after GROUP BY, so we need the grand total once. Easiest: CTE that has series totals, and also (SELECT SUM(value) FROM silver.treasury_yields) AS grand_total in the outer SELECT, then pct_of_total = 100.0 * total_value / grand_total.
-- STEP 3: Output: series_id, total_value, pct_of_total. One row per series.

-- Write your query below:

WITH series_totals AS (
        SELECT
             series_id
            ,SUM(value) AS total_value
        FROM silver.treasury_yields
        GROUP BY series_id
),
    grand AS (
        SELECT
             SUM(value) as total
        FROM silver.treasury_yields
    )
    SELECT
         series_id
        ,total_value
        ,100.0 * total_value / total AS pct_of_total
    FROM series_totals CROSS JOIN grand
    ORDER BY series_id

-- ========== Problem 7: Dates where DGS10 was the highest among all series ==========
-- STEP 1: "Maximum across all series that day" = per observation_date, MAX(value). So we need the max per date: GROUP BY observation_date, MAX(value) AS max_val. That gives one row per date with the highest yield that day.
-- STEP 2: We want only dates where that max belongs to DGS10. So we need DGS10's value on that date to equal the max. Approach: get DGS10 rows (WHERE series_id = 'DGS10'), then for each, check if that value equals the max for that date. So: JOIN (SELECT observation_date, MAX(value) AS max_val FROM silver.treasury_yields GROUP BY observation_date) m ON d.observation_date = m.observation_date AND d.value = m.max_val. So we're keeping DGS10 rows where value = daily max.
-- STEP 3: Output: observation_date, value (DGS10's value, which is the max). One row per date where DGS10 was the top.

-- Write your query below:

WITH max_per_date AS (
    SELECT
         observation_date
        ,MAX(value) AS max_val
    FROM silver.treasury_yields
    GROUP BY observation_date
)
    SELECT
         observation_date
        ,value
    FROM silver.treasury_yields d
    JOIN max_per_date m ON
    d.observation_date = m.observation_date
    AND d.value = m.max_val
    WHERE d.series_id = 'DGS10'
    ORDER BY observation_date

-- ========== Problem 8: Moving standard deviation (DGS10, 5-day window) ==========
-- STEP 1: Restrict to DGS10: WHERE series_id = 'DGS10'.
-- STEP 2: "5-day backward-looking" = current row plus 4 rows before = 5 rows. In window terms: ROWS BETWEEN 4 PRECEDING AND CURRENT ROW. So STDDEV(value) OVER (ORDER BY observation_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS moving_stddev. For the first 4 rows there are fewer than 5 rows in the window; PostgreSQL still computes stddev over available rows (e.g. row 1 has 1 value, row 4 has 4).
-- STEP 3: SELECT observation_date, value, moving_stddev. Order by observation_date.

-- Write your query below:

SELECT
     observation_date
    ,value
    ,STDDEV(value) OVER (ORDER BY observation_date ROWS BETWEEN 4 PRECEDING AND CURRENT ROW) AS moving_stddev
FROM silver.treasury_yields
WHERE series_id = 'DGS10'

-- ========== Problem 9: Second-highest yield per series ==========
-- STEP 1: Same "top N per group" as Problem 1, but N=2 and we want exactly one row per series (the second-highest). So ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY value DESC) AS rn, then WHERE rn = 2.
-- STEP 2: If two rows tie for second-highest, ROW_NUMBER gives one of them 2 and the other 3—we get one row. If we used DENSE_RANK and there were two rows with the same second-highest value, we'd get two rows per series. Problem says "one row per series", so ROW_NUMBER is the right choice.
-- STEP 3: Output: series_id, observation_date, value. Order by series_id. Series with only one observation will have no row (no "second").

-- Write your query below:

WITH cte AS (
    SELECT
        ROW_NUMBER() OVER (PARTITION BY series_id ORDER BY value DESC) AS rn
        ,series_id
        ,observation_date
        ,value
    FROM silver.treasury_yields
)
    SELECT
        rn
        ,series_id
        ,value
        ,observation_date
    FROM cte
    WHERE rn =2

-- ========== Problem 10: Gaps in observation dates (DGS10) ==========
-- STEP 1: Restrict to DGS10. We need "consecutive" dates: for each row, the next observation_date. So ORDER BY observation_date and use LAG(observation_date) to get the previous row's date, or LEAD(observation_date) to get the next. For "gap between prev and next", we want: current row's date is "next", previous row's date is "prev". So LAG(observation_date) OVER (ORDER BY observation_date) AS prev_date, and observation_date AS next_date. Then gap_days = next_date - prev_date (or observation_date - prev_date).
-- STEP 2: Filter to gap > 1: WHERE observation_date - LAG(observation_date) OVER (...) > 1. But you can't use a window function in WHERE. So put the LAG in a CTE/subquery, then outer query WHERE next_date - prev_date > 1. (And prev_date IS NOT NULL so we don't include the first row.)
-- STEP 3: Output: prev_date, next_date, gap_days. Order by prev_date.

-- Write your query below:

WITH cte AS (
    SELECT
         observation_date
        ,LAG(observation_date) OVER (ORDER BY observation_date) AS prev_date
    FROM silver.treasury_yields
    WHERE series_id = 'DGS10'
)
    SELECT
         observation_date AS next_date
        ,prev_date
        ,observation_date - prev_date AS gap_days
    FROM cte
    WHERE observation_date - prev_date > 1
    AND prev_date IS NOT NULL
    ORDER BY prev_date