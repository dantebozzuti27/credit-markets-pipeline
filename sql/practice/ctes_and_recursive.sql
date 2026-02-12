-- Day 31: CTEs + Recursive CTEs
-- Go deep: 3+ problems, when to use / when not, interview angle.

-- CTE = Common Table Expression. "WITH name AS (SELECT ...) SELECT ... FROM name"
-- Why use: readability, reuse the same subquery, sometimes optimizer treats them like inline views.

-- Recursive CTE = base case + recursive part. Stops when the recursive part returns no rows.
-- Syntax: WITH RECURSIVE name AS (
--   SELECT ... FROM base_table   -- anchor (base case)
--   UNION ALL
--   SELECT ... FROM name JOIN other  -- recursive part; must reference name
-- ) SELECT * FROM name;

-- Problem 1: Simple CTE (no recursion)
-- List each series_id with its average yield and how many observations it has.
-- Use one CTE that computes both AVG(value) and COUNT(*) per series, then SELECT from it.

-- Write your query below:

WITH cte AS (
    SELECT
         series_id
        ,AVG(value) as avg_value
        ,COUNT(*) as observation_count
    FROM
        silver.treasury_yields
    GROUP BY series_id
)
    SELECT
         series_id
        ,avg_value
        ,observation_count
    FROM
        cte


-- Problem 2: CTE for reuse
-- For DGS10 only: show observation_date, value, and the overall average yield (same value on every row).
-- Use a CTE to compute the overall AVG(value) once, then join it to the rows so you don't repeat the aggregate.

-- Write your query below:

WITH cte AS (
    SELECT
         AVG(value) AS overall_avg
    FROM
        silver.treasury_yields
    WHERE
        series_id = 'DGS10'
)
    SELECT
         observation_date
        ,value
        ,overall_avg
    FROM
        silver.treasury_yields
    WHERE
        series_id = 'DGS10'
    CROSS JOIN cte


-- Problem 3: Recursive CTE — row number without ROW_NUMBER()
-- For DGS10, number the rows 1, 2, 3, ... by observation_date order.
-- Recursive idea: base = first row (min date) with n=1; recursive = previous n + 1 using the "next" date.
-- Hint: In the recursive part, join to the table to get the "next" observation_date (e.g. the minimum date that is > anchor/current date). You may need two CTEs: one to get row ordering, then a recursive one that walks it.

-- (Alternative: recursive CTE that generates 1, 2, 3, ... N, then join to ordered DGS10 rows.)

-- Write your query below:

WITH recursive_cte_name AS(
    SELECT 1 AS n
    UNION ALL
    SELECT 
        n + 1 AS n
    FROM
        recursive_cte_name
    WHERE
        n < (
            SELECT 
                COUNT(*)
            FROM   
                silver.treasury_yields
            WHERE
                series_id = 'DGS10'
        )
)
,   second_cte AS(
        SELECT
             observation_date
            ,value
            ,ROW_NUMBER() OVER (ORDER BY observation_date) AS row_num
        FROM    
            silver.treasury_yields
        WHERE
            series_id = 'DGS10'
    )
SELECT
     observation_date
    ,value
    ,row_num
FROM
    second_cte
JOIN recursive_cte_name ON second_cte.row_num = recursive_cte_name.n


-- Problem 4: Recursive CTE — generate a date series
-- Generate all dates from 2024-01-01 through 2024-01-07 (7 rows) using a recursive CTE.
-- Base: SELECT '2024-01-01'::date AS dt
-- Recursive: SELECT dt + 1 FROM name WHERE dt < '2024-01-07'

-- Write your query below:

WITH
RECURSIVE name AS (
    SELECT
         '2024-01-01'::date AS dt
    UNION ALL
    SELECT
        dt + 1 as dt
    FROM
        name
    WHERE dt < '2024-01-07'         
)
    SELECT
        *
    FROM
        name
    ORDER BY dt