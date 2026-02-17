-- Day 32: Query Optimization (EXPLAIN ANALYZE, Indexes)
-- Run from project root: docker cp sql/practice/query_optimization.sql credit-markets-postgres:/tmp/ && docker exec -it credit-markets-postgres psql -U postgres -d credit_markets -f /tmp/query_optimization.sql

-- ========== Problem 1: Baseline (no index) ==========
-- You already ran this; result was Seq Scan, 38 rows, ~0.375 ms.

EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM silver.treasury_yields WHERE series_id = 'DGS10';

-- ========== Problem 2: Add index on series_id, then compare ==========

CREATE INDEX IF NOT EXISTS idx_treasury_yields_series_id
ON silver.treasury_yields(series_id);

EXPLAIN (ANALYZE, BUFFERS)
SELECT * FROM silver.treasury_yields WHERE series_id = 'DGS10';

-- ========== Problem 3: Composite index for WHERE + ORDER BY ==========

CREATE INDEX IF NOT EXISTS idx_treasury_series_date
ON silver.treasury_yields(series_id, observation_date DESC);

EXPLAIN (ANALYZE)
SELECT * FROM silver.treasury_yields
WHERE series_id = 'DGS10'
ORDER BY observation_date DESC
LIMIT 10;

-- ========== Problem 4: When NOT to add an index ==========
-- Fill in 3 short answers (one line each):

-- 1. when table is tiny, full scan is cheaper than using an index
-- 2. column is written often but rarely used in where clause, index slows down inserts and updates
-- 3. column has very few distinct values, planner often prefers sequential scan anyways.
