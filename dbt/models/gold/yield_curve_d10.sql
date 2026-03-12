-- Gold model: depends on staging via ref(). Builds the DAG.
-- Example: 10-year yield only, for analytics.
select
    observation_date,
    series_id,
    value
from {{ ref('stg_treasury_yields') }}
where series_id = 'DGS10'
