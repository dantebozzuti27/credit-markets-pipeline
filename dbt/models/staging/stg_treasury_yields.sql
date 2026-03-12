-- Staging model: select from silver.treasury_yields.
-- Materialized as view by default. Use ref() in downstream models, not the raw table.
select
    observation_date,
    series_id,
    value
from {{ source('silver', 'treasury_yields') }}
