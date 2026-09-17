-- Marts model: aggregates staging data into business-facing metrics,
-- one row per destination instead of one row per individual departure.

with staged as (

    -- This line pulls from the stg_departures MODEL, not a raw table.
    -- Referencing another dbt model this way is what lets dbt automatically
    -- know the correct run order, without us specifying it manually anywhere.
    select * from {{ ref('stg_departures') }}

)

select
    destination,
    count(*) as total_departures,
    countif(cancelled) as cancelled_count,
    round(countif(cancelled) / count(*) * 100, 1) as cancelled_pct,
    round(avg(delay_minutes), 1) as avg_delay_minutes,
    max(delay_minutes) as max_delay_minutes

from staged
group by destination
order by avg_delay_minutes desc