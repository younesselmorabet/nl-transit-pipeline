-- Staging model: cleans and renames raw fields, adds one calculated column.
-- This is the "silver" layer - trustworthy, but still one row per departure.

with source as (

    -- This line pulls from the raw source table declared in sources.yml
    select * from {{ source('raw_transit', 'departures_raw') }}

),

cleaned as (
    select
        source_file,
        direction as destination,        -- clearer name than the raw "direction"
        train_name,
        planned_datetime,
        actual_datetime,
        planned_track,
        actual_track,
        cancelled,
        category as train_category,      -- avoids confusion with generic "category"
        departure_status,

        -- Calculated field, not present in raw data: minutes late (negative = early)
        timestamp_diff(actual_datetime, planned_datetime, minute) as delay_minutes

    from source
)

select * from cleaned