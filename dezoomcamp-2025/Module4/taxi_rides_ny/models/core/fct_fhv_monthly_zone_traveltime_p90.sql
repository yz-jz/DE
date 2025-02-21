{{ config(materialized='table') }}

with fhv_trip_duration as (
    select *,
    timestamp_diff(dropoff_datetime, pickup_datetime, second) as trip_duration
    from {{ ref('dim_fhv_trips')  }}
) select *,
    percentile_cont(trip_duration, 0.9) over(partition by year, month, pickup_location_id, dropoff_location_id) as p90
    from fhv_trip_duration
