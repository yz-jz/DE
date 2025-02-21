{{ config(materialized='table') }}

with dim_zones as (
select * from {{ ref('dim_zones')  }} where borough != 'Unknown'
)
, joined as (
    select  
    fhv.dispatching_base_num,
    fhv.pickup_datetime,
    fhv.dropOff_datetime as dropoff_datetime,
    fhv.PUlocationID,
    fhv.DOlocationID,
    fhv.SR_Flag,
    fhv.Affiliated_base_number,
    fhv.PUlocationID as pickup_location_id,
    fhv.DOlocationID as dropoff_location_id,
    puzones.zone as pickup_zone,
    dozones.zone as dropoff_zone

    from {{ ref('stg_fhv_trips')  }} fhv
    join dim_zones puzones
    on fhv.PUlocationID = puzones.locationid 
    join dim_zones dozones

    on fhv.DOlocationID = dozones.locationid
) select *,

    extract(year from pickup_datetime) as year,
    extract(month from pickup_datetime) as month

from joined
