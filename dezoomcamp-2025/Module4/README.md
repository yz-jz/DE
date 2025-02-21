# Module 4 HW

## Question5:
### Taxi quarterly revenue growth

``` sql
-- dim_taxi_trips
with taxi_trips as (
	select *,
	extract(year from pickup_datetime) as year,
	extract(month from pickup_datetime) as month,
	from {{ ref('fact_trips')  }}
	), dim_taxi_trips as (
	select *,
	 case 
			when month <= 3 then 1
			when month > 3 and month <= 6 then 2
			when month > 6 and month <= 9 then 3
			else 4
	end as quarter
	 from taxi_trips
)
select *,
	year || '/Q' || quarter as year_quarter
from dim_taxi_trips

-- fct_taxi_trips_quarterly_revenue
with quarterly_revenue as (

    select year,
    quarter,
    service_type,
    year_quarter, 
      sum(total_amount) as revenue
    from {{ ref('dim_taxi_trips')  }}
    where year >= 2019 and year <= 2020
    group by service_type, year, quarter, year_quarter

), lagged_revenue as (

    select *,
      lag(revenue) over(partition by service_type order by quarter ,year) as previous_quarter_revenue
      from quarterly_revenue
), yoy_growth as (
    select *,
      (revenue / previous_quarter_revenue) - 1 as growth
      from lagged_revenue

) select * from yoy_growth

```
## Question6:
### P97/P95/P90 Taxi Monthly Fare 

``` sql
-- fct_taxi_trips_monthly_fare_p95.sql
with filtered as (
	select service_type,
	month,
	year,
	fare_amount
	from {{  ref("dim_taxi_trips") }}
--	where fare_amount > 0 and
--	trip_distance > 0 and 
--	payment_type_description in ('Cash', 'Credit Card') and

	), percentile as (
	
	select *,
	percentile_cont(fare_amount, 0.97) over(partition by service_type, year, month) as p97,
	percentile_cont(fare_amount, 0.95) over(partition by service_type, year, month) as p95,
	percentile_cont(fare_amount, 0.90) over(partition by service_type, year, month) as p90

	from filtered

)
select * from percentile
```

### Query:

``` sql
select 
    service_type,
    p97,
    p95,
    p90,
    count(1)
from {{ ref('fct_taxi_trips_monthly_fare_p95') }}
group by service_type

```

## Question7:
### Top #Nth longest P90 travel time Location for FHV

``` sql
-- stg_fhv_trips.sql
select * from {{ source('staging','for_hire_vecicle')  }}
where dispatching_base_num is not null

-- dim_fhv_trips.sql
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

--fct_fhv_monthly_zone_traveltime_p90.sql
with fhv_trip_duration as (
    select *,
    timestamp_diff(dropoff_datetime, pickup_datetime, second) as trip_duration
    from {{ ref('dim_fhv_trips')  }}
) select *,
    percentile_cont(trip_duration, 0.9) over(partition by year, month, pickup_location_id, dropoff_location_id) as p90
    from fhv_trip_duration

```

### Query:

``` sql

with newark as (SELECT *, cast(p90 as int) as p90n FROM {{ ref('fct_fhv_monthly_zone_traveltime_p90')  }}
where pickup_zone ='Newark Airport' and month = 11 and year = 2019) 
,newark_r as
(select *, dense_rank() over(order by p90n DESC) as r from newark)

, soho as (SELECT *, cast(p90 as int) as p90n FROM {{ ref('fct_fhv_monthly_zone_traveltime_p90')  }}
where pickup_zone ='SoHo' and month = 11 and year = 2019) 
,soho_r as
(select *, dense_rank() over(order by p90n DESC) as r from soho)

, yorkville as (SELECT *, cast(p90 as int) as p90n FROM {{ ref('fct_fhv_monthly_zone_traveltime_p90')  }}
where pickup_zone ='Yorkville East' and month = 11 and year = 2019) 
,yorkville_r as
(select *, dense_rank() over(order by p90n DESC) as r from yorkville)



select * from newark_r where r = 2 limit 1
union all
select * from soho_r where r = 2 limit 1
union all
select * from yorkville_r where r = 2 limit 

```

Update the WHERE clause to pickup_datetime >= CURRENT_DATE - INTERVAL '{{ var("days_back", env_var("DAYS_BACK", "30")) }}' DAY

dbt run --select +models/core/

When using core, it materializes in the dataset defined in DBT_BIGQUERY_TARGET_DATASET
￼
When using stg, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET
￼
When using staging, it materializes in the dataset defined in DBT_BIGQUERY_STAGING_DATASET, or defaults to DBT_BIGQUERY_TARGET_DATASET

green: {best: 2020/Q1, worst: 2020/Q2}, yellow: {best: 2020/Q1, worst: 2020/Q2}

green: {p97: 55.0, p95: 45.0, p90: 26.5}, yellow: {p97: 31.5, p95: 25.5, p90: 19.0}

