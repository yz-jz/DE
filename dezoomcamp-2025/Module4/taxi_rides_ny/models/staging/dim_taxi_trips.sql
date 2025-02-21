{{
    config(
        materialized='table'
    )
}}
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

