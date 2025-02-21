{{
    config(
        materialized='table'
    )
}}

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
