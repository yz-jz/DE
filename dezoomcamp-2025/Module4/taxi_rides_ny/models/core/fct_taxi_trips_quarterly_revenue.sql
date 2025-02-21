{{
    config(
        materialized='table'
    )
}}

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
