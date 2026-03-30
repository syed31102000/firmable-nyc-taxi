/*
I chose monthly revenue rank rather than a single rank across the full year.
This is more useful for the business because zone performance varies by season,
events, weather, and travel patterns. Monthly ranking shows relative performance
within each month and makes the metric more actionable.
*/

with base as (

    select
        pickup_year,
        pickup_month,
        pickup_zone,
        count(*) as total_trips,
        avg(trip_distance) as avg_trip_distance,
        avg(fare_amount) as avg_fare,
        sum(total_amount) as total_revenue
    from {{ ref('fct_trips') }}
    group by pickup_year, pickup_month, pickup_zone

),

ranked as (

    select
        *,
        rank() over (
            partition by pickup_year, pickup_month
            order by total_revenue desc
        ) as revenue_rank,
        case
            when total_trips > 10000 then true
            else false
        end as high_volume_zone_flag
    from base

)

select * from ranked