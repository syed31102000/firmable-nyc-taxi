with hourly as (
    select
        pickup_hour as hour_of_day,
        count(*) as total_trips,
        avg(fare_amount) as avg_fare,
        avg(
            case
                when fare_amount = 0 then null
                else (tip_amount / fare_amount) * 100
            end
        ) as avg_tip_percentage
    from fct_trips
    group by pickup_hour
),
final as (
    select
        hour_of_day,
        total_trips,
        avg_fare,
        avg_tip_percentage,
        avg(total_trips) over (
            order by hour_of_day
            rows between 2 preceding and current row
        ) as rolling_3hr_avg_trip_count
    from hourly
)
select *
from final
order by hour_of_day