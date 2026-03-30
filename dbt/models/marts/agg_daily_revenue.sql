select
    pickup_date as trip_date,
    count(*) as total_trips,
    sum(fare_amount) as total_fare,
    avg(fare_amount) as avg_fare,
    sum(tip_amount) as total_tips,
    case
        when sum(fare_amount) = 0 then 0
        else (sum(tip_amount) / sum(fare_amount)) * 100
    end as tip_rate_percent
from {{ ref('fct_trips') }}
group by pickup_date
order by pickup_date