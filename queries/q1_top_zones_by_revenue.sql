/*
Snowflake performance note:
For large-scale execution, clustering on pickup_date / pickup_month and pickup_zone
would improve pruning and reduce scan overhead. Pre-aggregating monthly zone revenue
before ranking is also important to reduce the number of rows participating in the
window function.
*/

with monthly_zone_revenue as (
    select
        pickup_year,
        pickup_month,
        pickup_zone,
        sum(total_amount) as revenue
    from fct_trips
    group by pickup_year, pickup_month, pickup_zone
),
ranked as (
    select
        *,
        rank() over (
            partition by pickup_year, pickup_month
            order by revenue desc
        ) as revenue_rank
    from monthly_zone_revenue
)
select *
from ranked
where revenue_rank <= 10
order by pickup_year, pickup_month, revenue_rank, pickup_zone