/*
Snowflake performance ideas:
1. Cluster by pickup_date and pickup_location_id to improve partition pruning and local ordering.
2. Materialize a pre-filtered daily trip table containing only needed columns.
3. Use result cache for repeated analysis.
4. For repeated operational use, create an incremental daily zone-trip table and compute gaps only on new partitions.
5. Search optimization is less useful here than clustering because the query is window-heavy and ordered by time.
*/

with ordered as (
    select
        pickup_date,
        pickup_location_id,
        pickup_zone,
        dropoff_datetime,
        pickup_datetime,
        lag(dropoff_datetime) over (
            partition by pickup_date, pickup_location_id
            order by pickup_datetime
        ) as previous_dropoff_datetime
    from fct_trips
),
gaps as (
    select
        pickup_date,
        pickup_location_id,
        pickup_zone,
        datediff('minute', previous_dropoff_datetime, pickup_datetime) as gap_minutes
    from ordered
    where previous_dropoff_datetime is not null
)
select
    pickup_date,
    pickup_location_id,
    pickup_zone,
    max(gap_minutes) as max_gap_minutes
from gaps
group by pickup_date, pickup_location_id, pickup_zone
order by pickup_date, max_gap_minutes desc