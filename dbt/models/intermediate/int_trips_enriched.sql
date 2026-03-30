with trips as (

    select * from {{ ref('stg_yellow_trips') }}

),

pickup_zones as (

    select
        location_id,
        borough as pickup_borough,
        zone as pickup_zone,
        service_zone as pickup_service_zone
    from {{ ref('stg_taxi_zones') }}

),

dropoff_zones as (

    select
        location_id,
        borough as dropoff_borough,
        zone as dropoff_zone,
        service_zone as dropoff_service_zone
    from {{ ref('stg_taxi_zones') }}

),

joined as (

    select
        t.*,
        p.pickup_borough,
        p.pickup_zone,
        p.pickup_service_zone,
        d.dropoff_borough,
        d.dropoff_zone,
        d.dropoff_service_zone
    from trips t
    left join pickup_zones p
        on t.pickup_location_id = p.location_id
    left join dropoff_zones d
        on t.dropoff_location_id = d.location_id

),

filtered as (

    select *
    from joined
    where trip_distance > 0
      and fare_amount > 0
      and passenger_count > 0
      and trip_duration_minutes between 1 and 180

)

select * from filtered