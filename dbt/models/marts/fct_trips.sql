with base as (

    select * from {{ ref('int_trips_enriched') }}

),

deduplicated as (

    select *
    from (
        select
            *,
            row_number() over (
                partition by
                    vendor_id,
                    pickup_datetime,
                    dropoff_datetime,
                    passenger_count,
                    trip_distance,
                    rate_code_id,
                    store_and_fwd_flag,
                    pickup_location_id,
                    dropoff_location_id,
                    payment_type,
                    fare_amount,
                    extra,
                    mta_tax,
                    tip_amount,
                    tolls_amount,
                    improvement_surcharge,
                    total_amount,
                    congestion_surcharge,
                    airport_fee
                order by pickup_datetime
            ) as dedupe_rn
        from base
    )
    where dedupe_rn = 1

),

final as (

    select
        row_number() over (
            order by
                pickup_datetime,
                dropoff_datetime,
                pickup_location_id,
                dropoff_location_id,
                total_amount
        ) as trip_id,
        * exclude(dedupe_rn)
    from deduplicated

)

select * from final