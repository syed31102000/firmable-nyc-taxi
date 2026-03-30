with source as (

    select * from {{ source('raw', 'yellow_tripdata_raw') }}

),

renamed as (

    select
        vendorid as vendor_id,
        cast(tpep_pickup_datetime as timestamp) as pickup_datetime,
        cast(tpep_dropoff_datetime as timestamp) as dropoff_datetime,
        cast(passenger_count as integer) as passenger_count,
        cast(trip_distance as double) as trip_distance,
        cast(ratecodeid as integer) as rate_code_id,
        store_and_fwd_flag as store_and_fwd_flag,
        cast(pulocationid as integer) as pickup_location_id,
        cast(dolocationid as integer) as dropoff_location_id,
        cast(payment_type as integer) as payment_type,
        cast(fare_amount as double) as fare_amount,
        cast(extra as double) as extra,
        cast(mta_tax as double) as mta_tax,
        cast(tip_amount as double) as tip_amount,
        cast(tolls_amount as double) as tolls_amount,
        cast(improvement_surcharge as double) as improvement_surcharge,
        cast(total_amount as double) as total_amount,
        cast(congestion_surcharge as double) as congestion_surcharge,
        cast(airport_fee as double) as airport_fee,

        datediff('minute',
            cast(tpep_pickup_datetime as timestamp),
            cast(tpep_dropoff_datetime as timestamp)
        ) as trip_duration_minutes,

        cast(tpep_pickup_datetime as date) as pickup_date,
        cast(tpep_dropoff_datetime as date) as dropoff_date,
        extract(year from cast(tpep_pickup_datetime as timestamp)) as pickup_year,
        extract(month from cast(tpep_pickup_datetime as timestamp)) as pickup_month,
        extract(hour from cast(tpep_pickup_datetime as timestamp)) as pickup_hour

    from source

)

select * from renamed