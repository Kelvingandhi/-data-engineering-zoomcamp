{{ config(materialized='table') }}

with green_data as (
    select *,
        'Green' as service_type 
    from
        {{ ref('stg_green_taxi_tripdata') }}
),

yellow_data as (
    select *,
        'Yellow' as service_type 
    from
        {{ ref('stg_yellow_taxi_tripdata') }}
),

combined_data as (
    select * from green_data
    union all
    select * from yellow_data
),

dim_zones as (
    select *
    from
        {{ ref('taxi_zone_lookup') }}
    where borough != 'Unknown'
)

select 
    combined_data.tripid, 
    combined_data.vendorid, 
    combined_data.service_type,
    combined_data.ratecodeid, 
    combined_data.pickup_locationid, 
    pickup_zone.borough as pickup_borough, 
    pickup_zone.zone as pickup_zone, 
    combined_data.dropoff_locationid,
    dropoff_zone.borough as dropoff_borough, 
    dropoff_zone.zone as dropoff_zone,  
    combined_data.pickup_datetime, 
    combined_data.dropoff_datetime, 
    combined_data.store_and_fwd_flag, 
    combined_data.passenger_count, 
    combined_data.trip_distance, 
    combined_data.trip_type, 
    combined_data.fare_amount, 
    combined_data.extra, 
    combined_data.mta_tax, 
    combined_data.tip_amount, 
    combined_data.tolls_amount, 
    combined_data.ehail_fee, 
    combined_data.improvement_surcharge, 
    combined_data.total_amount, 
    combined_data.payment_type, 
    combined_data.payment_type_desc, 
    combined_data.congestion_surcharge
from combined_data
inner join dim_zones as pickup_zone
    on combined_data.pickup_locationid = pickup_zone.locationid
inner join dim_zones as dropoff_zone
    on combined_data.dropoff_locationid = dropoff_zone.locationid
