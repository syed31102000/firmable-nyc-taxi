select
    cast(locationid as integer) as location_id,
    borough,
    zone,
    service_zone
from {{ ref('taxi_zone_lookup') }}