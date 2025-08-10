{{
    config(
        materialized='view'
    )
}}

select 
    {{ dbt_utils.generate_surrogate_key(['place', 'earthquake_datetime']) as event_id }}
    place, 
    cast(earthquake_datetime as timestamp) as earthquake_datetime,
    cast(magnitude as float64) as magnitude,
    cast(latitude as float64) as latitude,
    cast(longitude as float64) as longitude,
    cast(depth as float64) as depth,
    country,
    region,
    alert,
    tsunami,
    type
from {{ source('staging', 'stg_earthquake_data') }}

