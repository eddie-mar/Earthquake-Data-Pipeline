{{
    config(
        materialized='incremental',
        unique_key='event_id'
    )
}}

with earthquake_data as (
    {% if is_incremental() %}

        select * from
        {{ source('staging', 'stg_earthquake_data_monthly') }}
        where earthquake_datetime > (
            select max(earthquake_datetime) from {{ this }}
        )

    {% else %}

        select * from {{ source('staging', 'stg_earthquake_data_historical') }}
        union all
        select * from {{ source('staging', 'stg_earthquake_data_monthly') }}

    {% endif %}
),
duplicate_check as (
    select *,
        row_number() over (partition by place, earthquake_datetime order by earthquake_datetime) as rn
    from earthquake_data
)
select 
    {{ dbt_utils.generate_surrogate_key(['place', 'earthquake_datetime'])}} as event_id,
    place, 
    cast(earthquake_datetime as timestamp) as event_datetime,
    cast(magnitude as float64) as magnitude,
    cast(latitude as float64) as latitude,
    cast(longitude as float64) as longitude,
    cast(depth as float64) as depth,
    country,
    region,
    alert,
    tsunami,
    type
from duplicate_check
where rn = 1

