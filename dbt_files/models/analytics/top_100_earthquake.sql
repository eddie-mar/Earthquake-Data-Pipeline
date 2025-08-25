{{
    config(
        materialized='view'
    )
}}
-- top 100 earthquake in history

-- (place, country, region, magnitude, depth, type)

select 
    event_datetime,
    place,
    country, 
    region, 
    magnitude,
    depth,
    alert,
    type
from {{ ref('fact_earthquake_data') }}
where magnitude > 4
order by magnitude desc
