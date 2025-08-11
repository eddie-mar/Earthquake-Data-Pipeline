{{
    config(
        materialized='view'
    )
}}
-- top 100 earthquake in history

-- (place, country, region, magnitude, depth, type)

select 
    place,
    country, 
    region, 
    magnitude,
    depth,
    alert,
    type
from {{ ref('fact_earthquake_data') }}
order by magnitude desc
