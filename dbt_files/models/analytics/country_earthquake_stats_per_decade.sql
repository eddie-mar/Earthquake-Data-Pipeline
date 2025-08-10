{{
    config(
        materialized='view'
    )
}}


select 
    country, 
    region, 
    event_decade,
    count(*) as frequency, 
    avg(magnitude) as avg_magnitude, 
    avg(depth) as avg_depth
from {{ ref('fact_earthquake_data') }}
where magnitude > 3
group by country, region, event_decade
order by frequency desc
