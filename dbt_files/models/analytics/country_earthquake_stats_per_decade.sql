{{
    config(
        materialized='view'
    )
}}
-- to see frequency and strength of earthquakes in a country per decade

select 
    country, 
    region, 
    event_decade,
    count(*) as frequency, 
    avg(magnitude) as avg_magnitude, 
    avg(depth) as avg_depth
from {{ ref('fact_earthquake_data') }}
where magnitude > 3 and country is not null
group by country, region, event_decade
order by frequency desc
