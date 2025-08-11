{{
    config(
        materialized='view'
    )
}}
-- to see yearly increase of count and magnitude and depth

select 
    event_year as year, 
    count(*) as earthquake_count,
    avg(magnitude) as avg_magnitude, 
    avg(depth) as avg_depth
from {{ ref('fact_earthquake_data') }}
where magnitude > 3
group by event_year
order by event_year
