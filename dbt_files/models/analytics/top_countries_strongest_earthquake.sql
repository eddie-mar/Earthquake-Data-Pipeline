{{
    config(
        materialized='view'
    )
}}
-- to see country with strongest earthquake recorded

select 
    country, 
    max(magnitude) as max_magnitude
from {{ ref('fact_earthquake_data') }}
group by country
order by max_magnitude desc