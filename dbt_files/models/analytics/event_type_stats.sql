{{
    config(
        materialized='view'
    )
}}
-- event type count and strength

select
    type as event_type,
    count(*) as event_frequency,
    avg(magnitude) as avg_magnitude
from {{ ref('fact_earthquake_data') }}
group by type
order by event_frequency desc