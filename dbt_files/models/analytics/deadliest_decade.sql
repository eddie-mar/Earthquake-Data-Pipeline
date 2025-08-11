{{
    config(
        materialized='view'
    )
}}
-- decade with highest magnitudes recorded

select
    event_decade,
    count(*) as earthquake_frequency,
    avg(magnitude) as avg_magnitude_recorded
from {{ ref('fact_earthquake_data') }}
where magnitude > 3
group by event_decade
order by event_decade

