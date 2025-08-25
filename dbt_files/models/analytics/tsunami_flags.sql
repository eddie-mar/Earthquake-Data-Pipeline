{{
    config(
        materialized='view'
    )
}}
-- tsunami risk per country. no. of earthquake flagged with tsunami / total events. only consider mag > 4
-- disclaimer: this does not really mean that tsunami occured. this are large events with link to actual tsunami sites. for further research

select
    country,
    region, 
    count(*) as total_events,
    sum(case when tsunami = 1 then 1 else 0 end) as tsunami_flags,
    (sum(case when tsunami = 1 then 1 else 0 end) / count(*)) * 100 as percent_tsunami_flagged
from {{ ref('fact_earthquake_data') }} 
where magnitude > 3 and country is not null
group by country, region
order by percent_tsunami_flagged desc

