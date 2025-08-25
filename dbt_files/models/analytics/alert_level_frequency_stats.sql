{{
    config(
        materialized='view'
    )
}}
-- recorded earthquakes with alert level ordered

select
    place, 
    country, 
    region,
    alert,
    magnitude
from {{ ref('fact_earthquake_data') }}
where alert in ('green', 'yellow', 'orange', 'red') and country is not null
order by
    case alert
        when 'red' then 1
        when 'orange' then 2
        when 'yellow' then 3
        when 'green' then 4
        else 5
    end,
    magnitude desc,
    country
    