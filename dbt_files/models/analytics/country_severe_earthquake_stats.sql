{{
    config(
        materialized='view'
    )
}}
-- country severity counts, only consider earthquake > mag 3

select
    country,
    severity as earthquake_severity,
    count(*) as frequency
from {{ ref('fact_earthquake_data') }}
where magnitude > 3
group by country, severity
order by 
    country, 
    case severity
        when 'Great' then 1
        when 'Major' then 2
        when 'Strong' then 3
        when 'Moderate' then 4
        when 'Light' then 5
        when 'Minor' then 6
        else 7
    end


