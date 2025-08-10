{{
    config(
        materialized='table'
    )
}}

with data as (
    select *,
        case
            when magnitude >= 8 then 'Great'
            when magnitude >= 7 then 'Major'
            when magnitude >= 6 then 'Strong'
            when magnitude >= 5 then 'Moderate'
            when magnitude >= 4 then 'Light'
            else 'Minor'
        end as severity,
        extract(year from event_datetime) as event_year,
        {{ get_decade('event_datetime') }} as event_decade
    from {{ ref('stg_earthquake') }}
)

select * from data