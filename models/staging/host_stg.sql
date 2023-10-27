{{
    config(
        unique_key='host_id'
    )
}}

with

transform as (
    select
        host_id,
        host_name,
        -- Converting host_since to date format
        to_date(host_since, 'DD/MM/YYYY') as host_since,
        host_is_superhost,
        -- Replace 'NaN' host neighbourhoods to 'unknown' to match with the nsw_lga tables
        replace(host_neighbourhood, 'NaN', 'unknown') as host_neighbourhood,
        dbt_valid_from,
        dbt_valid_to
    from {{ ref('host_snapshot') }}
    where host_name <> 'NaN' 
        or host_since <> 'NaN'
        or host_is_superhost <> 'NaN'
        or host_neighbourhood <> 'NaN'
),

unknown as (
    select
        0 as host_id,
        'unknown' as host_name,
        to_date('01/01/1900', 'DD/MM/YYYY') as host_since,
        'unknown' as host_is_superhost,
        'unknown' as host_neighbourhood,
        '1900-01-01'::timestamp  as dbt_valid_from,
        null::timestamp as dbt_valid_to
)

select * from unknown
union all
select * from transform