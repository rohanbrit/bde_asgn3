{{
    config(
        unique_key='host_id'
    )
}}

with

source  as (
    select
        host_id,
        host_name,
        host_since,
        host_is_superhost,
        host_neighbourhood,
        -- Taking the minimum dbt_valid_from of each distinct entry in the host table
        min(dbt_valid_from) as dbt_valid_from,
        max(dbt_valid_to) as dbt_valid_to
    from {{ ref('host_snapshot') }}
    -- Selecting only those rows where host details are present
    where host_name <> 'NaN' 
        or host_since <> 'NaN'
        or host_is_superhost <> 'NaN'
        or host_neighbourhood <> 'NaN'
    -- Selecting distinct host entries from the table
    group by
        host_id,
        host_name,
        host_since,
        host_is_superhost,
        host_neighbourhood
),

transform as (
    select
        host_id,
        host_name,
        -- Converting host_since to date format
        to_date(host_since, 'DD/MM/YYYY') as host_since,
        host_is_superhost,
        -- Replace 'NaN' host neighbourhoods to 'unknown' to match with the nsw_lga tables
        replace(host_neighbourhood, 'NaN', 'unknown') as host_neighbourhood,
        -- Valid from date set to 01-01-1900 if it is the earliest entry for the host
        case when dbt_valid_from = (select min(dbt_valid_from) from source where host_id = ext_query.host_id) then '1900-01-01'::timestamp else dbt_valid_from end as dbt_valid_from,
        -- Every entry is set to be valid upto the next valid from date. For the last entry, the date is set to null
        lead(dbt_valid_from - interval '1 second', 1, null) over (partition by host_id order by dbt_valid_from) as dbt_valid_to
    from source as ext_query
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