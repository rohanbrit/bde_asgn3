{{
    config(
        unique_key='property_type'
    )
}}

with

transform  as (
    select
        property_type,
        dbt_valid_from,
        dbt_valid_to
    from
        {{ ref('property_snapshot') }}
),

unknown as (
    select
        'unknown' as property_type,
        '1900-01-01'::timestamp  as dbt_valid_from,
        null::timestamp as dbt_valid_to
)

select * from unknown
union all
select * from transform