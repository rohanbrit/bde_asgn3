{{
    config(
        unique_key='property_id'
    )
}}

with

transform  as (
    select
        row_number() over() as property_id,
        property_type,
        min(dbt_valid_from) as dbt_valid_from,
        null::timestamp as dbt_valid_to
    from
        {{ ref('property_snapshot') }}
    group by
        property_type
),

unknown as (
    select
        0 as property_id,
        'unknown' as property_type,
        '1900-01-01'::timestamp  as dbt_valid_from,
        null::timestamp as dbt_valid_to
)

select * from unknown
union all
select * from transform