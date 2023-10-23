{{
    config(
        unique_key='property_id'
    )
}}

with

source  as (
    select distinct property_type from {{ ref('property_snapshot') }}
),

transform as (
    select
        row_number() over() as property_id,
        property_type
    from source
),

unknown as (
    select
        0 as property_id,
        'unknown' as property_type
)

select * from unknown
union all
select * from transform