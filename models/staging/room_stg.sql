{{
    config(
        unique_key='room_type'
    )
}}

with

transform as (
    select
        room_type,
        dbt_valid_from,
        dbt_valid_to
    from
        {{ ref('room_snapshot') }}
),

unknown as (
    select
        'unknown' as room_type,
        '1900-01-01'::timestamp as dbt_valid_from,
        null::timestamp as dbt_valid_to
)

select * from unknown
union all
select * from transform