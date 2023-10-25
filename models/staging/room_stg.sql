{{
    config(
        unique_key='room_id'
    )
}}

with

source  as (
    select distinct room_type from {{ ref('room_snapshot') }}
),

transform as (
    select
        row_number() over() as room_id,
        room_type,
        min(dbt_valid_from) as dbt_valid_from,
        null::timestamp as dbt_valid_to
    from
        {{ ref('room_snapshot') }}
    group by
        room_type
),

unknown as (
    select
        0 as room_id,
        'unknown' as room_type,
        '1900-01-01'::timestamp  as dbt_valid_from,
        null::timestamp as dbt_valid_to
)

select * from unknown
union all
select * from transform