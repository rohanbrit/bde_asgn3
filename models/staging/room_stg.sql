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
        room_type
    from source
),

unknown as (
    select
        0 as room_id,
        'unknown' as room_type
)

select * from unknown
union all
select * from transform