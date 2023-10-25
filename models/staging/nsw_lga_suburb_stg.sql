{{
    config(
        unique_key='suburb_id'
    )
}}

with

source  as (
    select * from {{ source('raw', 'nsw_lga_suburb') }}
),

suburb as (
    select
        row_number() over() as suburb_id,
        -- Capitalizing first letter of each word in suburb and lga name similar to the other tables
        initcap(suburb_name) as suburb_name,
        initcap(lga_name) as lga_name
    from source
),

unknown as (
    select
        0 as suburb_id,
        'unknown' as suburb_name,
        'unknown' as lga_name
)

select * from unknown
union all
select * from suburb