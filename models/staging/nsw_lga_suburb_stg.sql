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
        -- Capitalizing first letter of each word in suburb and lga name similar to the other tables
        initcap(suburb_name) as neighbourhood_name,
        initcap(lga_name) as lga_name
    from source
),

-- I noticed that many of the listings have listing neighbourhood or host neighbourhood as an lga instead of suburb
-- Adding all LGAs to neighbourhood so that they can be mapped
lga as (
    select
        initcap(lga_name) as neighbourhood_name,
        initcap(lga_name) as lga_name
    from source
),

neighbourhood as (
    select
        row_number() over() as neighbourhood_id,
        neighbourhood.*
    from (
        select * from suburb
        union
        select * from lga
    ) neighbourhood
),

unknown as (
    select
        0 as neighbourhood_id,
        'unknown' as neighbourhood_name,
        'unknown' as lga_name
)

select * from unknown
union all
select * from neighbourhood