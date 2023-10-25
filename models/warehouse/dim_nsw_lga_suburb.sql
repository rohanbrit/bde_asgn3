{{
    config(
        unique_key='suburb_id'
    )
}}

with check_lga as
(
    select
        suburb_id,
        suburb_name,
        case when lga_name in (select distinct lga_name from {{ ref('nsw_lga_stg') }}) then lga_name else 'unknown' end as lga_name
    from {{ ref('nsw_lga_suburb_stg') }}
)

select * from check_lga
