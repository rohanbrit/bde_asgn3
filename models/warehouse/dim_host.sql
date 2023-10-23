{{
    config(
        unique_key='host_id'
    )
}}

with check_host_neighbourhood as
(
    select
        host_id,
        host_name,
        host_since,
        host_is_superhost,
        case when host_neighbourhood in (select distinct neighbourhood_name from {{ ref('nsw_lga_suburb_stg') }}) then host_neighbourhood else 'unknown' end as host_neighbourhood
    from {{ ref('host_stg') }}
)

select * from check_host_neighbourhood