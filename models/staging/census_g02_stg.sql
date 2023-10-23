{{
    config(
        unique_key='lga_code'
    )
}}

with

source  as (
    select * from {{ source('raw', 'census_g02') }}
),

transform as (
    select
        -- remove text 'LGA' from lga code to get numeric value similar to nsw_lga tables
        CAST(REPLACE(lga_code, 'LGA', '') as int) as lga_code,
        median_age_persons,
        median_mortgage_repay_monthly,
        median_tot_prsnl_inc_weekly,
        median_rent_weekly,
        median_tot_fam_inc_weekly,
        average_num_psns_per_bedroom,
        median_tot_hhd_inc_weekly,
        average_household_size
    from source
),

unknown as (
    select
        0 as lga_code,
        0 as median_age_persons,
        0 as median_mortgage_repay_monthly,
        0 as median_tot_prsnl_inc_weekly,
        0 as median_rent_weekly,
        0 as median_tot_fam_inc_weekly,
        0 as average_num_psns_per_bedroom,
        0 as median_tot_hhd_inc_weekly,
        0 as average_household_size
)

select * from unknown
union all
select * from transform