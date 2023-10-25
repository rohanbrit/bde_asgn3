with tot_list as
(
    select
        listing_neighbourhood,
        to_char(listing_date, 'YYYY/MM') as month_year,
        count(*)::numeric as tot_list_cnt,
        nullif(sum(case when has_availability = 'f' then 1 else 0 end), 0)::numeric as inact_list_cnt,
        nullif(lag(sum(case when has_availability = 'f' then 1 else 0 end)) over (partition by listing_neighbourhood order by to_char(listing_date, 'YYYY/MM')), 0)::numeric as prior_inact_list_cnt,
        count(distinct host_id)::numeric as dist_host_cnt
    from {{ ref('facts_listings') }}
    group by listing_neighbourhood,
    to_char(listing_date, 'YYYY/MM')
), act_list as
(
    select
        listing_neighbourhood,
        to_char(listing_date, 'YYYY/MM') as month_year,
        count(*)::numeric as act_list_cnt,
        lag(count(*)) over (partition by listing_neighbourhood order by to_char(listing_date, 'YYYY/MM'))::numeric as prior_act_list_cnt,
        min(price) as min_act_list_price,
        max(price) as max_act_list_price,
        PERCENTILE_CONT(0.5) within group (order by price) as med_act_list_price,
        round(avg(price), 2) as avg_act_list_price,
        round(avg(review_scores_rating), 2) as avg_rev_scores_rating,
        sum(30-availability_30) as tot_num_stays,
        round(avg((30-availability_30)*price), 2) as avg_est_revenue
    from {{ ref('facts_listings') }}
    where has_availability = 't'
    group by listing_neighbourhood,
    to_char(listing_date, 'YYYY/MM')
), superhost as 
(
    select
        listing_neighbourhood,
        to_char(listing_date, 'YYYY/MM') as month_year,
        count(distinct host_id)::numeric as dist_superhost_cnt
    from {{ ref('facts_listings') }}
    where host_is_superhost = 't'
    group by listing_neighbourhood,
    to_char(listing_date, 'YYYY/MM')
)
select
    tot_list.listing_neighbourhood,
    tot_list.month_year,
    round((act_list.act_list_cnt/tot_list.tot_list_cnt)*100, 2) as act_list_rate,
    act_list.min_act_list_price,
    act_list.max_act_list_price,
    act_list.med_act_list_price,
    act_list.avg_act_list_price,
    tot_list.dist_host_cnt,
    round(superhost.dist_superhost_cnt/tot_list.dist_host_cnt, 2) as superhost_rate,
    avg_rev_scores_rating,
    round(((act_list.act_list_cnt-act_list.prior_act_list_cnt)/act_list.prior_act_list_cnt)*100, 2) as prcnt_chng_act_list,
    round(((tot_list.inact_list_cnt-tot_list.prior_inact_list_cnt)/tot_list.prior_inact_list_cnt)*100, 2) as prcnt_chng_inact_list,
    act_list.tot_num_stays,
    act_list.avg_est_revenue
from
(
    tot_list
    left join act_list
    on tot_list.listing_neighbourhood = act_list.listing_neighbourhood
        and tot_list.month_year = act_list.month_year
    left join superhost
    on tot_list.listing_neighbourhood = superhost.listing_neighbourhood
        and tot_list.month_year = superhost.month_year
)
order by
    listing_neighbourhood,
    month_year