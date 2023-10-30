with main as
(
	select
		host_neighbourhood_lga_name,
		to_char(listing_date, 'YYYY/MM') as month_year,
		count(distinct host_id)::numeric as dist_host_cnt,
		sum(case when has_availability = 't' then ((30-availability_30)*price) else 0 end)::numeric as est_revenue
	from {{ ref('facts_listings') }}
	group by
		host_neighbourhood_lga_name,
		to_char(listing_date, 'YYYY/MM')
)

select
	*,
	round(est_revenue/dist_host_cnt, 2) as est_revenue_host
from main