{{
    config(
        unique_key='id',
        updated_at='updated_at'
    )
}}

with

source  as (

    select * from "postgres"."raw"."listings"

),

transform as (
    select
        id as unique_id,
        listing_id,
        -- Ignoring the scrape ID field as it is not required
        --scrape_id,
        to_date(scraped_date,'YYYY-MM-DD') as listing_date,
        host_id,
        host_name,
        to_date(host_since,'DD/MM/YYYY') as host_since,
        host_is_superhost,
        host_neighbourhood,
        listing_neighbourhood,
        property_type,
        room_type,
        accommodates,
        price,
        has_availability,
        availability_30,
        number_of_reviews,
        CASE review_scores_rating when 'NaN' then 0 else review_scores_rating end as review_scores_rating,
        CASE review_scores_accuracy when 'NaN' then 0 else review_scores_accuracy end as review_scores_accuracy,
        CASE review_scores_cleanliness when 'NaN' then 0 else review_scores_cleanliness end as review_scores_cleanliness,
        CASE review_scores_checkin when 'NaN' then 0 else review_scores_checkin end as review_scores_checkin,
        CASE review_scores_communication when 'NaN' then 0 else review_scores_communication end as review_scores_communication,
        CASE review_scores_value when 'NaN' then 0 else review_scores_value end as review_scores_value
    from source
)

select * from transform
