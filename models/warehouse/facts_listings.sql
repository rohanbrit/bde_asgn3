{{
    config(
        unique_key='id'
    )
}}

with check_dimensions as
(
    select
        unique_id,
        listing_id,
        listing_date,
        case when host_id in (select distinct host_id from {{ ref('host_stg') }}) then host_id else 0 end as host_id,
        case when host_neighbourhood in (select distinct suburb_name from {{ ref('nsw_lga_suburb_stg') }}) then host_neighbourhood else 'unknown' end as host_neighbourhood,
        case when listing_neighbourhood in (select distinct lga_name from {{ ref('nsw_lga_suburb_stg') }}) then listing_neighbourhood else 'unknown' end as listing_neighbourhood,
        case when property_type in (select distinct property_type from {{ ref('property_stg') }}) then property_type else 'unknown' end as property_type,
        case when room_type in (select distinct room_type from {{ ref('room_stg') }}) then room_type else 'unknown' end as room_type,
        accommodates,
        price,
        has_availability,
        availability_30,
        number_of_reviews,
        review_scores_rating,
        review_scores_accuracy,
        review_scores_cleanliness,
        review_scores_checkin,
        review_scores_communication,
        review_scores_value
    from {{ ref('listings_stg') }}
)

select
    listing.unique_id as unique_id,
    listing.listing_id as listing_id,
    listing.listing_date as listing_date,
    host.host_id as host_id,
    host.host_name as host_name,
    host.host_since as host_since,
    host.host_is_superhost as host_is_superhost,
    host_suburb.suburb_name as host_neighbourhood,
    host_lga.lga_code as host_neighbourhood_lga_code,
    host_lga.lga_name as host_neighbourhood_lga_name,
    listing_lga.lga_name as listing_neighbourhood,
    listing_lga.lga_code as listing_neighbourhood_lga_code,
    property.property_type as property_type,
    room.room_type as room_type,
    listing.accommodates as accommodates,
    listing.price as price,
    listing.has_availability as has_availability,
    listing.availability_30 as availability_30,
    listing.number_of_reviews as number_of_reviews,
    listing.review_scores_rating as review_scores_rating,
    listing.review_scores_accuracy as review_scores_accuracy,
    listing.review_scores_cleanliness as review_scores_cleanliness,
    listing.review_scores_checkin as review_scores_checkin,
    listing.review_scores_communication as review_scores_communication,
    listing.review_scores_value as review_scores_value
from check_dimensions listing
left join {{ ref('host_stg') }} as host on listing.host_id = host.host_id and listing.listing_date::timestamp >= host.dbt_valid_from and listing.listing_date::timestamp < coalesce(host.dbt_valid_to, '9999-12-31'::timestamp)
left join {{ ref('nsw_lga_suburb_stg')}} as host_suburb on listing.host_neighbourhood = host_suburb.suburb_name
left join {{ ref('nsw_lga_stg')}} as host_lga on host_suburb.lga_name = host_lga.lga_name
left join {{ ref('nsw_lga_stg')}} as listing_lga on listing.listing_neighbourhood = listing_lga.lga_name
left join {{ ref('property_stg') }} as property on listing.property_type = property.property_type and listing.listing_date::timestamp >= property.dbt_valid_from and listing.listing_date::timestamp < coalesce(property.dbt_valid_to, '9999-12-31'::timestamp)
left join {{ ref('room_stg') }} as room on listing.room_type = room.room_type and listing.listing_date::timestamp >= room.dbt_valid_from and listing.listing_date::timestamp < coalesce(room.dbt_valid_to, '9999-12-31'::timestamp)
