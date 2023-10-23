{% snapshot room_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='room_type',
          updated_at='scraped_date',
        )
    }}

select distinct room_type, to_timestamp(scraped_date, 'YYYY-MM-DD') as scraped_date from {{ source('raw', 'listings') }}

{% endsnapshot %}