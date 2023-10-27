{% snapshot room_snapshot %}

{{
        config(
          target_schema='raw',
          strategy='timestamp',
          unique_key='room_type',
          updated_at='scraped_date',
        )
    }}

select distinct room_type, scraped_date::timestamp as scraped_date from {{ source('raw', 'listings') }}

{% endsnapshot %}