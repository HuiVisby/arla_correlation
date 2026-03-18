 with source as (
      select * from {{ source('raw_ingest', 'google_trends_weekly') }}
  ),

  cleaned as (
      select
          cast(week_start as date)    as week_start,
          year,
          month,
          keyword,
          keyword_category,
          search_interest
      from source
      where search_interest is not null
  )

  select * from cleaned
