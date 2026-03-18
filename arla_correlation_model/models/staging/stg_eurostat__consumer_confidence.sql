  with source as (
      select * from {{ source('raw_ingest', 'consumer_confidence') }}
  ),

  cleaned as (
      select
          period,
          year,
          month,
          consumer_confidence,
          source
      from source
      where year >= 2019
  )

  select * from cleaned
