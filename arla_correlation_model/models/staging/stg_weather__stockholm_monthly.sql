 with source as (
      select * from {{ source('raw_ingest', 'weather_stockholm_monthly') }}
  ),

  cleaned as (
      select
          period,
          year,
          month,
          avg_temp,
          total_precipitation,
          case
              when month in (12, 1, 2) then 'Winter'
              when month in (3, 4, 5)  then 'Spring'
              when month in (6, 7, 8)  then 'Summer'
              else 'Autumn'
          end as season
      from source
      where year >= 2019
  )

  select * from cleaned
