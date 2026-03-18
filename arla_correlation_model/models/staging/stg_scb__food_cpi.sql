 with source as (
      select * from {{ source('raw_ingest', 'scb_food_cpi') }}
  ),

  cleaned as (
      select
          period,
          year,
          month,
          food_cpi,
          lag(food_cpi) over (order by period)                           as prev_food_cpi,
          round(100.0 * (food_cpi - lag(food_cpi) over (order by period))
                / nullif(lag(food_cpi) over (order by period), 0), 2)   as food_cpi_yoy_pct,
          source
      from source
      where year >= 2019
  )

  select * from cleaned
