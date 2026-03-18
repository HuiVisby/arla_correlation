  {{
      config(materialized='table')
  }}

  with weather as (
      select period, year, month, avg_temp, total_precipitation, season
      from {{ ref('stg_weather__stockholm_monthly') }}
  ),

  cpi as (
      select period, food_cpi, food_cpi_yoy_pct
      from {{ ref('stg_scb__food_cpi') }}
  ),

  confidence as (
      select period, consumer_confidence
      from {{ ref('stg_eurostat__consumer_confidence') }}
  ),

  trends_monthly as (
      select
          year,
          month,
          keyword,
          keyword_category,
          round(avg(search_interest), 1) as avg_search_interest
      from {{ ref('stg_google_trends__weekly') }}
      group by year, month, keyword, keyword_category
  ),

  -- Pivot keywords into columns
  arla_trend as (
      select year, month, avg_search_interest as search_arla
      from trends_monthly where keyword = 'Arla'
  ),
  oatly_trend as (
      select year, month, avg_search_interest as search_oatly
      from trends_monthly where keyword = 'Oatly'
  ),
  milk_trend as (
      select year, month, avg_search_interest as search_mjolk
      from trends_monthly where keyword = 'mjölk'
  ),
  oat_trend as (
      select year, month, avg_search_interest as search_havredryck
      from trends_monthly where keyword = 'havredryck'
  ),
  youtube_trend as (
      select year, month, avg_search_interest as search_youtube_ads
      from trends_monthly where keyword = 'YouTube reklam'
  ),
  instagram_trend as (
      select year, month, avg_search_interest as search_instagram_ads
      from trends_monthly where keyword = 'Instagram reklam'
  ),
  facebook_trend as (
      select year, month, avg_search_interest as search_facebook_ads
      from trends_monthly where keyword = 'Facebook reklam'
  )

  select
      w.period,
      w.year,
      w.month,
      w.avg_temp,
      w.total_precipitation,
      w.season,
      c.food_cpi,
      c.food_cpi_yoy_pct,
      conf.consumer_confidence,
      a.search_arla,
      o.search_oatly,
      m.search_mjolk,
      oa.search_havredryck,
      yt.search_youtube_ads,
      ig.search_instagram_ads,
      fb.search_facebook_ads,
      -- Lagged Arla search interest (4 weeks ≈ 1 month)
      lag(a.search_arla, 1) over (order by w.period) as search_arla_lag1m,
      lag(a.search_arla, 2) over (order by w.period) as search_arla_lag2m,
      -- Competitor gap
      round(a.search_arla - o.search_oatly, 1)        as arla_vs_oatly_gap
  from weather w
  left join cpi           c    using (period)
  left join confidence    conf using (period)
  left join arla_trend    a    using (year, month)
  left join oatly_trend   o    using (year, month)
  left join milk_trend    m    using (year, month)
  left join oat_trend     oa   using (year, month)
  left join youtube_trend yt   using (year, month)
  left join instagram_trend ig using (year, month)
  left join facebook_trend  fb using (year, month)
  order by w.period
