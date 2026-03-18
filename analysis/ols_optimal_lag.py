import pandas as pd
from google.cloud import bigquery
from statsmodels.regression.linear_model import OLS
from statsmodels.tools import add_constant
import warnings
warnings.filterwarnings('ignore')

client = bigquery.Client(project='arla-media-correlation-2025')
df = client.query('SELECT * FROM `arla-media-correlation-2025.dbt_marts.mart_correlation_input` ORDER BY period').to_dataframe()

for v in ['search_arla','search_youtube_ads','search_facebook_ads','search_instagram_ads','consumer_confidence','food_cpi']:
    df[f'{v}_diff'] = df[v].diff()
df = df.dropna()

df['yt_lag3']   = df['search_youtube_ads_diff'].shift(3)
df['fb_lag4']   = df['search_facebook_ads_diff'].shift(4)
df['ig_lag1']   = df['search_instagram_ads_diff'].shift(1)
df['conf_lag1'] = df['consumer_confidence_diff'].shift(1)
df['cpi_lag1']  = df['food_cpi_diff'].shift(1)
df['temp_lag1'] = df['avg_temp'].shift(1)
df['is_summer'] = df['month'].isin([6,7,8]).astype(int)

preds = ['yt_lag3','fb_lag4','ig_lag1','conf_lag1','cpi_lag1','temp_lag1','is_summer']
df = df.dropna(subset=['search_arla_diff'] + preds)
X = add_constant(df[preds])
y = df['search_arla_diff']
m = OLS(y, X).fit(cov_type='HC3')
print(m.summary())
