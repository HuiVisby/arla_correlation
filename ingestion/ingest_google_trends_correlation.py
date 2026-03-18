from pytrends.request import TrendReq
import pandas as pd
from google.cloud import bigquery
from datetime import datetime
import time

PROJECT_ID = "arla-media-correlation-2025"
DATASET = "raw_ingest"

# Brand + category keywords
KEYWORD_GROUPS = [
    {
        "keywords": ["Arla", "Oatly", "Alpro"],
        "category": "brand",
        "label": "brands"
    },
    {
        "keywords": ["mjölk", "havredryck", "ekologisk mjölk"],
        "category": "category",
        "label": "dairy_category"
    },
    {
        "keywords": ["YouTube reklam", "Instagram reklam", "Facebook reklam"],
        "category": "channel_awareness",
        "label": "ad_channels"
    }
]


def fetch_trends(group, timeframe="2019-01-01 2025-12-31"):
    pt = TrendReq(hl="sv-SE", tz=60)
    pt.build_payload(group["keywords"], geo="SE", timeframe=timeframe)
    df = pt.interest_over_time()
    if df.empty:
        return pd.DataFrame()
    df = df.drop(columns=["isPartial"], errors="ignore")
    df = df.reset_index().rename(columns={"date": "week_start"})
    df = df.melt(id_vars=["week_start"], var_name="keyword", value_name="search_interest")
    df["keyword_category"] = group["category"]
    df["year"] = df["week_start"].dt.year
    df["month"] = df["week_start"].dt.month
    df["ingested_at"] = datetime.utcnow().isoformat()
    return df


def load_to_bigquery(df, table_name):
    client = bigquery.Client(project=PROJECT_ID)
    table_id = f"{PROJECT_ID}.{DATASET}.{table_name}"
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_TRUNCATE",
        autodetect=True
    )
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"Loaded {len(df)} rows into {table_id}")


def main():
    all_dfs = []
    for group in KEYWORD_GROUPS:
        print(f"Fetching trends: {group['keywords']}")
        try:
            df = fetch_trends(group)
            if not df.empty:
                all_dfs.append(df)
                print(f"  {len(df)} rows")
        except Exception as e:
            print(f"Error: {e}")
        time.sleep(3)

    if all_dfs:
        df_all = pd.concat(all_dfs, ignore_index=True)
        load_to_bigquery(df_all, "google_trends_weekly")


if __name__ == "__main__":
    main()
