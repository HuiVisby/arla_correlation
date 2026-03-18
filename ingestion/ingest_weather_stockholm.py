import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

PROJECT_ID = "arla-media-correlation-2025"
DATASET = "raw_ingest"


def fetch_weather():
    """Fetch monthly weather for Stockholm from Open-Meteo archive."""
    print("Fetching Stockholm weather...")
    url = "https://archive-api.open-meteo.com/v1/archive"
    params = {
        "latitude": 59.3293,
        "longitude": 18.0686,
        "start_date": "2019-01-01",
        "end_date": "2025-12-31",
        "daily": ["temperature_2m_mean", "precipitation_sum"],
        "timezone": "Europe/Stockholm"
    }
    r = requests.get(url, params=params, timeout=60)
    data = r.json()
    df = pd.DataFrame(data["daily"])
    df["date"] = pd.to_datetime(df["time"])
    df["year"] = df["date"].dt.year
    df["month"] = df["date"].dt.month
    df["period"] = df["date"].dt.strftime("%Y-%m")

    # Aggregate to monthly
    monthly = df.groupby(["year", "month", "period"]).agg(
        avg_temp=("temperature_2m_mean", "mean"),
        total_precipitation=("precipitation_sum", "sum")
    ).reset_index()
    monthly["avg_temp"] = monthly["avg_temp"].round(1)
    monthly["total_precipitation"] = monthly["total_precipitation"].round(1)
    monthly["city"] = "Stockholm"
    monthly["ingested_at"] = datetime.utcnow().isoformat()
    print(f"Weather: {len(monthly)} months")
    return monthly


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
    df = fetch_weather()
    if not df.empty:
        load_to_bigquery(df, "weather_stockholm_monthly")


if __name__ == "__main__":
    main()
