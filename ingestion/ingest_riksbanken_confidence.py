import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

PROJECT_ID = "arla-media-correlation-2025"
DATASET = "raw_ingest"


def fetch_consumer_confidence():
    """Fetch Swedish consumer confidence from OECD (covers Riksbanken data)."""
    print("Fetching consumer confidence...")
    url = "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_CLI,4.0/SWE.M.CSCICP03.IXOBTE...."
    headers = {"Accept": "application/json"}
    r = requests.get(url, headers=headers, timeout=30)
    data = r.json()
    series = data["data"]["dataSets"][0]["series"]
    time_periods = data["data"]["structures"][0]["dimensions"]["observation"][0]["values"]
    rows = []
    for series_key, series_data in series.items():
        for time_idx, obs in series_data["observations"].items():
            value = obs[0]
            if value is not None:
                period = time_periods[int(time_idx)]["id"]
                rows.append({
                    "period": period.replace("-", "-"),
                    "year": int(period[:4]),
                    "month": int(period[5:7]),
                    "consumer_confidence": float(value),
                    "source": "OECD"
                })
    print(f"Consumer confidence: {len(rows)} rows")
    return pd.DataFrame(rows)


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
    df = fetch_consumer_confidence()
    if not df.empty:
        load_to_bigquery(df, "consumer_confidence")


if __name__ == "__main__":
    main()
