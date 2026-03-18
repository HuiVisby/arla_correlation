import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

PROJECT_ID = "arla-media-correlation-2025"
DATASET = "raw_ingest"


def fetch_food_cpi():
    """SCB KPI2020COICOP2M — Food CPI by COICOP division, 2020=100."""
    print("Fetching food CPI from SCB...")
    url = "https://api.scb.se/OV0104/v1/doris/en/ssd/PR/PR0101/PR0101A/KPI2020COICOP2M"
    payload = {
        "query": [
            {"code": "VaruTjanstegrupp", "selection": {"filter": "item", "values": ["01"]}},
            {"code": "ContentsCode", "selection": {"filter": "item", "values": ["0000080C"]}},
            {"code": "Tid", "selection": {"filter": "all", "values": ["*"]}}
        ],
        "response": {"format": "json-stat2"}
    }
    r = requests.post(url, json=payload, timeout=30)
    r.raise_for_status()
    data = r.json()
    periods = list(data["dimension"]["Tid"]["category"]["index"].keys())
    values = data["value"]
    rows = []
    for period, value in zip(periods, values):
        if value is not None:
            rows.append({
                "period": period.replace("M", "-"),
                "year": int(period[:4]),
                "month": int(period[5:7]),
                "food_cpi": float(value),
                "source": "SCB"
            })
    print(f"Food CPI: {len(rows)} rows")
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
    df = fetch_food_cpi()
    if not df.empty:
        load_to_bigquery(df, "scb_food_cpi")


if __name__ == "__main__":
    main()
