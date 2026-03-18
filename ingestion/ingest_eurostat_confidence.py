
import requests
import pandas as pd
from google.cloud import bigquery
from datetime import datetime

PROJECT_ID = "arla-media-correlation-2025"
DATASET = "raw_ingest"


def fetch_consumer_confidence():
    """Eurostat ei_bsco_m — Consumer confidence indicator, Sweden monthly."""
    print("Fetching consumer confidence...")
    url = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/ei_bsco_m"
    params = {
        "geo": "SE",
        "indic": "BS-CSMCI",
        "s_adj": "NSA",
        "format": "JSON",
        "lang": "en"
    }
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    data = r.json()
    time_index = data["dimension"]["time"]["category"]["index"]
    values = data["value"]
    rows = []
    for period, pos in time_index.items():
        value = values.get(str(pos))
        if value is not None:
            rows.append({
                "period": period.replace("M", "-"),
                "year": int(period[:4]),
                "month": int(period[5:7]),
                "consumer_confidence": float(value),
                "source": "Eurostat"
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
