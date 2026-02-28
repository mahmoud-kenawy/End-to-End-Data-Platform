"""@bruin

name: ingestion.trips
type: python
image: python:3.11

connection: duckdb-default

columns:
  - name: vendor_id
    type: integer
    description: "ID of the taxi vendor"
  - name: pickup_datetime
    type: timestamp
    description: "When the trip started"
  - name: dropoff_datetime
    type: timestamp
    description: "When the trip ended"
  - name: passenger_count
    type: float
    description: "Number of passengers"
  - name: trip_distance
    type: float
    description: "Trip distance in miles"
  - name: pickup_location_id
    type: integer
    description: "TLC Taxi Zone pickup location ID"
  - name: dropoff_location_id
    type: integer
    description: "TLC Taxi Zone dropoff location ID"
  - name: payment_type
    type: integer
    description: "Payment type code"
  - name: fare_amount
    type: float
    description: "Base fare amount in USD"
  - name: tip_amount
    type: float
    description: "Tip amount in USD"
  - name: total_amount
    type: float
    description: "Total amount charged to passenger"
  - name: taxi_type
    type: string
    description: "Type of taxi (yellow or green)"
  - name: extracted_at
    type: timestamp
    description: "Timestamp when data was extracted"

@bruin"""

# Manual DuckDB write approach - avoids ingestr/dlt timezone issues on Windows.
# Bruin runs this script directly; we write to DuckDB ourselves.

import os
import json
import duckdb
import pandas as pd
from datetime import datetime
from dateutil.relativedelta import relativedelta


BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data/"

COLUMN_MAPPING = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "lpep_pickup_datetime": "pickup_datetime",
    "lpep_dropoff_datetime": "dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "tip_amount": "tip_amount",
    "total_amount": "total_amount",
}

KEEP_COLUMNS = [
    "vendor_id",
    "pickup_datetime",
    "dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "pickup_location_id",
    "dropoff_location_id",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "total_amount",
]


def generate_month_list(start_date: str, end_date: str):
    start = datetime.strptime(start_date, "%Y-%m-%d")
    end = datetime.strptime(end_date, "%Y-%m-%d")
    months = []
    current = start.replace(day=1)
    while current < end:
        months.append((current.year, current.month))
        current += relativedelta(months=1)
    return months


def fetch_taxi_data(taxi_type: str, year: int, month: int) -> pd.DataFrame:
    filename = f"{taxi_type}_tripdata_{year:04d}-{month:02d}.parquet"
    url = BASE_URL + filename
    print(f"Fetching {url}")
    try:
        df = pd.read_parquet(url)
        df = df.rename(columns=COLUMN_MAPPING)
        available = [c for c in KEEP_COLUMNS if c in df.columns]
        df = df[available]
        # Strip timezone info to keep timestamps naive (avoids DuckDB/Arrow issues)
        for col in df.select_dtypes(include=["datetimetz"]).columns:
            df[col] = df[col].dt.tz_convert("UTC").dt.tz_localize(None)
        df["taxi_type"] = taxi_type
        return df
    except Exception as e:
        print(f"Warning: Failed to fetch {url}: {e}")
        return pd.DataFrame()


start_date = os.environ.get("BRUIN_START_DATE", "2022-01-01")
end_date = os.environ.get("BRUIN_END_DATE", "2022-02-01")

bruin_vars = json.loads(os.environ.get("BRUIN_VARS", "{}"))
taxi_types = bruin_vars.get("taxi_types", ["yellow"])

print(f"Ingesting data from {start_date} to {end_date} for taxi types: {taxi_types}")

months = generate_month_list(start_date, end_date)
all_dfs = []

for taxi_type in taxi_types:
    for year, month in months:
        df = fetch_taxi_data(taxi_type, year, month)
        if not df.empty:
            all_dfs.append(df)

if not all_dfs:
    print("No data fetched for the given date range and taxi types.")
else:
    result = pd.concat(all_dfs, ignore_index=True)
    result["extracted_at"] = datetime.utcnow()

    print(f"Total rows fetched: {len(result)}")

    # Write directly to DuckDB (append strategy)
    db_path = os.environ.get("BRUIN_DUCKDB_PATH", "duckdb.db")
    print(f"Writing to DuckDB at: {db_path}")

    con = duckdb.connect(db_path)
    con.execute("CREATE SCHEMA IF NOT EXISTS ingestion")
    con.execute("""
        CREATE TABLE IF NOT EXISTS ingestion.trips (
            vendor_id INTEGER,
            pickup_datetime TIMESTAMP,
            dropoff_datetime TIMESTAMP,
            passenger_count DOUBLE,
            trip_distance DOUBLE,
            pickup_location_id INTEGER,
            dropoff_location_id INTEGER,
            payment_type INTEGER,
            fare_amount DOUBLE,
            tip_amount DOUBLE,
            total_amount DOUBLE,
            taxi_type VARCHAR,
            extracted_at TIMESTAMP
        )
    """)
    con.execute("INSERT INTO ingestion.trips SELECT * FROM result")
    row_count = con.execute("SELECT COUNT(*) FROM ingestion.trips").fetchone()[0]
    con.close()

    print(f"Successfully inserted {len(result)} rows. Total rows in ingestion.trips: {row_count}")



