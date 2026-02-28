/* @bruin

name: staging.trips
type: duckdb.sql

depends:
  - ingestion.trips
  - ingestion.payment_lookup

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_datetime
  time_granularity: timestamp

columns:
  - name: pickup_datetime
    type: timestamp
    description: "When the trip started"
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_datetime
    type: timestamp
    description: "When the trip ended"
    primary_key: true
    checks:
      - name: not_null
  - name: pickup_location_id
    type: integer
    description: "TLC Taxi Zone pickup location ID"
    primary_key: true
    checks:
      - name: not_null
  - name: dropoff_location_id
    type: integer
    description: "TLC Taxi Zone dropoff location ID"
    primary_key: true
    checks:
      - name: not_null
  - name: fare_amount
    type: float
    description: "Base fare amount in USD"
    primary_key: true
    checks:
      - name: not_null
  - name: vendor_id
    type: integer
    description: "ID of the taxi vendor"
  - name: passenger_count
    type: float
    description: "Number of passengers"
  - name: trip_distance
    type: float
    description: "Trip distance in miles"
    checks:
      - name: non_negative
  - name: payment_type
    type: integer
    description: "Payment type code"
  - name: payment_type_name
    type: string
    description: "Human-readable payment type name"
  - name: tip_amount
    type: float
    description: "Tip amount in USD"
  - name: total_amount
    type: float
    description: "Total amount charged to passenger"
    checks:
      - name: non_negative
  - name: taxi_type
    type: string
    description: "Type of taxi (yellow or green)"
    checks:
      - name: not_null
      - name: accepted_values
        value: ["yellow", "green"]
  - name: extracted_at
    type: timestamp
    description: "Timestamp when data was extracted"

custom_checks:
  - name: no_duplicate_trips
    description: "Ensure no duplicate trips after deduplication"
    query: |
      SELECT COUNT(*) - COUNT(DISTINCT (pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, fare_amount))
      FROM staging.trips
      WHERE pickup_datetime >= '{{ start_datetime }}'
        AND pickup_datetime < '{{ end_datetime }}'
    value: 0

@bruin */

-- Staging: Clean, deduplicate, and enrich raw trip data
WITH deduplicated AS (
    SELECT
        *,
        ROW_NUMBER() OVER (
            PARTITION BY pickup_datetime, dropoff_datetime, pickup_location_id, dropoff_location_id, fare_amount
            ORDER BY extracted_at DESC
        ) AS row_num
    FROM ingestion.trips
    WHERE pickup_datetime >= '{{ start_datetime }}'
      AND pickup_datetime < '{{ end_datetime }}'
      AND pickup_datetime IS NOT NULL
      AND dropoff_datetime IS NOT NULL
      AND fare_amount >= 0
      AND total_amount >= 0
)

SELECT
    d.vendor_id,
    d.pickup_datetime,
    d.dropoff_datetime,
    d.passenger_count,
    d.trip_distance,
    d.pickup_location_id,
    d.dropoff_location_id,
    d.payment_type,
    p.payment_type_name,
    d.fare_amount,
    d.tip_amount,
    d.total_amount,
    d.taxi_type,
    d.extracted_at
FROM deduplicated d
LEFT JOIN ingestion.payment_lookup p
    ON d.payment_type = p.payment_type_id
WHERE d.row_num = 1
