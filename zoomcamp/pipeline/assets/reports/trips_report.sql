/* @bruin

name: reports.trips_report
type: duckdb.sql

depends:
  - staging.trips

materialization:
  type: table
  strategy: time_interval
  incremental_key: pickup_date
  time_granularity: date

columns:
  - name: pickup_date
    type: date
    description: "Date of trip pickup"
    primary_key: true
    checks:
      - name: not_null
  - name: taxi_type
    type: string
    description: "Type of taxi (yellow or green)"
    primary_key: true
    checks:
      - name: not_null
  - name: payment_type_name
    type: string
    description: "Human-readable payment type name"
    primary_key: true
  - name: trip_count
    type: bigint
    description: "Total number of trips"
    checks:
      - name: positive
  - name: total_passengers
    type: float
    description: "Sum of passenger counts"
    checks:
      - name: non_negative
  - name: avg_trip_distance
    type: float
    description: "Average trip distance in miles"
    checks:
      - name: non_negative
  - name: total_fare
    type: float
    description: "Sum of fare amounts in USD"
    checks:
      - name: non_negative
  - name: total_tips
    type: float
    description: "Sum of tip amounts in USD"
    checks:
      - name: non_negative
  - name: total_revenue
    type: float
    description: "Sum of total amounts in USD"
    checks:
      - name: non_negative

@bruin */

-- Reports: Aggregate staging data by date, taxi type, and payment type
SELECT
    CAST(pickup_datetime AS DATE) AS pickup_date,
    taxi_type,
    payment_type_name,
    COUNT(*) AS trip_count,
    SUM(passenger_count) AS total_passengers,
    AVG(trip_distance) AS avg_trip_distance,
    SUM(fare_amount) AS total_fare,
    SUM(tip_amount) AS total_tips,
    SUM(total_amount) AS total_revenue
FROM staging.trips
WHERE pickup_datetime >= '{{ start_datetime }}'
  AND pickup_datetime < '{{ end_datetime }}'
GROUP BY
    CAST(pickup_datetime AS DATE),
    taxi_type,
    payment_type_name
