{{
  config(
  	alias='datamart_daily_taxi_metrics',
    datalake='lakehouse',
    root_path='warehouse.datamart',
    database='warehouse',
    schema='datamart',
    materialized='view',
  )
}}

WITH analytic_data AS (
    SELECT
    	DATE_TRUNC('DAY', tpep_pickup_datetime) AS pickup_date,
    	payment_type,
    	passenger_count,
    	trip_distance,
    	TIMESTAMPDIFF(SECOND,
        tpep_pickup_datetime,
        tpep_dropoff_datetime
      ) AS trip_duration_seconds,
      fare_amount,
      tip_amount,
      total_amount
    FROM {{ ref('analytic_yellowtripdata') }}
)

SELECT
    pickup_date,
    payment_type,
    COUNT(*) AS total_trips,
    SUM(passenger_count) AS total_passengers,
    ROUND(SUM(trip_distance), 2) AS total_distance_miles,
    ROUND(AVG(trip_distance), 2) AS avg_distance_miles,
    ROUND(AVG(trip_duration_seconds / 60), 2) AS avg_duration_minutes,
    ROUND(SUM(fare_amount), 2) AS total_fare_amount,
    ROUND(SUM(tip_amount), 2) AS total_tip_amount,
    ROUND(SUM(total_amount), 2) AS total_amount,
    ROUND(AVG(total_amount), 2) AS avg_total_amount,
    ROUND(SUM(tip_amount) / NULLIF(SUM(fare_amount), 0), 4) AS avg_tip_percentage,
    'lakehouse.warehouse.analytic.analytic_yellowtripdata' AS ingest_from,
    USER() AS ingest_by,
    CURRENT_TIMESTAMP() AS ingest_time

FROM analytic_data
GROUP BY pickup_date, payment_type
ORDER BY pickup_date, payment_type
