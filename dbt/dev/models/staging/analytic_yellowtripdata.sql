{{
  config(
    alias='analytic_yellowtripdata',
    datalake='lakehouse',
    root_path='warehouse.analytic',
    database='warehouse',
    schema='analytic',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='trip_id'
  )
}}

SELECT
    trip_id,
    Airport_fee AS airport_fee,
    congestion_surcharge,
    DOLocationID AS do_location_id,
    GREATEST(0, extra) AS extra,
    GREATEST(0, fare_amount) AS fare_amount,
    GREATEST(0, improvement_surcharge) AS improvement_surcharge,
    GREATEST(0, mta_tax) AS mta_tax,
    NULLIF(passenger_count, 0) AS passenger_count,
    CASE payment_type
        WHEN '1' THEN 'Credit card'
        WHEN '2' THEN 'Cash'
        WHEN '3' THEN 'No charge'
        WHEN '4' THEN 'Dispute'
        WHEN '5' THEN 'Unknown'
        WHEN '6' THEN 'Voided trip'
        ELSE 'Unknown'
    END AS payment_type,
    PULocationID AS pu_location_id,
    CAST(RatecodeID AS INT) AS rate_code_id,
    CASE
        WHEN store_and_fwd_flag IN ('Y', 'N') THEN store_and_fwd_flag
        ELSE NULL
    END AS store_and_fwd_flag,
    GREATEST(0, tip_amount) AS tip_amount,
    GREATEST(0, tolls_amount) AS tolls_amount,
    GREATEST(0, total_amount) AS total_amount,
    CAST(tpep_pickup_datetime AS TIMESTAMP) AS tpep_pickup_datetime,
    CAST(tpep_dropoff_datetime AS TIMESTAMP) AS tpep_dropoff_datetime,
    GREATEST(0, trip_distance) AS trip_distance,
    VendorID AS vendor_id,
    'lakehouse.warehouse.staging.staging_yellowtripdata' AS ingest_from,
    USER() AS ingest_by,
    CURRENT_TIMESTAMP() AS ingest_time

FROM {{ ref('staging_yellowtripdata') }}
WHERE
    tpep_pickup_datetime IS NOT NULL
    {% if is_incremental() %}
        AND tpep_pickup_datetime > (SELECT MAX(tpep_pickup_datetime) FROM {{ this }})
    {% endif %}
    AND tpep_dropoff_datetime IS NOT NULL
    AND tpep_pickup_datetime < tpep_dropoff_datetime
    AND passenger_count BETWEEN 1 AND 9
