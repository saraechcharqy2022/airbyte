{{
  config(
    alias='staging_yellowtripdata',
    datalake='lakehouse',
    root_path='warehouse.staging',
    database='warehouse',
    schema='staging',
    materialized='incremental',
    incremental_strategy='merge',
    unique_key='trip_id',
    pre_hook='ALTER TABLE "lakehouse"."datalake"."raw"."raw_yellowtripdata" REFRESH METADATA AUTO PROMOTION'
  )
}}

SELECT
    *,
    {{
        dbt_utils.generate_surrogate_key(
            [
                'VendorID',
                'tpep_pickup_datetime',
                'tpep_dropoff_datetime',
                'PULocationID',
                'DOLocationID'
            ]
        )
    }} AS trip_id,
    'lakehouse.datalake.raw.raw_yellowtripdata' AS ingest_from,
    USER() AS ingest_by,
    CURRENT_TIMESTAMP() AS ingest_time
FROM "lakehouse"."datalake"."raw"."raw_yellowtripdata"
{% if is_incremental() %}
    WHERE tpep_pickup_datetime > (SELECT MAX(tpep_pickup_datetime) FROM {{ this }})
{% endif %}
