--dbt run --select stg_postcode_lookup

{{ config(materialized='view') }}

SELECT Postcode,
       Borough,
       Zone,
       Outcode
FROM {{ source('staging','london_postcode_lookup') }}