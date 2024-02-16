--dbt run --select ave_price_to_nearest_stations

{{ config(
    materialized='incremental',
    partition_by={
      "field": "Date_Listed",
      "data_type": "DATE",
      "granularity": "day"
    }
)}}

WITH t AS (
    SELECT Nearest_Station,
        COUNT(*) as Cnt
    FROM {{ ref('stg_cleaned_london_properties')}}
    GROUP BY Nearest_Station
    HAVING Cnt >= 5
)
SELECT id,
    Date_Listed,
    Price,
    Price_per_Bedroom,
    Borough,
    c.Nearest_station
FROM {{ ref('stg_cleaned_london_properties')}} as c
JOIN {{ ref('stg_postcode_lookup') }} as l
ON c.Postcode = l.Postcode
INNER JOIN t
ON t.Nearest_Station = c.Nearest_Station

{% if is_incremental() %}

-- {{This}} refers to target table
-- So, we're adding rows where rows with the same id and Date_listed aren't already in the BigQuery table
where concat(id, ' ', Date_Listed) NOT IN (SELECT concat(id, ' ', Date_Listed) FROM {{this}})

{% endif %}