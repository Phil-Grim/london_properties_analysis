--dbt run --select stations

{{ config(
    materialized='table',
    partition_by={
      "field": "Date_Listed",
      "data_type": "DATE",
      "granularity": "day"
    }
)}}


With list_date_rank as (
    SELECT id,
    Date_Listed,
    Price,
    Price_per_Bedroom,
    Postcode,
    Distance_From_Nearest_Station_Miles,
    Distance_From_Second_Nearest_Station_Miles,
    Distance_From_Third_Nearest_Station_Miles,
    Nearest_Station,
    Second_Nearest_Station,
    Third_Nearest_Station,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY Date_Listed DESC) AS rnk
FROM {{ ref('stg_cleaned_london_properties') }}
)

SELECT id,
    p.Date_Listed,
    Price,
    Price_per_Bedroom,
    p.Postcode,
    Borough,
    Case When Distance_From_Nearest_Station_Miles <= Distance_From_Second_Nearest_Station_Miles And Distance_From_Nearest_Station_Miles < Distance_From_Third_Nearest_Station_Miles Then Distance_From_Nearest_Station_Miles
        When Distance_From_Second_Nearest_Station_Miles < Distance_From_Nearest_Station_Miles And Distance_From_Second_Nearest_Station_Miles < Distance_From_Third_Nearest_Station_Miles Then Distance_From_Second_Nearest_Station_Miles 
        Else Distance_From_Third_Nearest_Station_Miles
        End As Distance_From_Nearest_Station,
    Case When Distance_From_Nearest_Station_Miles <= Distance_From_Second_Nearest_Station_Miles And Distance_From_Nearest_Station_Miles < Distance_From_Third_Nearest_Station_Miles Then Nearest_Station
        When Distance_From_Second_Nearest_Station_Miles < Distance_From_Nearest_Station_Miles And Distance_From_Second_Nearest_Station_Miles < Distance_From_Third_Nearest_Station_Miles Then Second_Nearest_Station 
        Else Third_Nearest_Station
        End As Nearest_Station,
    ROUND(Distance_From_Nearest_Station_Miles + Distance_From_Second_Nearest_Station_Miles + Distance_From_Third_Nearest_Station_Miles, 2) AS Total_Distance_From_Three_Nearest_Stations,
FROM list_date_rank as p
JOIN {{ ref('stg_postcode_lookup') }} as l
ON l.Postcode = p.Postcode
WHERE Distance_From_Nearest_Station_Miles IS NOT NULL AND Distance_From_Second_Nearest_Station_Miles IS NOT NULL
AND Distance_From_Third_Nearest_Station_Miles IS NOT NULL AND rnk = 1

{% if is_incremental() %}

-- {{This}} refers to target table
-- So, we're adding rows where rows with the same id and Date_listed aren't already in the BigQuery table
where concat(id, ' ', Date_Listed) NOT IN (SELECT concat(id, ' ', Date_Listed) FROM {{this}})

{% endif %}