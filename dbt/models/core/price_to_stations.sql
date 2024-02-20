--dbt run --select stations_new


{{ config(
    materialized='incremental',
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
    Borough,
    Distance_From_Nearest_Station_Miles,
    Distance_From_Second_Nearest_Station_Miles,
    Distance_From_Third_Nearest_Station_Miles,
    Case When Distance_From_Nearest_Station_Miles <= Distance_From_Second_Nearest_Station_Miles And Distance_From_Nearest_Station_Miles < Distance_From_Third_Nearest_Station_Miles Then Distance_From_Nearest_Station_Miles
        When Distance_From_Second_Nearest_Station_Miles < Distance_From_Nearest_Station_Miles And Distance_From_Second_Nearest_Station_Miles < Distance_From_Third_Nearest_Station_Miles Then Distance_From_Second_Nearest_Station_Miles 
        Else Distance_From_Third_Nearest_Station_Miles
        End As Distance_From_Nearest_Station,
    Case When Distance_From_Nearest_Station_Miles <= Distance_From_Second_Nearest_Station_Miles And Distance_From_Nearest_Station_Miles < Distance_From_Third_Nearest_Station_Miles Then Nearest_Station
        When Distance_From_Second_Nearest_Station_Miles < Distance_From_Nearest_Station_Miles And Distance_From_Second_Nearest_Station_Miles < Distance_From_Third_Nearest_Station_Miles Then Second_Nearest_Station 
        Else Third_Nearest_Station
        End As Nearest_station,
    ROW_NUMBER() OVER (PARTITION BY id ORDER BY Date_Listed DESC) AS rnk
-- FROM {{ ref('stg_cleaned_london_properties') }}
FROM {{ ref('stg_cleaned_london_properties') }}
),

stations_five_or_more AS (
    SELECT Nearest_Station as N_S,
        COUNT(*) as Cnt
    FROM list_date_rank
    WHERE rnk = 1
    GROUP BY Nearest_Station
    HAVING Cnt >= 5
)

SELECT id,
    -- p.Date_Listed,
    Date_Listed,
    Price,
    Price_per_Bedroom,
    -- p.Postcode,
    Postcode,
    Borough,
    Nearest_station,
    Distance_From_Nearest_Station,

    ROUND(Distance_From_Nearest_Station_Miles + Distance_From_Second_Nearest_Station_Miles + Distance_From_Third_Nearest_Station_Miles, 2) AS Total_Distance_From_Three_Nearest_Stations
FROM list_date_rank as p
-- JOIN {{ ref('stg_postcode_lookup') }} as l
-- ON l.Postcode = p.Postcode
INNER JOIN stations_five_or_more
ON stations_five_or_more.N_S = p.Nearest_station
WHERE Distance_From_Nearest_Station_Miles IS NOT NULL AND Distance_From_Second_Nearest_Station_Miles IS NOT NULL AND Distance_From_Third_Nearest_Station_Miles IS NOT NULL AND rnk = 1


{% if is_incremental() %}

-- {{This}} refers to target table
-- So, we're adding rows where rows with the same id and Date_listed aren't already in the BigQuery table
and concat(id, ' ', Date_Listed) NOT IN (SELECT concat(id, ' ', Date_Listed) FROM {{this}})

{% endif %}