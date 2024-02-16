--dbt run --select ave_price_to_features

{{ config(
    materialized='incremental',
    partition_by={
      "field": "Date_Listed",
      "data_type": "DATE",
      "granularity": "day"
    }
)}}

With t AS(
    SELECT id,
        Postcode,
        Date_Listed,
        Price,
        Price_per_Bedroom,
        Tenure_Type,
        Estate_Agent,
        Bedrooms,
        Bathrooms,
        Lease_Length_Years,
        Size_Sqm,
        Service_Charge,
        FLOOR(Lease_Length_Years/20) * 20 AS Lease_Length_Rounded,
        FLOOR(Size_Sqm/20) * 20 AS Size_Sqm_Rounded,
        FLOOR(Service_Charge/1000) * 1000 AS Service_Charge_Rounded
    FROM {{ ref('stg_cleaned_london_properties')}}
    -- WHERE Lease_Length_Years IS NOT NULL
    )
SELECT id, 
    Borough,
    Date_Listed,
    Price,
    Price_per_Bedroom,
    Tenure_Type,
    Estate_Agent,
    Bedrooms,
    Bathrooms,
    Lease_Length_Years,
    Size_Sqm,
    Service_Charge,
    CASE WHEN Lease_Length_Rounded <= 240 THEN concat(cast((Lease_Length_Rounded + 1) as string), ' - ', cast((Lease_Length_Rounded + 20) as string))
    WHEN Lease_Length_Rounded > 240 THEN '240+'
    ELSE NULL END AS Lease_Length_Bands,
    CASE WHEN Size_Sqm_Rounded <= 300 THEN concat(cast((Size_Sqm_Rounded + 1) as string), ' - ', cast((Size_Sqm_Rounded + 20) as string))
    ELSE NULL END AS Size_Sqm_Bands,
    CASE WHEN Service_Charge = 0 THEN '0'
    WHEN Service_Charge_Rounded <= 12000 THEN concat(cast((Service_Charge_Rounded + 1) as string), ' - ', cast((Service_Charge_Rounded + 1000) as string))
    WHEN Service_Charge_Rounded > 12000 THEN '12000+'
    ELSE NULL END AS Service_Charge_Bands
FROM t
JOIN {{ ref('stg_postcode_lookup') }} as l
ON t.Postcode = l.Postcode


{% if is_incremental() %}

-- {{This}} refers to target table
-- So, we're adding rows where rows with the same id and Date_listed aren't already in the BigQuery table
where concat(id, ' ', Date_Listed) NOT IN (SELECT concat(id, ' ', Date_Listed) FROM {{this}})

{% endif %}