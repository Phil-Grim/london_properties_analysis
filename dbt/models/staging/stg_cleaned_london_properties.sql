--dbt run --select stg_cleaned_london_properties_new

{{ config(materialized='view') }}

With p AS(
    SELECT id,
        Property_Link,
        Address,
        Outcode,
        concat(Outcode, ' ', Incode) as Postcode,
        Price,
        Case When Bedrooms > 0 THEN CAST(ROUND(Price / Bedrooms, 0) as int)
        Else CAST(ROUND(Price / 1, 0) as int) End as Price_per_Bedroom ,
        Listing_Type,
        CAST(Date as DATE) as Date_Listed,
        Property_Type,
        Size_Sqm,
        Bedrooms,
        Bathrooms,
        Ground_Rent,
        Service_Charge,
        Tenure_Type,
        Lease_Length_Years,
        Estate_Agent,
        Nearest_Station,
        Distance_From_Nearest_Station_Miles,
        Second_Nearest_Station,
        Distance_From_Second_Nearest_Station_Miles,
        Third_Nearest_Station,
        Distance_From_Third_Nearest_Station_Miles,
        Description
    FROM {{ source('staging','raw_london_properties') }}
)
SELECT p.*, l.Borough FROM p
JOIN {{ ref('stg_postcode_lookup') }} as l
ON l.Postcode = p.Postcode