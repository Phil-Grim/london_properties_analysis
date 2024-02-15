--dbt run --select stg_cleaned_london_properties

{{ config(materialized='table') }}

SELECT id,
       Property_Link,
       Address,
       Outcode,
       concat(Outcode, ' ', Incode) as Postcode,
       Price,
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