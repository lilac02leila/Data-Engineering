/*
    Staging model for apps
    
    Purpose: Clean and standardize raw app data from Google Play Store
    Source: raw.apps table (loaded from Lab 1 CSV)
    
    Transformations:
    - Rename columns to standard naming convention
    - Cast data types appropriately
    - Handle nulls and missing values
    - Basic data quality filters
*/

WITH source AS (
    SELECT * FROM read_json_auto('data\raw\apps_metadata.json')
),

cleaned AS (
    SELECT
        -- Business keys
        appId AS app_id,
        
        -- Descriptive attributes
        title AS app_title,
        developer AS developer_name,
        genre AS app_genre,
        
        -- Numeric attributes
        TRY_CAST(price AS DECIMAL(10,2)) AS price,
        
        -- Strip '10,000+' → '10000' then cast
        TRY_CAST(
            REGEXP_REPLACE(CAST(installs AS VARCHAR), '[^0-9]', '', 'g')
        AS INTEGER) AS install_count,
        
        TRY_CAST(score AS DECIMAL(3,2)) AS app_rating,
        TRY_CAST(ratings AS INTEGER) AS rating_count
        
    FROM source
    
    -- Data quality filters
    WHERE appId IS NOT NULL
      AND title IS NOT NULL
)

SELECT * FROM cleaned
