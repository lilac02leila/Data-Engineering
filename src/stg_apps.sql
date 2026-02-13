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
    SELECT * FROM {{ source('raw', 'apps') }}
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
        CAST(price AS DECIMAL(10,2)) AS price,
        CAST(installs AS INTEGER) AS install_count,
        CAST(score AS DECIMAL(3,2)) AS app_rating,
        CAST(ratings AS INTEGER) AS rating_count
        
    FROM source
    
    -- Data quality filters
    WHERE appId IS NOT NULL
      AND title IS NOT NULL
)

SELECT * FROM cleaned
