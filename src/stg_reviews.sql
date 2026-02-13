/*
    Staging model for reviews
    
    Purpose: Clean and standardize raw review data
    Source: raw.reviews table (loaded from Lab 1 CSV)
    
    Transformations:
    - Rename columns to standard naming convention
    - Cast data types appropriately
    - Extract date components
    - Calculate derived metrics
    - Filter invalid records
*/

WITH source AS (
    SELECT * FROM {{ source('raw', 'reviews') }}
),

cleaned AS (
    SELECT
        -- Business keys
        reviewId AS review_id,
        app_id,
        userName AS user_name,
        
        -- Temporal attributes
        CAST(at AS TIMESTAMP) AS review_timestamp,
        CAST(at AS DATE) AS review_date,
        
        -- Measures
        CAST(score AS DECIMAL(2,1)) AS review_score,
        CAST(thumbsUpCount AS INTEGER) AS thumbs_up_count,
        
        -- Text content
        content AS review_content,
        
        -- Derived metrics
        LENGTH(content) AS review_length,
        CASE WHEN score <= 2 THEN TRUE ELSE FALSE END AS is_low_rating,
        CASE WHEN LENGTH(TRIM(content)) > 0 THEN TRUE ELSE FALSE END AS has_content
        
    FROM source
    
    -- Data quality filters
    WHERE reviewId IS NOT NULL
      AND app_id IS NOT NULL
      AND at IS NOT NULL
      AND score IS NOT NULL
      AND score BETWEEN 1 AND 5  -- Valid rating range
)

SELECT * FROM cleaned
