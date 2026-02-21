/*
    Dimension: Date
    
    Purpose: Time dimension for temporal analysis
    Grain: One row per unique date
    
    Business Use: Enables time-based analysis of reviews
    - Trend analysis over time
    - Seasonal patterns
    - Day-of-week effects
    - Weekend vs weekday comparison
*/

{{ config(
    materialized='table',
    unique_key='date_key'
) }}

WITH review_dates AS (
    -- Get all unique dates from reviews
    SELECT DISTINCT review_date
    FROM {{ ref('stg_reviews') }}
),

date_spine AS (
    SELECT
        -- Surrogate key (YYYYMMDD format)
        CAST(STRFTIME(review_date, '%Y%m%d') AS INTEGER) AS date_key,
        
        -- Actual date
        review_date AS date,
        
        -- Year attributes
        EXTRACT(YEAR FROM review_date) AS year,
        
        -- Quarter attributes
        EXTRACT(QUARTER FROM review_date) AS quarter,
        CONCAT('Q', EXTRACT(QUARTER FROM review_date)) AS quarter_name,
        
        -- Month attributes
        EXTRACT(MONTH FROM review_date) AS month,
        STRFTIME(review_date, '%B') AS month_name,
        STRFTIME(review_date, '%b') AS month_name_short,
        
        -- Week attributes  
        EXTRACT(WEEK FROM review_date) AS week_of_year,
        
        -- Day attributes
        EXTRACT(DAY FROM review_date) AS day_of_month,
        EXTRACT(DAYOFWEEK FROM review_date) AS day_of_week,
        STRFTIME(review_date, '%A') AS day_name,
        STRFTIME(review_date, '%a') AS day_name_short,
        
        -- Boolean flags
        CASE 
            WHEN EXTRACT(DAYOFWEEK FROM review_date) IN (0, 6) THEN TRUE 
            ELSE FALSE 
        END AS is_weekend,
        
        CASE 
            WHEN EXTRACT(DAY FROM review_date) = 1 THEN TRUE 
            ELSE FALSE 
        END AS is_month_start,
        
        CASE 
            WHEN EXTRACT(DAY FROM review_date) = DAY(LAST_DAY(review_date)) THEN TRUE 
            ELSE FALSE 
        END AS is_month_end
        
    FROM review_dates
)

SELECT * FROM date_spine
ORDER BY date_key
