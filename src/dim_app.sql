/*
    Dimension: Applications
    
    Purpose: Slowly Changing Dimension (Type 1) for app attributes
    Grain: One row per unique app
    
    Business Use: Provides context for analyzing reviews by app characteristics
    - Filter by app name, developer, genre
    - Group by price tier or popularity
    - Analyze free vs paid apps
*/

{{ config(
    materialized='table',
    unique_key='app_key'
) }}

WITH apps AS (
    SELECT * FROM {{ ref('stg_apps') }}
),

final AS (
    SELECT
        -- Surrogate key (auto-generated sequence)
        ROW_NUMBER() OVER (ORDER BY app_id) AS app_key,
        
        -- Business key (natural key from source)
        app_id,
        
        -- Descriptive attributes
        app_title,
        developer_name,
        app_genre,
        
        -- Numeric attributes
        price,
        install_count,
        app_rating,
        rating_count,
        
        -- Derived attributes
        CASE 
            WHEN price = 0 THEN 'Free'
            WHEN price <= 5 THEN 'Budget'
            WHEN price <= 15 THEN 'Standard'
            ELSE 'Premium'
        END AS price_tier,
        
        CASE
            WHEN install_count < 1000 THEN 'New'
            WHEN install_count < 50000 THEN 'Growing'
            WHEN install_count < 500000 THEN 'Popular'
            ELSE 'Top'
        END AS popularity_tier,
        
        CASE
            WHEN app_rating >= 4.5 THEN 'Excellent'
            WHEN app_rating >= 4.0 THEN 'Good'
            WHEN app_rating >= 3.5 THEN 'Average'
            WHEN app_rating >= 3.0 THEN 'Below Average'
            ELSE 'Poor'
        END AS quality_tier
        
    FROM apps
)

SELECT * FROM final
