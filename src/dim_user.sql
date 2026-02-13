/*
    Dimension: User
    
    Purpose: User dimension for reviewer analysis
    Grain: One row per unique user
    
    Business Use: Enables user-based analysis
    - Power users (multiple reviews)
    - User engagement patterns
    - Privacy-preserving user analytics
    
    Note: Limited attributes due to privacy/data availability
*/

{{ config(
    materialized='table',
    unique_key='user_key'
) }}

WITH users AS (
    SELECT DISTINCT user_name
    FROM {{ ref('stg_reviews') }}
),

final AS (
    SELECT
        -- Surrogate key
        ROW_NUMBER() OVER (ORDER BY user_name) AS user_key,
        
        -- Business key
        user_name,
        
        -- Privacy-preserving hash
        MD5(user_name) AS user_hash,
        
        -- User type classification (based on name pattern)
        CASE
            WHEN user_name = 'Anonymous' THEN 'Anonymous'
            WHEN user_name LIKE 'A Google User%' THEN 'Default Google'
            ELSE 'Named User'
        END AS user_type
        
    FROM users
)

SELECT * FROM final
