{{ config(
    materialized='incremental',
    unique_key='review_id'
) }}

WITH max_date AS (
    {% if is_incremental() %}
        SELECT MAX(review_timestamp) AS max_review_date FROM {{ this }}
    {% else %}
        SELECT CAST('1900-01-01' AS TIMESTAMP) AS max_review_date
    {% endif %}
),

reviews AS (
    SELECT r.*
    FROM {{ ref('stg_reviews') }} r
    CROSS JOIN max_date m
    {% if is_incremental() %}
        WHERE r.review_timestamp > m.max_review_date
    {% endif %}
),

apps AS (
    SELECT app_scd_key AS app_key, app_id, valid_from, valid_to
    FROM {{ ref('dim_apps_scd') }}
),

dates AS (
    SELECT date_key, date FROM {{ ref('dim_date') }}
),

users AS (
    SELECT user_key, user_name FROM {{ ref('dim_user') }}
),

final AS (
    SELECT
        r.review_id,
        a.app_key,
        d.date_key,
        u.user_key,
        r.review_timestamp,
        r.review_score,
        r.thumbs_up_count,
        r.review_length,
        r.review_content,
        r.is_low_rating,
        r.has_content
    FROM reviews r
    INNER JOIN apps a
        ON r.app_id = a.app_id
        AND r.review_timestamp >= a.valid_from
        AND (a.valid_to IS NULL OR r.review_timestamp < a.valid_to)
    INNER JOIN dates d ON r.review_date = d.date
    INNER JOIN users u ON r.user_name = u.user_name
)

SELECT * FROM final