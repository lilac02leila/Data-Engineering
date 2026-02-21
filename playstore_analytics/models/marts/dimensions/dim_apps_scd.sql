{{ config(materialized='table') }}

WITH snapshot_data AS (
    SELECT * FROM {{ ref('snap_apps') }}
)

SELECT
    dbt_scd_id                          AS app_scd_key,
    app_id,
    app_title,
    developer_name,
    app_genre,
    price,
    install_count,
    app_rating,
    rating_count,
    dbt_valid_from                      AS valid_from,
    dbt_valid_to                        AS valid_to,
    CASE
        WHEN dbt_valid_to IS NULL THEN TRUE
        ELSE FALSE
    END                                 AS is_current

FROM snapshot_data