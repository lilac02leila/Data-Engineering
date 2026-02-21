{% snapshot snap_apps %}

{{
    config(
        target_schema='main',
        unique_key='app_id',
        strategy='check',
        check_cols=['app_genre', 'developer_name', 'price', 'app_rating']
    )
}}

SELECT * FROM {{ ref('stg_apps') }}

{% endsnapshot %}