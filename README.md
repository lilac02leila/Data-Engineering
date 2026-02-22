# Data Engineering Labs - Complete Guide

**Date:** February 2026  
**Course:** 3A ECC - Data Engineering
**Contributers:** AIT BIHI Laila & DAHHASSI Chaymae

---

##  **Overview**

This repository contains two progressive data engineering labs that demonstrate the evolution from manual, script-based pipelines to modern, tool-based data platforms.

### **Lab 1: Python-Only Pipeline**
We've built a data pipeline using only Python to understand fundamental data engineering challenges and pain points.

### **Lab 2: Modern Data Stack**
We've rebuilt the same pipeline using professional tools (dbt + DuckDB) to solve the problems identified in Lab 1.

---

## **Lab 1: Python-Only Data Pipeline**

### **Objectives**
-  Extract data from Google Play Store API
-  Transform messy JSON/CSV data
-  Create analytics-ready outputs
-  Build dashboards with Plotly
-  Experience pipeline pain points

### **Quick Start**

```bash
# 1. Create virtual environment
conda create -n data_pipeline_lab python=3.10
conda activate data_pipeline_lab

# 2. Install dependencies
pip install google-play-scraper pandas plotly

# 3. Create project structure
mkdir -p lab1_project/{data/{raw,processed},src}
cd lab1_project

# 4. Run pipeline
python src/1_data_ingestion.py      # Extract data
python src/2_data_transformation.py  # Clean data
python src/3_serving_layer.py        # Create metrics
python src/4_dashboard.py            # Visualize
```

### **Lab 1 Architecture**

```
Google Play API
      ↓
Python Scripts (data extraction)
      ↓
JSON/JSONL Files (raw storage)
      ↓
Python Scripts (transformation)
      ↓
CSV Files (processed data)
      ↓
Python Scripts (aggregation)
      ↓
Plotly Dashboards (visualization)
```

### **Part A & B: Core Pipeline**

**Scripts:**
- `1_data_ingestion.py` - Extract apps and reviews from Google Play
- `2_data_transformation.py` - Clean and standardize data
- `3_serving_layer.py` - Create app-level and daily metrics
- `4_dashboard.py` - Generate interactive visualizations
- `run_pipeline.py` - Master orchestration script

**Outputs:**
```
data/
├── dashboard_lab1/
│   └── nweplots.png                # Visualizations
├── raw/
│   ├── apps_metadata.json          # 15 apps
│   └── apps_reviews.jsonl          # User reviews
└── processed/
    ├── apps_catalog.csv            # Clean app data
    ├── apps_reviews.csv            # Clean reviews
    ├── app_level_kpis.csv          # Per-app metrics
    └── daily_metrics.csv           # Time series
```

### **Part C: Stress Testing**

Test the pipeline against real-world scenarios:

1. **New Batch:** Handle new review data
2. **Schema Drift:** Column names change
3. **Dirty Data:** Invalid values
4. **Updated Apps:** Duplicate records
5. **New Logic:** Add sentiment analysis

**Scripts:**
- `scenario1_new_batch.py`
- `scenario2_schema_drift.py`
- `scenario3_dirty_data.py`
- `scenario4_updated_apps.py`
- `scenario5_sentiment_analysis.py`
- `run_all_scenarios.py`

### **Lab 1 Pain Points Discovered**

 **No Schema Validation** - Column names hard-coded everywhere  
 **Silent Failures** - Bad data corrupts analytics without warning  
 **Manual Orchestration** - Scripts must be run in order  
 **No Testing Framework** - Quality checks done manually  
 **Poor Performance** - Full table scans on every query  
 **No Data Lineage** - Can't track where data came from  
 **Scattered Logic** - Business rules in multiple files  

---

## **Lab 2: dbt + DuckDB Pipeline**

### **Objectives**
-  Apply Kimball dimensional modeling
-  Design star schema for analytics
-  Implement pipeline with dbt + DuckDB
-  Add automated data quality tests
-  Create incremental loading
-  Implement Slowly Changing Dimensions (SCD Type 2)

### **Quick Start**

```bash
# 1. Activate environment
conda activate data_pipeline_lab

# 2. Install dbt + DuckDB
pip install dbt-core dbt-duckdb

# 3. Initialize dbt project
cd lab1_project
dbt init playstore_analytics

# 4. Configure dbt
# Edit playstore_analytics/dbt_project.yml
# Edit ~/.dbt/profiles.yml

# 5. Create models
# Save all .sql files to models/ folder

# 6. Run pipeline
cd playstore_analytics
dbt run      # Build all models
dbt test     # Run data quality tests
dbt docs generate  # Create documentation
dbt docs serve     # View docs in browser
```

### **Lab 2 Architecture**

```
Google Play API
      ↓
Python Script (extraction only)
      ↓
DuckDB Raw Schema (ACID storage)
      ↓
dbt Staging Models (cleaning)
      ↓
dbt Dimensions (denormalized context)
      ↓
dbt Fact Table (star schema core)
      ↓
dbt Aggregates (pre-computed metrics)
      ↓
BI Tools (Tableau/PowerBI/Metabase)
```

### **Part C: Data Modeling (Kimball)**

**Business Process:** App Reviews  
**Grain:** One row per review  
**Dimensions:** app, date, user, developer, category  
**Facts:** review_score, thumbs_up_count, review_length  

**Star Schema:**
```
       dim_date
           |
           |
dim_developer --- dim_app --- fact_review --- dim_user
                     |
                     |
                dim_category
```

### **Part D: dbt Implementation**

**Model Layers:**

```
playstore_analytics/
models/
├── staging/                    # Clean raw data
│   ├── stg_apps.sql
│   ├── stg_reviews.sql
│   └── schema.yml              # Tests & documentation

├── marts/
│   ├── dimensions/             # Star schema dimensions
│   │   ├── dim_apps_scd.sql
│   │   ├── dim_app.sql
│   │   ├── dim_date.sql
│   │   ├── dim_user.sql
│   │   └── schema.yml          # Tests & documentation

│   ├── facts/                  # Star schema facts
│   │   ├── fact_reviwe_old.sql
│   │   ├── fact_review.sql
│   │   └── schema.yml          # Tests & documentation

```



### **Part E: Advanced Features**

**1. Incremental Loading**
```sql
{{ config(
    materialized='incremental',
    unique_key='review_id'
) }}

SELECT * FROM {{ ref('stg_reviews') }}
{% if is_incremental() %}
WHERE review_date > (SELECT MAX(review_date) FROM {{ this }})
{% endif %}
```

**2. Slowly Changing Dimensions (SCD Type 2)**
```sql
-- Track historical changes in dim_app
-- Preserves complete history of app changes
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
```

### **Lab 2 Improvements Over Lab 1**

 **ACID Guarantees** - DuckDB provides transactions  
 **Schema Validation** - Tests enforce data quality  
 **Automated Orchestration** - `dbt run` handles dependencies  
 **Comprehensive Testing** - 26+ automated tests  
 **Fast Queries** - Indexed star schema  
 **Complete Lineage** - Visual DAG in docs  
 **Modular Logic** - Each model is independent  
 **Version Control** - SQL models in Git  

---

## ** Project Structure**

```
lab1_project/
├── data/
│   ├── raw/                        # Lab 1 raw data
│   │   ├── apps_metadata.json
│   │   ├── apps_reviews.jsonl
│   │   ├── note_taking_ai_reviews_batch2.csv
│   │   ├── note_taking_ai_reviews_schema_drift.csv
│   │   ├── note_taking_ai_reviews_dirty.csv
│   │   └── note_taking_ai_apps_updated.csv
│   └── processed/                  # Lab 1 processed data
│       ├── apps_catalog.csv
│       ├── apps_reviews.csv
│       ├── app_level_kpis.csv
│       ├── daily_metrics.csv
│       └── dashboard_*.html
│
├── src/                            # Lab 1 scripts
│   ├── 1_data_ingestion.py
│   ├── 2_data_transformation.py
│   ├── 3_serving_layer.py
│   ├── 4_dashboard.py
│   ├── run_pipeline.py
│   ├── debug_data.py
│   ├── scenario1_new_batch.py
│   ├── scenario2_schema_drift.py
│   ├── scenario3_dirty_data.py
│   ├── scenario4_updated_apps.py
│   └── scenario5_sentiment_analysis.py
│
└── playstore_analytics/            # Lab 2 dbt project
    ├── dbt_project.yml
    ├── models/
    │   ├── staging/
    │   │   ├── stg_apps.sql
    │   │   └── stg_reviews.sql
    │   ├── marts/
    │   │   ├── dimensions/
    │   │   │   ├── dim_developer.sql
    │   │   │   ├── dim_category.sql
    │   │   │   ├── dim_app.sql
    │   │   │   ├── dim_date.sql
    │   │   │   └── dim_user.sql
    │   │   ├── facts/
    │   │   │   └── fact_review.sql
    │   │   └── aggregates/
    │   │       ├── app_summary.sql
    │   │       └── daily_metrics.sql
    │   └── schema.yml
    ├── snapshots/
    │   └── snap_apps.sql
    ├── tests/
    ├── macros/
    └── playstore_analytics.duckdb   # DuckDB database
```

---

## **Acknowledgments**

- Google Play Scraper library maintainers
- dbt Labs for dbt Core
- DuckDB Foundation for DuckDB
- The open-source data engineering community

---

##  **Quick Command Reference**

### **Lab 1**
```bash
# Full pipeline
python src/run_pipeline.py

# Individual steps
python src/1_data_ingestion.py
python src/2_data_transformation.py
python src/3_serving_layer.py
python src/4_dashboard.py

# Stress tests
python src/run_all_scenarios.py
```

### **Lab 2**
```bash
# Setup
dbt init playstore_analytics
dbt debug

# Run pipeline
dbt run                    # All models
dbt run --select staging.* # Staging only
dbt run --select marts.*   # Marts only

# Testing
dbt test                   # All tests
dbt test --select staging.* # Staging tests

# Documentation
dbt docs generate
dbt docs serve

# Specific operations
dbt run --select +dim_app  # dim_app and dependencies
dbt test --select fact_review # Test one model
dbt run --full-refresh     # Force rebuild
```
