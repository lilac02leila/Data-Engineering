# Kimball Dimensional Modeling Worksheet
## Google Play AI Note-Taking Apps Analysis

---

## Step 1: Identify the Business Process

**Question:** What recurring event or activity do we want to measure and analyze?

**Answer:** 
```
Business Process: APP REVIEWS
- Users submit reviews for AI note-taking apps on Google Play Store
- Each review contains a rating (1-5 stars), text feedback, and metadata
- This is a measurable, recurring event we want to analyze
```

**Why this matters:** The business process defines what we're measuring. Reviews are the core events that tell us about user satisfaction, app quality, and market trends.

---

## Step 2: Declare the Grain

**Question:** Complete this sentence: "One row in the fact table represents ..."

**Answer:**
```
One row in the fact table represents:
A SINGLE USER REVIEW for a specific app at a specific point in time
```

**Grain Definition:**
- **Primary grain:** One review
- **Unique identifier:** reviewId
- **Temporal grain:** Exact timestamp of review submission
- **Dimensionality:** Linked to specific app, user, and date

**Why grain matters:** Grain determines the level of detail. Too detailed = huge tables, not detailed enough = can't answer questions.

---

## Step 3: Identify the Dimensions

Dimensions answer: **Who? What? Where? When?** about the business process.

### Dimension 1: **dim_app** (What app was reviewed?)

**Business Meaning:** Represents the AI note-taking application being reviewed

**Source Dataset:** `apps_metadata.json` from Lab 1

**Attributes:**
- `app_key` (surrogate key - auto-generated)
- `app_id` (business key - from Google Play)
- `title` (app name)
- `developer` (who built it)
- `genre` (category)
- `price` (cost)
- `installs` (popularity indicator)
- `score` (overall app rating)
- `ratings` (number of ratings)

**Sample Questions Answered:**
- Which apps have the most reviews?
- How do free vs paid apps compare?
- Which developers dominate the market?

---

### Dimension 2: **dim_date** (When was the review submitted?)

**Business Meaning:** Represents the date/time context of the review

**Source Dataset:** Derived from `at` field in reviews

**Attributes:**
- `date_key` (surrogate key - YYYYMMDD format)
- `date` (actual date)
- `year`
- `quarter`
- `month`
- `month_name`
- `week_of_year`
- `day_of_month`
- `day_of_week`
- `day_name`
- `is_weekend`

**Sample Questions Answered:**
- Are ratings improving over time?
- Which days have the most review activity?
- Seasonal patterns in reviews?

---

### Dimension 3: **dim_user** (Who submitted the review?)

**Business Meaning:** Represents the reviewer (anonymized)

**Source Dataset:** `userName` field in reviews

**Attributes:**
- `user_key` (surrogate key)
- `user_name` (anonymized username)
- `user_hash` (for privacy)

**Sample Questions Answered:**
- Do specific users review multiple apps?
- Review patterns by user type?

**Note:** Limited information available, but important for complete dimensional model

---

## Step 4: Identify the Facts (Measures)

Facts are **quantitative, measurable, aggregatable** values.

### Fact Table: **fact_review**

**Facts (Measures):**

| Fact Name | Business Meaning | Data Type | Aggregation Functions | Source |
|-----------|------------------|-----------|----------------------|--------|
| `score` | Star rating (1-5) | Numeric | AVG, MIN, MAX, COUNT | reviews |
| `thumbs_up_count` | Helpful votes | Integer | SUM, AVG, MAX | reviews |
| `review_length` | Length of text | Integer | AVG, MIN, MAX | DERIVED: LEN(content) |
| `is_low_rating` | Rating ≤ 2 stars | Boolean | COUNT, SUM | DERIVED: score <= 2 |
| `has_content` | Has review text | Boolean | COUNT | DERIVED: content != '' |

**Foreign Keys (Degenerate Dimensions):**
- `app_key` → dim_app
- `date_key` → dim_date
- `user_key` → dim_user

**Degenerate Dimensions** (stored in fact):
- `review_id` (no separate dimension needed)
- `content` (review text - for analysis, not aggregation)

---

## Step 5: Create the Bus Matrix

The Bus Matrix shows which dimensions apply to which business processes.

| Business Process | dim_app | dim_date | dim_user |
|------------------|---------|----------|----------|
| **App Reviews** | ✓ | ✓ | ✓ |

**Interpretation:**
- Reviews can be analyzed by app (which app?)
- Reviews can be analyzed by date (when?)
- Reviews can be analyzed by user (who?)

**Future Business Processes** (for expansion):
| Business Process | dim_app | dim_date | dim_user |
|------------------|---------|----------|----------|
| App Downloads | ✓ | ✓ | ✗ |
| App Updates | ✓ | ✓ | ✗ |
| In-App Purchases | ✓ | ✓ | ✓ |

---

## Step 6: Design the Star Schema

### Physical Model:

```
            ┌─────────────────┐
            │   dim_date      │
            ├─────────────────┤
            │ date_key (PK)   │◄───┐
            │ date            │    │
            │ year            │    │
            │ month           │    │
            │ day_of_week     │    │
            └─────────────────┘    │
                                   │
┌─────────────────┐          ┌─────────────────────┐          ┌─────────────────┐
│   dim_app       │          │   fact_review       │          │   dim_user      │
├─────────────────┤          ├─────────────────────┤          ├─────────────────┤
│ app_key (PK)    │◄─────────┤ review_id (PK)      │─────────►│ user_key (PK)   │
│ app_id          │          │ app_key (FK)        │          │ user_name       │
│ title           │          │ date_key (FK)       │          │ user_hash       │
│ developer       │          │ user_key (FK)       │          └─────────────────┘
│ genre           │          │ score               │
│ price           │          │ thumbs_up_count     │
│ installs        │          │ review_length       │
│ score           │          │ content             │
│ ratings         │          │ is_low_rating       │
└─────────────────┘          │ has_content         │
                             └─────────────────────┘
```

### Table Specifications:

#### **dim_app** (Dimension Table)
```sql
CREATE TABLE dim_app (
    app_key INTEGER PRIMARY KEY,
    app_id VARCHAR UNIQUE NOT NULL,
    title VARCHAR,
    developer VARCHAR,
    genre VARCHAR,
    price DECIMAL(10,2),
    installs INTEGER,
    score DECIMAL(3,2),
    ratings INTEGER
);
```

#### **dim_date** (Dimension Table)
```sql
CREATE TABLE dim_date (
    date_key INTEGER PRIMARY KEY,
    date DATE UNIQUE NOT NULL,
    year INTEGER,
    quarter INTEGER,
    month INTEGER,
    month_name VARCHAR,
    week_of_year INTEGER,
    day_of_month INTEGER,
    day_of_week INTEGER,
    day_name VARCHAR,
    is_weekend BOOLEAN
);
```

#### **dim_user** (Dimension Table)
```sql
CREATE TABLE dim_user (
    user_key INTEGER PRIMARY KEY,
    user_name VARCHAR UNIQUE NOT NULL,
    user_hash VARCHAR
);
```

#### **fact_review** (Fact Table)
```sql
CREATE TABLE fact_review (
    review_id VARCHAR PRIMARY KEY,
    app_key INTEGER REFERENCES dim_app(app_key),
    date_key INTEGER REFERENCES dim_date(date_key),
    user_key INTEGER REFERENCES dim_user(user_key),
    score DECIMAL(2,1),
    thumbs_up_count INTEGER,
    review_length INTEGER,
    content TEXT,
    is_low_rating BOOLEAN,
    has_content BOOLEAN
);
```

---

## Step 7: Validate Against Analytical Needs

### Can we answer our business questions?

#### Question 1: "Which apps have the highest average ratings?"
```sql
SELECT 
    a.title,
    AVG(f.score) as avg_rating,
    COUNT(*) as review_count
FROM fact_review f
JOIN dim_app a ON f.app_key = a.app_key
GROUP BY a.title
ORDER BY avg_rating DESC;
```
✅ **Yes** - app dimension provides title, fact provides scores

---

#### Question 2: "Are ratings improving over time?"
```sql
SELECT 
    d.year,
    d.month,
    AVG(f.score) as avg_rating
FROM fact_review f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.year, d.month
ORDER BY d.year, d.month;
```
✅ **Yes** - date dimension provides time periods, fact provides scores

---

#### Question 3: "What percentage of reviews are negative (≤2 stars)?"
```sql
SELECT 
    a.title,
    SUM(CASE WHEN f.is_low_rating THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as pct_negative
FROM fact_review f
JOIN dim_app a ON f.app_key = a.app_key
GROUP BY a.title;
```
✅ **Yes** - is_low_rating flag enables easy calculation

---

#### Question 4: "Which developers have the most reviewed apps?"
```sql
SELECT 
    a.developer,
    COUNT(DISTINCT a.app_id) as num_apps,
    COUNT(f.review_id) as total_reviews
FROM fact_review f
JOIN dim_app a ON f.app_key = a.app_key
GROUP BY a.developer
ORDER BY total_reviews DESC;
```
✅ **Yes** - app dimension provides developer, fact provides review counts

---

#### Question 5: "Do reviews spike on weekends?"
```sql
SELECT 
    d.is_weekend,
    COUNT(*) as review_count,
    AVG(f.score) as avg_rating
FROM fact_review f
JOIN dim_date d ON f.date_key = d.date_key
GROUP BY d.is_weekend;
```
✅ **Yes** - date dimension provides is_weekend flag

---

## Grain Validation

**Declared Grain:** One row = One review

**Validation Checks:**
1. ✅ Primary key is `review_id` (unique per review)
2. ✅ Foreign keys link to dimensions (app, date, user)
3. ✅ All measures are at review level (score, thumbs_up_count)
4. ✅ No aggregation in fact table itself
5. ✅ Each review appears exactly once

**Join Validation:**
```sql
-- This should return same count as fact table
SELECT COUNT(*)
FROM fact_review f
JOIN dim_app a ON f.app_key = a.app_key
JOIN dim_date d ON f.date_key = d.date_key
JOIN dim_user u ON f.user_key = u.user_key;
```

---

## Summary

### Star Schema Design Decisions

| Decision | Rationale |
|----------|-----------|
| **Separate date dimension** | Enables time-based analysis without date functions |
| **App attributes in dim_app** | Denormalized for query simplicity |
| **review_id in fact** | Degenerate dimension - no need for separate table |
| **content in fact** | Needed for sentiment analysis, too large for dimension |
| **Derived measures** | Computed at load time for query performance |

### Benefits Over Lab 1 Flat Tables

| Lab 1 (Flat) | Lab 2 (Star Schema) |
|--------------|---------------------|
| Repeated app info in every row | App info stored once in dim_app |
| Date parsing in every query | Pre-computed date attributes |
| No primary/foreign keys | Enforced referential integrity |
| Hard to add new metrics | Add columns to fact table |
| Mixed analytical and transactional | Pure analytical structure |

---

## Next Steps

1. ✅ Business process identified: App Reviews
2. ✅ Grain declared: One review per row
3. ✅ Dimensions identified: app, date, user
4. ✅ Facts identified: score, thumbs_up_count, etc.
5. ✅ Bus Matrix created
6. ✅ Star schema designed
7. ✅ Validated against analytical needs

**Ready to implement in dbt!** 🚀
