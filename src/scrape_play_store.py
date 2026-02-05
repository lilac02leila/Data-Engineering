import pandas as pd
import json
from google_play_scraper import app, reviews, Sort

# File paths
CSV_PATH = "AppMarketResearch/data/source/note_taking_ai_apps_updated.csv"
APPS_OUTPUT_PATH = "data/raw/apps_metadata.json"
REVIEWS_OUTPUT_PATH = "data/raw/reviews.json"
REVIEWS_JSONL_PATH = "data/raw/reviews.jsonl"

# Load source CSV (as-is)
df = pd.read_csv(CSV_PATH)

# Extract appIds (technical necessity)
app_ids = (
    df["appId"]
    .dropna()
    .unique()
    .tolist()
)

# Scrape apps metadata
apps_metadata = []

for app_id in app_ids:
    try:
        result = app(
            app_id,
            lang="en",
            country="us"
        )
        apps_metadata.append(result)
        print(f"Metadata fetched for {app_id}")
    except Exception as e:
        print(f"Metadata failed for {app_id}: {e}")

# Save apps metadata (RAW)
with open(APPS_OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(apps_metadata, f, ensure_ascii=False, indent=2)

# Scrape reviews
all_reviews = []

for app_id in app_ids:
    try:
        result, _ = reviews(
            app_id,
            lang="en",
            country="us",
            sort=Sort.NEWEST,
            count=500
        )

        for r in result:
            r["appId"] = app_id
            all_reviews.append(r)

        print(f"{len(result)} reviews fetched for {app_id}")

    except Exception as e:
        print(f"Reviews failed for {app_id}: {e}")

# Save reviews (JSON)
with open(REVIEWS_OUTPUT_PATH, "w", encoding="utf-8") as f:
    json.dump(all_reviews, f, ensure_ascii=False, indent=2)

# Save reviews (JSONL)
with open(REVIEWS_JSONL_PATH, "w", encoding="utf-8") as f:
    for review in all_reviews:
        f.write(json.dumps(review, ensure_ascii=False) + "\n")

print("Scraping completed successfully.")
