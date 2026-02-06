# Data-Engineering - lab 1 
Group members:
AIT BIHI Laila
DAHHASSI Chaymae


## Feedback
- The code is more complex than it needs to be
- You are retrieving the continuation token but I don't see you using it.
- Think about writing with append in the loop; it is always better to prevent data loss if code crashes - noted
- Please add a screenshot of your dashboard to the readmefile - created a folder named dashboard containing the screenshots

## Answers to observation questions

How many changes for the new batch?
> Zero manual changes, the pipeline handles everything automatically.

Explicit or implicit full refresh?
> Explicit: the pipeline completely overwrites the data at each run.

Duplicate handling?
> Duplicates are removed (keep='first') and counted in the report.

Reviews for non-existent apps?
> Counted as orphan_reviews in the quality report, but kept in the dataset.

Hard-coded columns?
> The schema mapping handles drift, but internal names are fixed.

Explicit or silent failure?
> The pipeline continues with valid data and generates an explicit quality report.