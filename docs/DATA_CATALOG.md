# Glue Data Catalog

## Databases
- **youtube_api_raw**
  - `raw_statistics_reference_data` (crawler over Bronze JSON)
  - `raw_statistics` (crawler over Bronze CSV)
- **youtube_api_cleaned**
  - `cleaned_statistics_reference_data` (flattened JSON → Silver)
  - `raw_statistics` (CSV→Parquet → Silver)
- **dl_analytics** (optional mirror for Gold)
  - `final_analytics` (joined)

## Crawlers
- Raw JSON Crawler → `s3://.../youtube/raw_statistics_reference_data/`
- Raw CSV Crawler  → `s3://.../youtube/raw_statistics/`
- Cleaned Parquet Crawler → `s3://.../youtube/raw_statistics_reference_data/` and `/youtube/raw_statistics/` (Silver)

## Jobs
- `csv_to_parquet` → converts Bronze CSV to Silver Parquet
- `analytics_join` → inner join on `category_id` → writes Gold Parquet
