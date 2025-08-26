# Buckets & Layout

## Bronze (raw)
`s3://personal-raw-useast2-...-youtubeapi`
- `youtube/raw_statistics_reference_data/{REGION}_category_id.json`
- `youtube/raw_statistics/region=XX/ts=YYYYMMDDHHMM/*.csv`

## Silver (cleaned Parquet)
`s3://personal-cleaned-useast2-...-youtubeapi`
- `youtube/raw_statistics_reference_data/*.parquet`   # flattened JSON
- `youtube/raw_statistics/region=XX/ts=.../*.parquet` # CSV→Parquet

## Gold (analytics Parquet)
`s3://personal-analytics-useast2-...-youtubeapi`
- `youtube/analytics/region=XX/ts=.../*.parquet`      # joined output
