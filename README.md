# YouTube Trending – End-to-End Analytics on AWS (Raw → Cleaned → Analytics)

**Goal:** Ingest trending YouTube data per region, clean it into a data lake (Parquet), join against reference JSON, and visualize popularity (views/likes/comments) in QuickSight.

## Architecture

```mermaid
flowchart LR
  Kaggle[(Kaggle Trending CSV\nper region)]
  YTAPI[(YouTube API JSON\ncategories)]
  subgraph Raw S3
    R1[youtube/raw_statistics/\nregion=xx/*.csv]
    R2[youtube/raw_statistics_reference_data/*.json]
  end
  subgraph Cleaned S3
    C1[youtube/raw_statistics/\nParquet partitioned by region]
    C2[youtube/raw_statistics_reference_data/\nParquet]
  end
  subgraph Analytics S3
    A1[final_analytics/\nParquet partitioned by region,category_id]
  end

  Kaggle -->|aws s3 cp| R1
  YTAPI -->|aws s3 cp| R2

  R2 -->|S3 trigger| Lambda[Lambda: JSON → Parquet]
  Lambda --> C2

  R1 -->|Glue Job: CSV → Parquet\n(UTF-8, quote, escape, multiline, badRows)| C1

  C1 -->|Glue Job: Join| A1
  C2 -->|Glue Job: Join| A1

  A1 --> Athena[(Amazon Athena)]
  Athena --> QuickSight[(Amazon QuickSight)]
