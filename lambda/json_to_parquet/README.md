# Lambda: JSON → Parquet (YouTube category reference)

Converts the raw **YouTube API category JSON** files in the **raw** S3 bucket into **Parquet** in the **cleaned** bucket, and updates the Glue Data Catalog.

- Function name (example): `personal-raw-useast2-884576820425-dev-lambda-json-parquet`
- Runtime: **Python 3.10**
- Architecture: **x86_64**

---

## Environment Variables

| Key                     | Value                                                                                                   |
|-------------------------|---------------------------------------------------------------------------------------------------------|
| `glue_catalog_db_name`  | `youtube_api_cleaned`                                                                                   |
| `glue_catalog_table_name` | `cleaned_statistics_reference_data`                                                                   |
| `s3_cleaned_layer`      | `s3://personal-cleaned-useast2-884576820425-dev-youtubeapi/youtube/raw_statistics_reference_data`       |
| `write_data_operation`  | `append`                                                                                                |

> These are read by `lambda_function.py` to know where to write Parquet and how to register the table/partitions.

---

## Layers

- **AWSSDKPandas-Python310** — **Version 25** — `python3.10` — **x86_64**

This provides `awswrangler` (AWS SDK for Pandas) for reading/writing to S3/Glue/Athena.

---

## Trigger (S3 → Lambda)

- **Bucket**: `arn:aws:s3:::personal-raw-useast2-884576820425-dev-youtubeapi`
- **Event types**: `s3:ObjectCreated:*`
- **Prefix**: `youtube/raw_statistics_reference_data/`
- **Suffix**: `.json`
- **Service principal**: `s3.amazonaws.com`
- **Source account**: `884576820425`
- **Notification / Config ID**: `a687680c-cc93-4185-87d9-05febfe8634c`
- **Statement ID**: `lambda-23e2fe11-af4d-4e2c-b7e7-102f89d1b32a`

> Any new/updated **JSON** under that prefix will invoke this Lambda.

---

## Local/Test Invocation

A ready-to-use test event is included as **`s3_put.json`**.
It simulates an S3 *ObjectCreated* event for a single JSON key.
