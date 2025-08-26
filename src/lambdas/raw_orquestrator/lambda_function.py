import json
import awswrangler as wr
import pandas as pd
import urllib.parse
import os
import boto3

# environment to run glue crawlers and glue job

glue = boto3.client("glue")
JSON_CRAWLER = os.environ.get("JSON_CRAWLER_NAME")  # set this in the Lambda env
CSV_CRAWLER = os.environ.get("CSV_CRAWLER_NAME")  # set this in the Lambda env
CSV_TO_PARQUET_JOB = os.environ.get("CSV_TO_PARQUET_IN_CLEAN_BUCKET_JOB")

# environments for variable to turn json to parquet

os_input_s3_cleaned_layer = os.environ["s3_cleaned_layer"]
os_input_glue_catalog_db_name = os.environ["glue_catalog_db_name"]
os_input_glue_catalog_table_name = os.environ["glue_catalog_table_name"]
os_input_write_data_operation = os.environ[
    "write_data_operation"
]  


def lambda_handler(event, context):
    bucket = event["Records"][0]["s3"]["bucket"]["name"]
    key = urllib.parse.unquote_plus(
        event["Records"][0]["s3"]["object"]["key"], encoding="utf-8"
    )

    # try and except block to handle errors

    try:
        # create a dataframe from the S3 object
        df_raw = wr.s3.read_json(f"s3://{bucket}/{key}")

        # extract the requered columns
        df_extraction = pd.json_normalize(df_raw["items"])

        # write to S3
        wr_response = wr.s3.to_parquet(
            df=df_extraction,
            path=os_input_s3_cleaned_layer,
            dataset=True,
            database=os_input_glue_catalog_db_name,
            table=os_input_glue_catalog_table_name,
            mode=os_input_write_data_operation,
        )

        # start glue crawlers and glue job

        glue.start_crawler(Name=JSON_CRAWLER)
        glue.start_crawler(Name=CSV_CRAWLER)
        job = glue.start_job_run(JobName=CSV_TO_PARQUET_JOB)

        return {
            "started": [JSON_CRAWLER, CSV_CRAWLER],
            "jobRunId": job["JobRunId"],
            "wr_response": wr_response,
        }

    except Exception as e:
        print(e)
        print(
            f"Error getting object {key} from bucket {bucket}. Make sure they exist and your bucket is in the same region as this function."
        )
        raise e
