import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsgluedq.transforms import EvaluateDataQuality
from awsglue.dynamicframe import DynamicFrame
from pyspark.sql import functions as F

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Default ruleset used by all target nodes with data quality enabled
DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# --- paths ---

RAW_BASE = (
    "s3://personal-raw-useast2-884576820425-dev-youtubeapi/youtube/raw_statistics/"
)
RAW_GLOB = RAW_BASE + "region=*/"  # this reads all regions
BAD_ROWS_PATH = (
    "s3://personal-cleaned-useast2-884576820425-dev-youtubeapi/youtube/tmp/badrows/csv/"
)
CLEANED_OUT = (
    "s3://personal-cleaned-useast2-884576820425-dev-youtubeapi/youtube/raw_statistics/"
)

# --- Read all Regions from s3 with more specific CSV options ---

df = (
    spark.read.option("header", "true")
    .option("multiLine", "true")  # to ensure that descriptions may have new lines
    .option("quote", '"')
    .option("escape", "\\")
    .option("encoding", "UTF-8")  # handle foreign characters
    .option("mode", "PERMISSIVE")  # to tolerate bad lines
    .option("badRecordsPath", BAD_ROWS_PATH)  # separated the bad rows
    .option("basePath", RAW_BASE)  # to enable spark to partition by region
    .csv(RAW_GLOB)
)

# --- Strip UTF-8 BOM on first column name if present ---

first_col = df.columns[0]
if first_col.startswith("\ufeff"):
    df = df.withColumnRenamed(first_col, first_col.lstrip("\ufeff"))


# --- casting changes to make sure csv works ---
num_cols = ["category_id", "views", "likes", "dislikes", "comment_count"]
bool_cols = ["comments_disabled", "ratings_disabled", "video_error_or_removed"]


# turn "" into nulls so casting works
for c in num_cols + bool_cols:
    df = df.withColumn(c, F.when(F.trim(F.col(c)) == "", None).otherwise(F.col(c)))

# normalize boolean strings so casting is reliable
for c in bool_cols:
    df = df.withColumn(c, F.lower(F.col(c)))  # "TRUE"/"False" -> "true"/"false"

# --- Convert to DynamicFrame for downstream glue transforms

src_dyf = DynamicFrame.fromDF(df, glueContext, "src_dyf")

# --- apply schema mapping ---

mapped = ApplyMapping.apply(
    frame=src_dyf,
    mappings=[
        ("video_id", "string", "video_id", "string"),
        ("trending_date", "string", "trending_date", "string"),
        ("title", "string", "title", "string"),
        ("channel_title", "string", "channel_title", "string"),
        ("category_id", "string", "category_id", "long"),
        ("publish_time", "string", "publish_time", "string"),
        ("tags", "string", "tags", "string"),
        ("views", "string", "views", "long"),
        ("likes", "string", "likes", "long"),
        ("dislikes", "string", "dislikes", "long"),
        ("comment_count", "string", "comment_count", "long"),
        ("thumbnail_link", "string", "thumbnail_link", "string"),
        ("comments_disabled", "string", "comments_disabled", "boolean"),
        ("ratings_disabled", "string", "ratings_disabled", "boolean"),
        ("video_error_or_removed", "string", "video_error_or_removed", "boolean"),
        ("description", "string", "description", "string"),
        ("region", "string", "region", "string"),
    ],
    transformation_ctx="ChangeSchema",
)

# --- data quality checks ---

EvaluateDataQuality().process_rows(
    frame=mapped,
    ruleset=DEFAULT_DATA_QUALITY_RULESET,
    publishing_options={
        "dataQualityEvaluationContext": "DQ_ctx",
        "enableDataQualityResultsPublishing": True,
    },
    additional_options={
        "dataQualityResultsPublishing.strategy": "BEST_EFFORT",
        "observations.scope": "ALL",
    },
)
# --- write parquet file ---

spark.conf.set("spark.sql.shuffle.partitions", "1")

glueContext.write_dynamic_frame.from_options(
    frame=mapped,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": CLEANED_OUT, "partitionKeys": ["region"]},
    format_options={"compression": "snappy"},
    transformation_ctx="sink",
)

job.commit()
