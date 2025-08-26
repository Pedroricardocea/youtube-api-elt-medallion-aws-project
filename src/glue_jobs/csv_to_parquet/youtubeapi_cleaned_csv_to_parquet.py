import sys
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

DEFAULT_DATA_QUALITY_RULESET = """
    Rules = [
        ColumnCount > 0
    ]
"""

# --- paths ---
RAW_BASE = (
    "s3://personal-raw-useast2-884576820425-dev-youtubeapi/youtube/raw_statistics/"
)
RAW_GLOB = RAW_BASE + "region=*/ts=*/"
BAD_ROWS_PATH = (
    "s3://personal-cleaned-useast2-884576820425-dev-youtubeapi/youtube/tmp/badrows/csv/"
)
CLEANED_OUT = (
    "s3://personal-cleaned-useast2-884576820425-dev-youtubeapi/youtube/raw_statistics/"
)

# --- Read CSVs ---
df = (
    spark.read.option("header", "true")
    .option("multiLine", "true")
    .option("quote", '"')
    .option("escape", "\\")
    .option("encoding", "UTF-8")
    .option("mode", "PERMISSIVE")
    .option("badRecordsPath", BAD_ROWS_PATH)
    .option("basePath", RAW_BASE)
    .csv(RAW_GLOB)
)

# Strip BOM if present
first_col = df.columns[0]
if first_col.startswith("\ufeff"):
    df = df.withColumnRenamed(first_col, first_col.lstrip("\ufeff"))

# --- Cast numeric fields ---
num_cols = ["category_id", "view_count", "like_count", "comment_count"]
for c in num_cols:
    df = df.withColumn(c, F.when(F.trim(F.col(c)) == "", None).otherwise(F.col(c)))
    df = df.withColumn(c, F.col(c).cast("long"))

# --- Cast boolean fields ---
df = df.withColumn("licensed_content", F.col("licensed_content").cast("boolean"))

# --- Convert to DynamicFrame ---
src_dyf = DynamicFrame.fromDF(df, glueContext, "src_dyf")

# --- Schema mapping ---
mapped = src_dyf  # already cast above, so no need for a long ApplyMapping list

# --- Data quality check ---
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

# --- Write out as Parquet ---
spark.conf.set("spark.sql.shuffle.partitions", "1")

glueContext.write_dynamic_frame.from_options(
    frame=mapped,
    connection_type="s3",
    format="glueparquet",
    connection_options={"path": CLEANED_OUT, "partitionKeys": ["region", "ts"]},
    format_options={"compression": "snappy"},
    transformation_ctx="sink",
)

job.commit()
