import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1755995711264 = glueContext.create_dynamic_frame.from_catalog(database="youtube_api_cleaned", table_name="raw_statistics", transformation_ctx="AWSGlueDataCatalog_node1755995711264")

# Script generated for node AWS Glue Data Catalog
AWSGlueDataCatalog_node1755995687932 = glueContext.create_dynamic_frame.from_catalog(database="youtube_api_cleaned", table_name="cleaned_statistics_reference_data", transformation_ctx="AWSGlueDataCatalog_node1755995687932")

# Script generated for node Join
Join_node1755995806274 = Join.apply(frame1=AWSGlueDataCatalog_node1755995687932, frame2=AWSGlueDataCatalog_node1755995711264, keys1=["id"], keys2=["category_id"], transformation_ctx="Join_node1755995806274")

# Script generated for node Amazon S3
AmazonS3_node1755996034722 = glueContext.getSink(path="s3://personal-analytics-useast2-884576820425-dev-youtubeapi", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=["region", "category_id", "ts"], enableUpdateCatalog=True, transformation_ctx="AmazonS3_node1755996034722")
AmazonS3_node1755996034722.setCatalogInfo(catalogDatabase="youtube_api_analytics",catalogTableName="final_analytics")
AmazonS3_node1755996034722.setFormat("glueparquet", compression="snappy")
AmazonS3_node1755996034722.writeFrame(Join_node1755995806274)
job.commit()