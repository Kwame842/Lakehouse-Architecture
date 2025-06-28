import os
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, to_date


# Initialize Spark and Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# S3 configuration
bucket = "ecom-lakehouse"
raw_prefix = "lakehouse/raw/orders/"
processed_path = f"s3://{bucket}/lakehouse/processed/orders/"
log_prefix = "lakehouse/processed/_processed_log/orders/"


# Initialize Boto3 S3 client
s3 = boto3.client("s3")

# Get all raw .csv files
response = s3.list_objects_v2(Bucket=bucket, Prefix=raw_prefix)
raw_files = [
    obj["Key"] for obj in response.get("Contents", [])
    if obj["Key"].endswith(".csv")
]

# Get all marker logs (already processed)
response = s3.list_objects_v2(Bucket=bucket, Prefix=log_prefix)
processed_logs = [
    os.path.basename(obj["Key"]).replace(".txt", "")
    for obj in response.get("Contents", [])
]