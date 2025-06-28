import os
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from pyspark.sql.functions import col, to_date

# Initialize Spark and Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# S3 paths and config
bucket = "ecom-lakehouse"
raw_prefix = "lakehouse/raw/order_items/"
processed_path = f"s3://{bucket}/lakehouse/processed/order_items/"
log_prefix = "lakehouse/processed/_processed_log/order_items/"