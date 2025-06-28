import os
import boto3
from pyspark.context import SparkContext
from awsglue.context import GlueContext

# Setup Spark & Glue
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session.builder \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .getOrCreate()

# S3 Paths
bucket = "ecom-lakehouse"
raw_key = "uploads/products.csv"
processed_path = f"s3://{bucket}/lakehouse/processed/products"
log_key = "lakehouse/processed/_processed_log/products/products.txt"

# S3 Clients
s3 = boto3.client("s3")