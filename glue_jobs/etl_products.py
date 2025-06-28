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

# Check if already processed
def already_processed():
    try:
        s3.head_object(Bucket=bucket, Key=log_key)
        return True
    except s3.exceptions.ClientError:
        return False

if already_processed():
    print("Products.csv already processed. Skipping ETL.")
else:
    print("Starting ETL for products.csv")


        # Read and clean
    df = spark.read.option("header", True).csv(f"s3://{bucket}/{raw_key}")
    df_clean = df.dropna(subset=["product_id"]).dropDuplicates(["product_id"])

        # Write Delta
    df_clean.write.format("delta").mode("overwrite").save(processed_path)

        # Register table
    spark.sql("CREATE DATABASE IF NOT EXISTS ecom_catalog")
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS ecom_catalog.products
        USING DELTA
        LOCATION '{processed_path}'
    """)