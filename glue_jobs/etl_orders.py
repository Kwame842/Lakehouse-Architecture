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

# Filter unprocessed files
files_to_process = [
    key for key in raw_files
    if os.path.basename(key).replace(".csv", "") not in processed_logs
]

if not files_to_process:
    print(" No new order files to process.")
else:
    for key in files_to_process:
        print(f" Processing {key}")
        s3_path = f"s3://{bucket}/{key}"
        df = spark.read.option("header", True).csv(s3_path)

        # Cleaning and transformations
        df = df.dropna(subset=["order_id", "order_timestamp"]).dropDuplicates(["order_id"])
        df = df.withColumn("order_timestamp", col("order_timestamp").cast("timestamp"))
        df = df.withColumn("date", to_date("order_timestamp"))

        # Write to Delta
        df.write.format("delta") \
            .mode("append") \
            .partitionBy("date") \
            .save(processed_path)

        # Create processed log
        file_marker = os.path.basename(key).replace(".csv", ".txt")
        s3.put_object(Bucket=bucket, Key=f"{log_prefix}{file_marker}", Body="processed")

    print(f" Processed {len(files_to_process)} file(s).")

    # Register table in Glue Catalog
    spark.sql("CREATE DATABASE IF NOT EXISTS ecom_catalog")
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS ecom_catalog.orders
    USING DELTA
    LOCATION '{processed_path}'
    """)

