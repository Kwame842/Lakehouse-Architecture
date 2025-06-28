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


s3 = boto3.client("s3")

# List all raw .csv files
response = s3.list_objects_v2(Bucket=bucket, Prefix=raw_prefix)
raw_files = [
    obj["Key"] for obj in response.get("Contents", [])
    if obj["Key"].endswith(".csv")
]

# List already processed logs
response = s3.list_objects_v2(Bucket=bucket, Prefix=log_prefix)
processed_logs = [
    os.path.basename(obj["Key"]).replace(".txt", "")
    for obj in response.get("Contents", [])
]

# Filter new files
files_to_process = [
    key for key in raw_files
    if os.path.basename(key).replace(".csv", "") not in processed_logs
]

if not files_to_process:
    print(" No new order_items files to process.")
else:
    # Load product reference table (FK)
    products_df = spark.read.format("delta").load(f"s3://{bucket}/lakehouse/processed/products")

    for key in files_to_process:
        print(f" Processing {key}")
        s3_path = f"s3://{bucket}/{key}"
        items_df = spark.read.option("header", True).csv(s3_path)

        # FK validation (product_id must exist in products)
        valid_items = items_df.join(products_df, on="product_id", how="inner")

        # Clean and enrich
        valid_items = valid_items.dropDuplicates(["id"])
        valid_items = valid_items.withColumn("order_timestamp", col("order_timestamp").cast("timestamp"))
        valid_items = valid_items.withColumn("date", to_date("order_timestamp"))

        # Write to Delta (append mode)
        valid_items.write.format("delta") \
            .mode("append") \
            .partitionBy("date") \
            .save(processed_path)

        # Write marker file
        marker = os.path.basename(key).replace(".csv", ".txt")
        s3.put_object(Bucket=bucket, Key=f"{log_prefix}{marker}", Body="processed")

    print(f" Processed {len(files_to_process)} new file(s).")

    # Register table
    spark.sql("CREATE DATABASE IF NOT EXISTS ecom_catalog")
    spark.sql(f"""
    CREATE TABLE IF NOT EXISTS ecom_catalog.order_items
    USING DELTA
    LOCATION '{processed_path}'
    """)
