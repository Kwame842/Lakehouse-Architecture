import boto3
import pandas as pd
from io import BytesIO
import os

# S3 config
bucket = "ecom-lakehouse"
prefix = "uploads/"
log_prefix = "lakehouse/processed/_processed_log/excel/"

# Mapping xlsx file to raw dataset
DATASET_PREFIX_MAP = {
    "orders_apr_2025.xlsx": "orders",
    "order_items_apr_2025.xlsx": "order_items"
}

s3 = boto3.client("s3")

def already_processed(file_name):
    log_key = f"{log_prefix}{file_name.replace('.xlsx', '.txt')}"
    try:
        s3.head_object(Bucket=bucket, Key=log_key)
        return True
    except s3.exceptions.ClientError:
        return False

def mark_as_processed(file_name):
    marker_key = f"{log_prefix}{file_name.replace('.xlsx', '.txt')}"
    s3.put_object(Bucket=bucket, Key=marker_key, Body="converted")

def main():
    # List uploaded .xlsx files
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    for obj in response.get("Contents", []):
        key = obj["Key"]
        file_name = os.path.basename(key)

        if file_name.endswith(".xlsx") and file_name in DATASET_PREFIX_MAP:
            if already_processed(file_name):
                print(f" {file_name} already converted. Skipping.")
                continue

            dataset = DATASET_PREFIX_MAP[file_name]
            print(f" Converting {file_name} into raw/{dataset}/...")

            # Read Excel from S3
            response = s3.get_object(Bucket=bucket, Key=key)
            content = response["Body"].read()
            xls = pd.read_excel(BytesIO(content), sheet_name=None)

            # Write each sheet as a CSV
            for sheet_name, df in xls.items():
                csv_buffer = BytesIO()
                df.to_csv(csv_buffer, index=False)
                out_key = f"lakehouse/raw/{dataset}/{file_name.replace('.xlsx','')}_{sheet_name}.csv"
                s3.put_object(Bucket=bucket, Key=out_key, Body=csv_buffer.getvalue())
                print(f" Written: {out_key}")

            # Mark as processed
            mark_as_processed(file_name)
            print(f" Marker written for {file_name}")

if __name__ == "__main__":
    main()
