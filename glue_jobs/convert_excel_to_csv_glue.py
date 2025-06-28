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