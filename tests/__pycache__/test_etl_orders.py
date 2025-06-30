import pandas as pd
from io import StringIO
from datetime import datetime

# Simulate a sample raw order dataset (like from lakehouse/raw/orders/)
csv_data = """
order_id,customer_id,total_amount,order_date
1,1001,49.99,2025-06-01
2,1002,89.99,2025/06/02
2,1002,89.99,2025/06/02
3,,99.00,2025-06-03
4,1003,15.00,
"""

def clean_orders(df):
    # Simulate what your Glue script does
    df = df.dropna(subset=["order_id", "customer_id"])
    df = df.drop_duplicates(subset=["order_id"])

    # Fix date format
    df["order_date"] = pd.to_datetime(df["order_date"], errors="coerce")
    df = df.dropna(subset=["order_date"])  # remove rows with invalid/missing dates
    return df

def test_etl_orders_cleaning():
    df_raw = pd.read_csv(StringIO(csv_data))
    df_cleaned = clean_orders(df_raw)

    # Check no nulls
    assert df_cleaned["order_id"].isnull().sum() == 0
    assert df_cleaned["customer_id"].isnull().sum() == 0
    assert df_cleaned["order_date"].isnull().sum() == 0

    # Check duplicates removed
    assert df_cleaned["order_id"].duplicated().sum() == 0

    # Check row count (should be 2 valid rows: ID 1 and 2 only)
    assert len(df_cleaned) == 2

    # Check date parsing
    assert all(isinstance(x, pd.Timestamp) for x in df_cleaned["order_date"])
