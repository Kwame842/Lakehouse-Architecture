import boto3

s3 = boto3.client("s3")
bucket = "ecom-lakehouse"

folders = ["orders", "order_items"]

for dataset in folders:
    prefix = f"lakehouse/raw/{dataset}/"
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

    for obj in response.get("Contents", []):
        key = obj["Key"]
        if key.endswith(".csv"):
            archive_key = key.replace("raw/", "archived/")
            # Copy file to archive
            s3.copy_object(
                Bucket=bucket,
                CopySource={"Bucket": bucket, "Key": key},
                Key=archive_key
            )
            # Delete original
            s3.delete_object(Bucket=bucket, Key=key)

print(" Archiving completed.")
