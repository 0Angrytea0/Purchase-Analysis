import os
import boto3
from botocore.exceptions import ClientError

endpoint = os.getenv("MINIO_ENDPOINT")
access  = os.getenv("MINIO_ROOT_USER")
secret  = os.getenv("MINIO_ROOT_PASSWORD")
bucket  = os.getenv("MINIO_BUCKET")

local_dir = os.path.join(os.getcwd(), "raw_data", "products")

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=access,
    aws_secret_access_key=secret,
    config=boto3.session.Config(s3={"addressing_style": "path"})
)

def upload_all():
    if not os.path.isdir(local_dir):
        print(f"[ERROR] Каталог не найден: {local_dir}")
        return

    for fn in os.listdir(local_dir):
        if not fn.lower().endswith(".csv"):
            continue
        key = f"raw/products/{fn}"
        path = os.path.join(local_dir, fn)

        try:
            s3.head_object(Bucket=bucket, Key=key)
            print(f"[SKIP] {fn} уже загружен.")
            continue
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code not in ("404", "NoSuchKey"):
                raise

        print(f"[UPLOAD] {fn} → s3://{bucket}/{key}")
        s3.upload_file(path, bucket, key)

if __name__ == "__main__":
    upload_all()
