#!/usr/bin/env python3
import os
import io
import sys
import boto3
import psycopg2
from psycopg2 import sql
from botocore.exceptions import ClientError


endpoint = os.getenv("MINIO_ENDPOINT")
access   = os.getenv("MINIO_ROOT_USER")
secret   = os.getenv("MINIO_ROOT_PASSWORD")
bucket   = os.getenv("MINIO_BUCKET")


pg = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "db":   os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "pwd":  os.getenv("POSTGRES_PASSWORD"),
}

s3 = boto3.client(
    "s3",
    endpoint_url=endpoint,
    aws_access_key_id=access,
    aws_secret_access_key=secret,
    config=boto3.session.Config(s3={"addressing_style": "path"})
)

def get_pg_conn():
    return psycopg2.connect(
        host=pg["host"], port=pg["port"], dbname=pg["db"],
        user=pg["user"], password=pg["pwd"]
    )

def list_csv_files(prefix="raw/products/"):
    resp = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    return [o["Key"] for o in resp.get("Contents", []) if o["Key"].endswith(".csv")]

def already_loaded(cur, filename):
    cur.execute(
        "SELECT 1 FROM raw_load_log WHERE filename = %s LIMIT 1",
        (filename,)
    )
    return cur.fetchone() is not None

def mark_as_loaded(cur, filename):
    cur.execute(
        "INSERT INTO raw_load_log(filename) VALUES (%s) ON CONFLICT DO NOTHING",
        (filename,)
    )

def load_csv_into_products(cur, key):
    obj = s3.get_object(Bucket=bucket, Key=key)
    data = obj["Body"].read().decode("utf-8")
    buf = io.StringIO(data)

    copy_sql = sql.SQL("""
        COPY products(seller_id, name, price, quantity, created_at)
        FROM STDIN WITH CSV HEADER
    """)
    cur.copy_expert(copy_sql, buf)

def main():
    try:
        conn = get_pg_conn()
        cur = conn.cursor()
    except Exception as e:
        print(f"[ERROR] Не удалось подключиться к Postgres: {e}", file=sys.stderr)
        sys.exit(1)

    for key in list_csv_files():
        fn = key.rsplit("/", 1)[-1]
        try:
            if already_loaded(cur, fn):
                print(f"[SKIP] {fn} уже загружен")
                continue

            print(f"[LOAD] {fn} → products")
            load_csv_into_products(cur, key)
            mark_as_loaded(cur, fn)
            conn.commit()

        except ClientError as e:
            print(f"[ERROR] Ошибка при доступе к S3 для {fn}: {e}", file=sys.stderr)
            conn.rollback()
        except Exception as e:
            print(f"[ERROR] При загрузке {fn}: {e}", file=sys.stderr)
            conn.rollback()

    cur.close()
    conn.close()

if __name__ == "__main__":
    main()
