#!/usr/bin/env python3
import os
import io
import sys
import psycopg2
from psycopg2 import sql

# Параметры подключения к Postgres берутся из .env
pg = {
    "host": os.getenv("POSTGRES_HOST"),
    "port": os.getenv("POSTGRES_PORT"),
    "db": os.getenv("POSTGRES_DB"),
    "user": os.getenv("POSTGRES_USER"),
    "pwd": os.getenv("POSTGRES_PASSWORD"),
}

# Путь к локальным CSV
LOCAL_DIR = os.getenv("CSV_LOCAL_DIR", "/app/raw_data/products")


def get_pg_conn():
    return psycopg2.connect(
        host=pg["host"], port=pg["port"], dbname=pg["db"],
        user=pg["user"], password=pg["pwd"]
    )


def list_csv_files():
    if not os.path.isdir(LOCAL_DIR):
        print(f"[ERROR] Не найден каталог {LOCAL_DIR}", file=sys.stderr)
        sys.exit(1)
    return [
        os.path.join(LOCAL_DIR, fn)
        for fn in os.listdir(LOCAL_DIR)
        if fn.lower().endswith(".csv")
    ]


def main():
    try:
        conn = get_pg_conn()
        cur = conn.cursor()
    except Exception as e:
        print(f"[ERROR] Не удалось подключиться к Postgres: {e}", file=sys.stderr)
        sys.exit(1)

    for path in list_csv_files():
        fn = os.path.basename(path)
        try:
            print(f"[LOAD] {fn} → products")
            with open(path, 'r', encoding='utf-8') as f:
                cur.copy_expert(
                    sql.SQL("""
                        COPY products(seller_id, name, price, quantity, created_at)
                        FROM STDIN WITH CSV HEADER
                    """),
                    f
                )
            conn.commit()
        except Exception as e:
            print(f"[ERROR] При загрузке {fn}: {e}", file=sys.stderr)
            conn.rollback()

    cur.close()
    conn.close()


if __name__ == "__main__":
    main()