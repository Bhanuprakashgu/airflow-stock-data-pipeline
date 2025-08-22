from __future__ import annotations
import os
import time
from datetime import datetime, timedelta
import pendulum
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2.extras import execute_values

API_KEY = os.environ.get("ALPHA_VANTAGE_API_KEY")
SYMBOLS = [s.strip().upper() for s in os.environ.get("STOCK_SYMBOLS", "IBM").split(",") if s.strip()]
TABLE_NAME = "stock_data"
REQUEST_INTERVAL_SEC = int(os.environ.get("REQUEST_INTERVAL_SEC", "15"))

def create_table_if_not_exists():
    pg = PostgresHook(postgres_conn_id="postgres_default")
    sql = f"""
    CREATE TABLE IF NOT EXISTS {TABLE_NAME} (
        symbol TEXT NOT NULL,
        record_date DATE NOT NULL,
        open DECIMAL(18,6),
        high DECIMAL(18,6),
        low DECIMAL(18,6),
        close DECIMAL(18,6),
        volume BIGINT,
        fetched_at TIMESTAMP NOT NULL DEFAULT NOW(),
        PRIMARY KEY (symbol, record_date)
    );
    """
    pg.run(sql)
    print(f"Table '{TABLE_NAME}' ensured.")

def fetch_stock_data(symbol: str):
    if not API_KEY:
        raise ValueError("ALPHA_VANTAGE_API_KEY is not set.")

    url = "https://www.alphavantage.co/query"
    params = {"function": "TIME_SERIES_DAILY", "symbol": symbol, "apikey": API_KEY}
    last_err = None
    for attempt in range(1, 6):
        try:
            r = requests.get(url, params=params, timeout=30)
            r.raise_for_status()
            data = r.json()
            if "Error Message" in data:
                raise ValueError(f"API error for {symbol}: {data['Error Message']}")
            if "Note" in data:
                time.sleep(REQUEST_INTERVAL_SEC * attempt)
                last_err = RuntimeError("Rate limited.")
                continue
            time.sleep(REQUEST_INTERVAL_SEC)
            return data
        except requests.RequestException as e:
            last_err = e
            time.sleep(REQUEST_INTERVAL_SEC * attempt)
    raise RuntimeError(f"Failed to fetch {symbol}: {last_err}")

def process_and_store_data(symbol: str, **context):
    ti = context["ti"]
    data = ti.xcom_pull(task_ids=f"fetch_{symbol}")
    if not data:
        print(f"[{symbol}] No payload.")
        return

    series = data.get("Time Series (Daily)", {})
    if not series:
        print(f"[{symbol}] Empty series.")
        return

    rows = []
    for date_str, vals in series.items():
        try:
            record_date = datetime.strptime(date_str, "%Y-%m-%d").date()
            rows.append((
                symbol,
                record_date,
                float(vals.get("1. open", 0) or 0),
                float(vals.get("2. high", 0) or 0),
                float(vals.get("3. low", 0) or 0),
                float(vals.get("4. close", 0) or 0),
                int(float(vals.get("5. volume", 0) or 0)),
            ))
        except Exception as e:
            print(f"[{symbol}] Skipping row {date_str}: {e}")

    if not rows:
        return

    upsert_sql = f"""
    INSERT INTO {TABLE_NAME} (symbol, record_date, open, high, low, close, volume)
    VALUES %s
    ON CONFLICT (symbol, record_date) DO UPDATE SET
        open = EXCLUDED.open,
        high = EXCLUDED.high,
        low = EXCLUDED.low,
        close = EXCLUDED.close,
        volume = EXCLUDED.volume,
        fetched_at = NOW();
    """
    pg = PostgresHook(postgres_conn_id="postgres_default")
    conn = pg.get_conn()
    try:
        with conn.cursor() as cur:
            execute_values(cur, upsert_sql, rows, page_size=500)
        conn.commit()
        print(f"[{symbol}] Upserted {len(rows)} rows.")
    except Exception as e:
        conn.rollback()
        raise RuntimeError(f"[{symbol}] DB error: {e}")
    finally:
        conn.close()

with DAG(
    dag_id="stock_data_pipeline",
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule_interval=timedelta(days=1),  # change to hours if needed
    catchup=False,
    tags=["stock_data", "ETL"],
) as dag:

    create_table = PythonOperator(
        task_id="create_table_if_not_exists",
        python_callable=create_table_if_not_exists,
    )

    for sym in SYMBOLS:
        fetch = PythonOperator(
            task_id=f"fetch_{sym}",
            python_callable=fetch_stock_data,
            op_args=[sym],
            do_xcom_push=True,
            retries=3,
            retry_delay=timedelta(minutes=2),
        )
        load = PythonOperator(
            task_id=f"load_{sym}",
            python_callable=process_and_store_data,
            op_args=[sym],
            provide_context=True,
        )
        create_table >> fetch >> load
