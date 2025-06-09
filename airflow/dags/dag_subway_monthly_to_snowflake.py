from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import io
from datetime import datetime, timedelta

PROCESSED_BUCKET = "de6-team7"
S3_CONN_ID = "aws_s3_conn"
SNOWFLAKE_CONN_ID = "snowflake"
TABLE_NAME = "SUBWAY_INFO"

def upload_latest_month_to_snowflake():
    today = datetime.today()
    if today.month == 1:
        year = today.year - 1
        month = 12
    else:
        year = today.year
        month = today.month - 1
    date = datetime(year, month, 1)

    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    sf_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    connection = sf_hook.get_conn()

    try:
        while date.month == month:
            s3_key = f"processed/subway/{date.year}/{date.month:02d}/{date.day:02d}/cleaned.parquet"
            print(f"[{date.strftime('%Y-%m-%d')}] Loading {s3_key}")
            try:
                obj = s3_hook.get_key(key=s3_key, bucket_name=PROCESSED_BUCKET)
                buffer = io.BytesIO(obj.get()["Body"].read())
                df = pd.read_parquet(buffer)
            except Exception as e:
                print(f"[!] S3 파일 불러오기 실패 ({s3_key}): {e}")
                date += timedelta(days=1)
                continue

            write_pandas(connection, df, TABLE_NAME)
            print(f"[✓] Uploaded {len(df)} rows to {TABLE_NAME} for {date.strftime('%Y-%m-%d')}")
            date += timedelta(days=1)
    finally:
        connection.close()

default_args = {
    'owner': 'sieun',
    'start_date': datetime(2025, 7, 7),   # 최초 갱신 일자 (적절히 조정)
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='dag_subway_monthly_to_snowflake',
    default_args=default_args,
    schedule='0 14 7 * *',   # 매달 7일 14시, processed 이후에 동작
    catchup=False,
    tags=["subway", "snowflake", "monthly", "auto"],
) as dag:
    upload_task = PythonOperator(
        task_id="upload_latest_month_to_snowflake",
        python_callable=upload_latest_month_to_snowflake
    )
