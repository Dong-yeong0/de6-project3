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

def upload_init_range_to_snowflake(start_date, end_date):
    s3_hook = S3Hook(aws_conn_id=S3_CONN_ID)
    sf_hook = SnowflakeHook(snowflake_conn_id=SNOWFLAKE_CONN_ID)
    connection = sf_hook.get_conn()
    curr = start_date

    try:
        while curr <= end_date:
            s3_key = f"processed/subway/{curr.year}/{curr.month:02d}/{curr.day:02d}/cleaned.parquet"
            print(f"[{curr.strftime('%Y-%m-%d')}] Loading {s3_key}")
            try:
                obj = s3_hook.get_key(key=s3_key, bucket_name=PROCESSED_BUCKET)
                buffer = io.BytesIO(obj.get()["Body"].read())
                df = pd.read_parquet(buffer)

                #if "USE_YMD" in df.columns:
                    #df["USE_YMD"] = pd.to_datetime(df["USE_YMD"].astype(str), format="%Y%m%d", errors="coerce").dt.date
                    
            except Exception as e:
                print(f"[!] S3 파일 불러오기 실패 ({s3_key}): {e}")
                curr += timedelta(days=1)
                continue

            # write_pandas로 한번에 적재
            write_pandas(connection, df, TABLE_NAME)
            print(f"[✓] Uploaded {len(df)} rows to {TABLE_NAME} for {curr.strftime('%Y-%m-%d')}")
            curr += timedelta(days=1)
    finally:
        connection.close()

default_args = {
    'owner': 'sieun',
    'start_date': datetime(2024, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='dag_subway_initial_to_snowflake',
    default_args=default_args,
    schedule=None,   # 수동 실행
    catchup=False,
    tags=["subway", "snowflake", "init", "bulk"],
) as dag:
    upload_task = PythonOperator(
        task_id="upload_init_range_to_snowflake",
        python_callable=upload_init_range_to_snowflake,
        op_args=[
            datetime(2024, 5, 1),    # 시작 날짜
            datetime(2025, 5, 31),   # 종료 날짜
        ],
    )
