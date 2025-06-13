from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import io
from datetime import datetime, timedelta

RAW_BUCKET = "de6-team7"
PROCESSED_BUCKET = "de6-team7"
S3_CONN_ID = "aws_s3_conn"

def process_and_upload_subway_day(year, month, day):
    # raw parquet S3 경로
    s3_raw_key = f"raw_data/subway/{year}/{month:02d}/{day:02d}/subway.parquet"
    # processed parquet S3 경로 (요청하신 포맷)
    s3_processed_key = f"processed/subway/{year}/{month:02d}/{day:02d}/cleaned.parquet"
    hook = S3Hook(aws_conn_id=S3_CONN_ID)

    # S3에서 raw parquet 읽기
    obj = hook.get_key(key=s3_raw_key, bucket_name=RAW_BUCKET)
    buffer = io.BytesIO(obj.get()["Body"].read())
    df = pd.read_parquet(buffer)

    # REG_DATE 컬럼 삭제 (전처리)
    if 'REG_DATE' in df.columns:
        df = df.drop(columns=['REG_DATE'])

    # S3에 processed parquet로 저장 (cleaned.parquet)
    out_buffer = io.BytesIO()
    df.to_parquet(out_buffer, index=False)
    out_buffer.seek(0)
    hook.load_bytes(
        out_buffer.getvalue(),
        key=s3_processed_key,
        bucket_name=PROCESSED_BUCKET,
        replace=True
    )
    print(f"[✓] Processed file uploaded to s3://{PROCESSED_BUCKET}/{s3_processed_key}")

def process_init_range_by_day(start_date, end_date):
    curr = start_date
    while curr <= end_date:
        print(f"[{curr.strftime('%Y-%m-%d')}] Processing…")
        try:
            process_and_upload_subway_day(curr.year, curr.month, curr.day)
        except Exception as e:
            print(f"[!] Failed to process {curr.strftime('%Y-%m-%d')}: {e}")
        curr += timedelta(days=1)

default_args = {
    'owner': 'sieun',
    'start_date': datetime(2024, 5, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='dag_subway_initial_daily_processing',
    default_args=default_args,
    schedule=None,   # 수동 실행
    catchup=False,
    tags=["subway", "processed", "init", "daily", "s3"],
) as dag:
    process_init_task = PythonOperator(
        task_id="process_init_range_by_day",
        python_callable=process_init_range_by_day,
        op_args=[
            datetime(2024, 5, 1),    # 시작 날짜
            datetime(2025, 5, 31),   # 종료 날짜
        ],
    )
