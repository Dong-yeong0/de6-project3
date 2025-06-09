from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import io
from datetime import datetime, timedelta

RAW_BUCKET = "de6-team7"
PROCESSED_BUCKET = "de6-team7"
S3_CONN_ID = "aws_s3_conn"

def process_latest_month_daily():
    today = datetime.today()
    if today.month == 1:
        year = today.year - 1
        month = 12
    else:
        year = today.year
        month = today.month - 1
    # 전월 1일부터 말일까지 반복
    date = datetime(year, month, 1)
    while date.month == month:
        # 아래 함수는 위에서 설명한 것과 동일하게 작성
        process_and_upload_subway_day(date.year, date.month, date.day)
        date += timedelta(days=1)

def process_and_upload_subway_day(year, month, day):
    s3_raw_key = f"raw_data/subway/{year}/{month:02d}/{day:02d}/subway.parquet"
    s3_processed_key = f"processed/subway/{year}/{month:02d}/{day:02d}/cleaned.parquet"
    hook = S3Hook(aws_conn_id=S3_CONN_ID)

    obj = hook.get_key(key=s3_raw_key, bucket_name=RAW_BUCKET)
    buffer = io.BytesIO(obj.get()["Body"].read())
    df = pd.read_parquet(buffer)

    if 'REG_DATE' in df.columns:
        df = df.drop(columns=['REG_DATE'])

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

default_args = {
    'owner': 'sieun',
    'start_date': datetime(2025, 7, 6),  # 전처리 시작일 (예: 2025-07-06)
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='dag_subway_monthly_processing',
    default_args=default_args,
    schedule='0 12 6 * *',    # 매달 6일 12시
    catchup=False,
    tags=["subway", "processed", "monthly", "s3"],
) as dag:
    process_task = PythonOperator(
        task_id="process_latest_month_subway_daily",
        python_callable=process_latest_month_daily
    )
