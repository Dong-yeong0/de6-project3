from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime, timedelta
import requests
import pandas as pd
import xml.etree.ElementTree as ET
import io

API_KEY = Variable.get("SEOUL_OPEN_API_KEY")
S3_CONN_ID = "aws_s3_conn"
S3_BUCKET = "de6-team7" 

def fetch_and_upload_day_to_s3(year, month, day):
    date_str = f"{year}{month:02d}{day:02d}"
    url = f"http://openapi.seoul.go.kr:8088/{API_KEY}/xml/CardSubwayStatsNew/1/1000/{date_str}"
    res = requests.get(url, timeout=30)
    res.encoding = 'utf-8'
    root = ET.fromstring(res.text)
    rows = []
    for row in root.findall("row"):
        data = {
            "USE_YMD": row.findtext("USE_YMD"),
            "LINE": row.findtext("SBWY_ROUT_LN_NM"),
            "STATION": row.findtext("SBWY_STNS_NM"),
            "GT_ON": int(row.findtext("GTON_TNOPE") or 0),
            "GT_OFF": int(row.findtext("GTOFF_TNOPE") or 0),
            "REG_DATE": row.findtext("REG_YMD")
        }
        rows.append(data)
    if rows:
        df = pd.DataFrame(rows)
        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False)
        buffer.seek(0)
        s3_key = f"raw_data/subway/{year}/{month:02d}/{day:02d}/subway.parquet"
        hook = S3Hook(aws_conn_id=S3_CONN_ID)
        hook.load_bytes(
            buffer.getvalue(),
            key=s3_key,
            bucket_name=S3_BUCKET,
            replace=True,
        )
        print(f"[✓] Uploaded to s3://{S3_BUCKET}/{s3_key} ({len(df)} rows)")
    else:
        print(f"[!] No data for {year}{month:02d}{day:02d}")

def fetch_init_range_by_day_to_s3(start_date, end_date):
    curr = start_date
    while curr <= end_date:
        fetch_and_upload_day_to_s3(curr.year, curr.month, curr.day)
        curr += timedelta(days=1)

default_args = {
    'owner': 'sieun',
    'start_date': datetime(2024, 6, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='dag_subway_initial_daily_to_s3',
    default_args=default_args,
    schedule=None,   
    catchup=False,
    tags=["init", "subway", "daily", "s3", "load"],
) as dag:
    fetch_init_task = PythonOperator(
        task_id="fetch_init_range_by_day_to_s3",
        python_callable=fetch_init_range_by_day_to_s3,
        op_args=[
            datetime(2024, 5, 1),  
            datetime(2025, 5, 31)  
        ],
    )
