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
S3_CONN_ID = "aws_s3_conn"  # Airflow Connection ID (웹UI에서 만든 값)
S3_BUCKET = "de6-team7"  # 본인의 S3 버킷명으로 변경

def fetch_days_in_month_to_s3(year, month):
    date = datetime(year, month, 1)
    while date.month == month:
        day_str = date.strftime("%Y%m%d")
        url = f"http://openapi.seoul.go.kr:8088/{API_KEY}/xml/CardSubwayStatsNew/1/1000/{day_str}"
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
            s3_key = f"raw_data/subway/{date.year}/{date.month:02d}/{date.day:02d}/subway.parquet"
            hook = S3Hook(aws_conn_id=S3_CONN_ID)
            hook.load_bytes(
                buffer.getvalue(),
                key=s3_key,
                bucket_name=S3_BUCKET,
                replace=True,
            )
            print(f"[✓] Uploaded to s3://{S3_BUCKET}/{s3_key} ({len(df)} rows)")
        else:
            print(f"[!] No data for {day_str}")
        date += timedelta(days=1)

def fetch_latest_month_daily_to_s3():
    # 전월 구하기
    today = datetime.today()
    if today.month == 1:
        year = today.year - 1
        month = 12
    else:
        year = today.year
        month = today.month - 1
    fetch_days_in_month_to_s3(year, month)

default_args = {
    'owner': 'sieun',
    'start_date': datetime(2025, 7, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='dag_subway_daily_auto_s3',
    default_args=default_args,
    schedule='0 10 5 * *',
    catchup=False,
    tags=["auto", "subway", "daily", "s3"],
) as dag:
    fetch_task = PythonOperator(
        task_id="fetch_last_month_subway_stats_daily_to_s3",
        python_callable=fetch_latest_month_daily_to_s3
    )
