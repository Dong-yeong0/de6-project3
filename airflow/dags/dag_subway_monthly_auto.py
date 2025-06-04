from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import xml.etree.ElementTree as ET
import os

API_KEY = "74776a4177746c643738656c4d6b77"
SAVE_DIR = "/opt/airflow/dags/output"

def fetch_month(year, month):
    date = datetime(year, month, 1)
    rows = []
    while date.month == month:
        day_str = date.strftime("%Y%m%d")
        url = f"http://openapi.seoul.go.kr:8088/{API_KEY}/xml/CardSubwayStatsNew/1/1000/{day_str}"
        res = requests.get(url, timeout=30)
        res.encoding = 'utf-8'
        root = ET.fromstring(res.text)
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
        date += timedelta(days=1)
    df = pd.DataFrame(rows)
    print(f"[✓] Saved {year}{month:02d} with {len(df)} rows")
    os.makedirs(SAVE_DIR, exist_ok=True)
    df.to_parquet(f"{SAVE_DIR}/subway_monthly_{year}{month:02d}.parquet", index=False)
    #df.to_csv(f"{SAVE_DIR}/subway_monthly_{year}{month:02d}.csv", index=False, encoding='utf-8-sig')

def fetch_latest_month():
    # 전월 구하기
    today = datetime.today()
    if today.month == 1:
        year = today.year - 1
        month = 12
    else:
        year = today.year
        month = today.month - 1
    fetch_month(year, month)

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 5),
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

with DAG(
    dag_id='dag_subway_monthly_auto',
    default_args=default_args,
    schedule='0 10 5 * *', 
    catchup=False,
    tags=["auto", "subway", "monthly"],
) as dag:
    fetch_task = PythonOperator(
        task_id="fetch_last_month_subway_stats",
        python_callable=fetch_latest_month
    )
