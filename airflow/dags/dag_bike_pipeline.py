from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import os
import pandas as pd
from io import BytesIO
import pyarrow as pa
import pyarrow.parquet as pq
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable

# ✅ 1. 수집: API 호출 → S3 JSON 저장
def collect_bike_data(**context):
    api_key = Variable.get("BIKE_LOCATION_API_KEY")
    base_url = "http://openapi.seoul.go.kr:8088"
    endpoint = "tbCycleStationInfo"
    json_key = "stationInfo"
    total_count = 3163

    results = []
    for start in range(1, total_count + 1, 1000):
        end = min(start + 999, total_count)
        url = f"{base_url}/{api_key}/json/{endpoint}/{start}/{end}/"
        response = requests.get(url)
        if response.status_code == 200:
            try:
                data = response.json()
                rows = data.get(json_key, {}).get('row', [])
                results.extend(rows)
            except Exception as e:
                print(f"❌ JSON 파싱 오류: {e}")
        else:
            print(f"❌ 요청 실패: {response.status_code}")

    if not results:
        raise Exception("❌ 결과 없음")

    run_date = context['ds_nodash']
    exec_date = context['execution_date']
    s3_key = f"raw_data/bike/{exec_date.strftime('%Y/%m')}/bike_station_{run_date}.json"


    local_path = f"/tmp/bike_station_{run_date}.json"
    with open(local_path, 'w') as f:
        json.dump(results, f)

    s3_hook = S3Hook(aws_conn_id="aws_s3_conn")
    s3_hook.load_file(
        filename=local_path,
        key=s3_key,
        bucket_name="de6-team7",
        replace=True
    )
    os.remove(local_path)

# ✅ 2. 전처리: JSON → 컬럼 정제 → Parquet 저장
def preprocess_bike_data(**context):
    run_date = context['ds_nodash']
    exec_date = context['execution_date']
    s3_key = f"raw_data/bike/{exec_date.strftime('%Y/%m')}/bike_station_{run_date}.json"
    output_key = f"processed_data/bike/{exec_date.strftime('%Y/%m')}/bike_station_{run_date}.parquet"


    s3_hook = S3Hook(aws_conn_id="aws_s3_conn")
    raw = s3_hook.get_key(key=s3_key, bucket_name="de6-team7").get()["Body"].read().decode("utf-8")
    data = json.loads(raw)

    df = pd.DataFrame(data)[['STA_ADD1', 'STA_ADD2', 'STA_LAT', 'STA_LONG', 'RENT_ID']]
    df = df.rename(columns={
        'STA_ADD1': 'ADDRESS1',
        'STA_ADD2': 'ADDRESS2',
        'STA_LAT': 'LATITUDE',
        'STA_LONG': 'LONGITUDE',
        'RENT_ID': 'STATION_ID'
    })
    df = df[['STATION_ID', 'ADDRESS1', 'ADDRESS2', 'LATITUDE', 'LONGITUDE']]
    df['LATITUDE'] = df['LATITUDE'].astype(float).replace(0.0, pd.NA)
    df['LONGITUDE'] = df['LONGITUDE'].astype(float).replace(0.0, pd.NA)
    df = df.astype(object).where(pd.notnull(df), None)

    table = pa.Table.from_pandas(df)
    buf = BytesIO()
    pq.write_table(table, buf)
    buf.seek(0)

    s3_hook.load_bytes(
        bytes_data=buf.read(),
        key=output_key,
        bucket_name="de6-team7",
        replace=True
    )

# ✅ 3. 적재: Parquet → Snowflake FULL REFRESH
def load_bike_data(**context):
    run_date = context['ds_nodash']
    exec_date = context['execution_date']
    parquet_key = f"processed_data/bike/{exec_date.strftime('%Y/%m')}/bike_station_{run_date}.parquet"

    s3_hook = S3Hook(aws_conn_id="aws_s3_conn")
    data = s3_hook.get_key(key=parquet_key, bucket_name="de6-team7").get()["Body"].read()
    table = pq.read_table(BytesIO(data))
    df = table.to_pandas()
    df = df.astype(object).where(pd.notnull(df), None)

    snow_hook = SnowflakeHook(snowflake_conn_id="snowflake")
    conn = snow_hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("TRUNCATE TABLE PUBLIC_TRANSPORTATION.RAW_DATA.BIKE_STATION_COORDINATES")

        insert_sql = """
            INSERT INTO PUBLIC_TRANSPORTATION.RAW_DATA.BIKE_STATION_COORDINATES
            (STATION_ID, ADDRESS1, ADDRESS2, LATITUDE, LONGITUDE)
            VALUES (%s, %s, %s, %s, %s)
        """

        cursor.executemany(insert_sql, df.values.tolist())
        print(f"✅ 총 {len(df)}건 업로드 완료")
    finally:
        cursor.close()
        conn.close()

# ✅ DAG 설정
default_args = {
    'owner': 'beom',
    'start_date': datetime(2025, 6, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='dag_bike_pipeline',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False,
    tags=['bike', 'pipeline'],
    description='자전거 위치정보 수집 → 전처리 → Snowflake 적재하는 단일 DAG'
) as dag:

    collect = PythonOperator(
        task_id='collect_bike_data',
        python_callable=collect_bike_data,
        provide_context=True
    )

    preprocess = PythonOperator(
        task_id='preprocess_bike_data',
        python_callable=preprocess_bike_data,
        provide_context=True
    )

    load = PythonOperator(
        task_id='load_bike_data',
        python_callable=load_bike_data,
        provide_context=True
    )

    collect >> preprocess >> load
