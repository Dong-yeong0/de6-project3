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

# ✅ 1. 수집: API 호출 → S3 저장
def collect_subway_data(**context):
    api_key = Variable.get("SUBWAY_LOCATION_API_KEY")
    base_url = "https://api.odcloud.kr/api/15099316/v1/uddi:e3362505-a95b-4480-8deb-b3e0671bc320"
    params = {
        "page": 1,
        "perPage": 1000,
        "serviceKey": api_key
    }

    response = requests.get(base_url, params=params)
    if response.status_code != 200:
        raise Exception(f"❌ 요청 실패: {response.status_code}")

    data = response.json().get("data", [])
    if not data:
        raise Exception("❌ 데이터 없음")

    run_date = context['ds_nodash']
    exec_date = context['execution_date']
    s3_key = f"raw_data/subway/{exec_date.strftime('%Y/%m')}/subway_station_{run_date}.json"

    local_path = f"/tmp/subway_station_{run_date}.json"
    with open(local_path, 'w') as f:
        json.dump(data, f)

    s3_hook = S3Hook(aws_conn_id="aws_s3_conn")
    s3_hook.load_file(
        filename=local_path,
        key=s3_key,
        bucket_name="de6-team7",
        replace=True
    )
    os.remove(local_path)

# ✅ 2. 전처리: JSON → Parquet
def preprocess_subway_data(**context):
    run_date = context['ds_nodash']
    exec_date = context['execution_date']
    s3_key = f"raw_data/subway/{exec_date.strftime('%Y/%m')}/subway_station_{run_date}.json"
    output_key = f"processed_data/subway/{exec_date.strftime('%Y/%m')}/subway_station_{run_date}.parquet"

    s3_hook = S3Hook(aws_conn_id="aws_s3_conn")
    raw = s3_hook.get_key(key=s3_key, bucket_name="de6-team7").get()["Body"].read().decode("utf-8")
    records = json.loads(raw)

    data = []
    for idx, r in enumerate(records, start=1):
        if None in [r.get("역명"), r.get("위도"), r.get("경도"), r.get("호선")]:
            continue
        data.append({
            "STATION_NAME": r.get("역명"),
            "EXTERNAL_STATION_CODE": r.get("고유역번호(외부역코드)") or -1,
            "LATITUDE": r.get("위도"),
            "LONGITUDE": r.get("경도"),
            "LINE_NO": r.get("호선"),
            "SEQ_NO": idx,
            "CREATED_DATE": datetime.today().date()
        })

    df = pd.DataFrame(data)
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

# ✅ 3. 적재: Snowflake FULL REFRESH
def load_subway_data(**context):
    run_date = context['ds_nodash']
    exec_date = context['execution_date']
    parquet_key = f"processed_data/subway/{exec_date.strftime('%Y/%m')}/subway_station_{run_date}.parquet"

    s3_hook = S3Hook(aws_conn_id="aws_s3_conn")
    data = s3_hook.get_key(key=parquet_key, bucket_name="de6-team7").get()["Body"].read()
    table = pq.read_table(BytesIO(data))
    df = table.to_pandas()

    snow_hook = SnowflakeHook(snowflake_conn_id="snowflake")
    conn = snow_hook.get_conn()
    cursor = conn.cursor()

    try:
        cursor.execute("TRUNCATE TABLE PUBLIC_TRANSPORTATION.RAW_DATA.SEOUL_METRO_STATION_COORDINATES")

        insert_sql = """
            INSERT INTO PUBLIC_TRANSPORTATION.RAW_DATA.SEOUL_METRO_STATION_COORDINATES
            (STATION_NAME, EXTERNAL_STATION_CODE, LATITUDE, LONGITUDE, LINE_NO, SEQ_NO, CREATED_DATE)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
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
    dag_id='dag_subway_pipeline',
    default_args=default_args,
    schedule_interval='@monthly',
    catchup=False,
    tags=['subway', 'pipeline'],
    description='지하철 위치정보 수집 → 전처리 → Snowflake 적재하는 단일 DAG'
) as dag:

    collect = PythonOperator(
        task_id='collect_subway_data',
        python_callable=collect_subway_data,
        provide_context=True
    )

    preprocess = PythonOperator(
        task_id='preprocess_subway_data',
        python_callable=preprocess_subway_data,
        provide_context=True
    )

    load = PythonOperator(
        task_id='load_subway_data',
        python_callable=load_subway_data,
        provide_context=True
    )

    collect >> preprocess >> load
