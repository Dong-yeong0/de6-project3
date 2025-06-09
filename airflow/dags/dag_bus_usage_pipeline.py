import json
import logging
import time
from datetime import timedelta, datetime
import xml.etree.ElementTree as ET
import pandas as pd
import pendulum
import requests

from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from plugins.s3 import upload_to_s3, read_from_s3
from plugins.slack import send_fail_alert

logger = logging.getLogger()
S3_BUCKET_NAME = 'de6-team7'

@dag(
    dag_id='bus_usage_pipeline',
    tags=['data-pipeline', 'etl', 'bus'],
    catchup=False,
    start_date=pendulum.datetime(2024, 5, 31, tz='UTC'),
    schedule="@daily",
    on_failure_callback=send_fail_alert,
    default_args={
        'owner': 'jun',
    },
)

def bus_usage_pipeline():

    @task
    def extract_data(**context):
        """
        서울 열린데이터 광장 버스 API에서 XML 데이터를 수집하여 S3에 저장하는 태스크
        - API 호출 → XML 파싱 → S3 업로드
        - 저장 위치: raw_data/bus/YYYY/MM/DD/bus.xml
        - INFO-200 응답일 경우 저장 생략
        """
        logical_date = context['logical_date'] - timedelta(days=1)
        use_dt = logical_date.strftime("%Y%m%d")
        api_key = Variable.get("SEOUL_DATA_API_KEY")

        url = f"http://openapi.seoul.go.kr:8088/{api_key}/xml/CardBusStatisticsServiceNew/1/1000/{use_dt}"
        try:
            response = requests.get(url)
            response.raise_for_status()
        except Exception as e:
            logger.error(f"[{use_dt}] API 요청 실패: {str(e)}")
            raise

        if "<CODE>INFO-200</CODE>" in response.text:
            logger.warning(f"[{use_dt}] 데이터 없음 - S3 저장 생략")
            return None

        s3_key = f"raw_data/bus/{logical_date.year}/{logical_date.month:02d}/{logical_date.day:02d}/bus.xml"
        try:
            upload_to_s3(
                key=s3_key,
                bucket_name=S3_BUCKET_NAME,
                data=response.text
            )
            logger.info(f"[{use_dt}] XML 저장 성공 → S3: {s3_key}")
            return {'raw_data_s3_key': s3_key}
        except Exception as e:
            logger.error(f"[{use_dt}] S3 업로드 실패: {str(e)}")
            raise



    @task
    def transform_data(extract_result, **context):
        """
        S3에 저장된 XML 원시 데이터를 불러와 변환(parsing + 정제) 후 Parquet로 저장하는 태스크
        - 저장 위치: processed_data/bus/YYYY/MM/DD/bus.parquet
        - extract_result가 None이면 태스크 스킵
        """
        if extract_result is None:
            logger.warning("transform_data: 이전 단계에서 수집된 데이터가 없어 작업을 건너뜁니다.")
            return None

        if isinstance(extract_result, str):
            extract_result = json.loads(extract_result)

        s3_key = extract_result['raw_data_s3_key']
        logical_date = context['logical_date'] - timedelta(days=1)

        xml_text = read_from_s3(key=s3_key, bucket_name=S3_BUCKET_NAME)
        root = ET.fromstring(xml_text)
        rows = root.findall("row")
        records = [{child.tag: child.text for child in row} for row in rows]

        df = pd.DataFrame(records)
        df['_updated_at'] = logical_date
        df['_loaded_at'] = logical_date

        processed_key = f"processed_data/bus/{logical_date.year}/{logical_date.month:02d}/{logical_date.day:02d}/bus.parquet"
        upload_to_s3(
            key=processed_key,
            bucket_name=S3_BUCKET_NAME,
            data=df.to_parquet(index=False)
        )
        logger.info(f"[{logical_date}] Transformed data saved to {processed_key}")
        return {'processed_data_s3_key': processed_key}

    @task
    def load_data(transform_result, **context):
        """
        Parquet 파일을 읽어 Snowflake 테이블에 적재하는 태스크
        - transform_result가 None이면 스킵
        - 기존 동일 날짜 데이터 삭제 후 COPY INTO 실행
        """
        if transform_result is None:
            logger.warning("load_data: 변환된 데이터가 없어 작업을 건너뜁니다.")
            return

        if isinstance(transform_result, str):
            transform_result = json.loads(transform_result)

        s3_key = transform_result['processed_data_s3_key']
        logical_date = context['logical_date'] - timedelta(days=1)
        snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake")

        sql = f"""
        USE SCHEMA RAW_DATA;

        BEGIN;

        DELETE FROM BUS_USAGE
        WHERE USAGE_DATE = DATE('{logical_date.strftime('%Y-%m-%d')}');

        COPY INTO BUS_USAGE
        FROM @S3_STAGE/{s3_key}
        FILE_FORMAT = (TYPE = 'PARQUET')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = 'ABORT_STATEMENT';

        COMMIT;
        """

        snowflake_hook.run(sql)
        logger.info(f"[{logical_date}] Data loaded to Snowflake from {s3_key}")


    # DAG Task Flow
    raw_data = extract_data()
    transformed = transform_data(raw_data)
    load_data(transformed)

dag_instance = bus_usage_pipeline()