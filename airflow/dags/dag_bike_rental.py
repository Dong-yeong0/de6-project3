import json
import logging
import time
from datetime import timedelta

import pandas as pd
import pendulum
import requests
from airflow.decorators import dag, task
from airflow.models.variable import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from plugins.s3 import read_from_s3, upload_to_s3
from plugins.slack import send_fail_alert

logger = logging.getLogger()
S3_BUCKET_NAME = 'de6-team7'
@dag(
    dag_id='bike_rental_pipeline',
    tags=['data-pipeline', 'etl', 'bike'],
    catchup=False,
    start_date=pendulum.datetime(2025, 5, 31, tz='UTC'),
    schedule="@daily",
    on_failure_callback=send_fail_alert,
    default_args={
        'owner': 'dongyeong',
    },
)
def bike_rental_pipeline():
    @task
    def extract_data(**context):
        logical_date = context['logical_date'] - timedelta(1)
        target_date = logical_date.strftime('%Y%m%d')
        logger.info(f"Extract started for date: {target_date}")

        # Load
        datas = []
        api_key = Variable.get('SEOUL_DATA_API_KEY')
        base_url = f"http://openapi.seoul.go.kr:8088/{api_key}/json/tbCycleRentUseTimeInfo"
        for hour in range(1, 24):
            logger.info(f"Extracting data for hour: {hour}")
            start_idx = 1
            end_idx = 1000
            url = f'{base_url}/{start_idx}/{end_idx}/{target_date}/{hour}'
            res = requests.get(url)
            res.raise_for_status()
            total_count = int(res.json()['cycleRentUseTimeInfo']['list_total_count'])
            logger.info(f"Total rows expected: {total_count} for hour {hour}")
            while start_idx <= total_count:
                url = f'{base_url}/{start_idx}/{end_idx}/{target_date}/{hour}'
                res = requests.get(url)
                res.raise_for_status()
                res_json = res.json()
                rows = res_json.get('cycleRentUseTimeInfo', {}).get('row', [])
                if not rows:
                    logger.warning(f"Empty data at {start_idx}-{end_idx} for hour {hour}")
                    break

                datas.extend(rows)
                start_idx += 1000
                end_idx = min(end_idx + 1000, total_count)
                time.sleep(0.3)

        logger.info(f"Total rows extracted: {len(datas)}")

        # Save
        raw_data_s3_key = f"raw_data/bike/{logical_date.year}/{logical_date.month:02d}/{logical_date.day:02d}/raw_data.json"
        logger.info(f"Uploading raw data to S3 at {raw_data_s3_key}")
        upload_to_s3(
            key=raw_data_s3_key,
            bucket_name=S3_BUCKET_NAME,
            data=json.dumps(datas),
        )
        return {'raw_data_s3_key': raw_data_s3_key}

    @task
    def transform_data(extract_result, **context):
        if isinstance(extract_result, str):
            extract_result = json.loads(extract_result)

        raw_data_s3_key = extract_result.get('raw_data_s3_key')
        
        # Load
        logger.info(f"Loading raw data from S3 key: {raw_data_s3_key}")
        raw_data = json.loads(
            read_from_s3(
                key=raw_data_s3_key,
                bucket_name=S3_BUCKET_NAME
            )
        )
        logger.info(f"Raw data rows loaded: {len(raw_data)}")

        df = pd.DataFrame(raw_data)
        logger.info(f"DataFrame shape after load: {df.shape}")

        # Preprocessing
        df.drop(['START_INDEX', 'END_INDEX', 'RNUM'], axis=1, inplace=True)
        df.columns = [
            'usage_date',
            'usage_time',
            'station_id',
            'station_name',
            'rental_type_code',
            'gender',
            'age_group',
            'usage_count',
            'calories',
            'carbon_emission',
            'distance',
            'duration',
        ]
        
        df['station_name'] = df['station_name'].str.split('.').str[1].str.strip()
        
        numeric_columns = ['usage_time', 'usage_count', 'duration']
        float_columns = ['calories', 'carbon_emission', 'distance']
        
        # Convert date and time_slot to datetime
        df['usage_date'] = pd.to_datetime(df['usage_date']).dt.date
        
        # Convert numeric(int) columns
        for col in numeric_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce').astype('Int64')

        # Convert numeric(float) columns
        for col in float_columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

        logger.info(f"Sample data after transform\n{df.head()}")

        # Save
        logical_date = context['logical_date'] - timedelta(1)
        processed_data_s3_key = f'processed_data/bike/{logical_date.year}/{logical_date.month}/{logical_date.day}/proceesed_data.parquet'
        logger.info(f"Uploading processed parquet data to S3 at {processed_data_s3_key}")
        upload_to_s3(
            key=processed_data_s3_key,
            bucket_name=S3_BUCKET_NAME,
            data=df.to_parquet(engine='pyarrow')
        )
        return {'processed_data_s3_key': processed_data_s3_key}

    @task
    def load_data(transform_result, **context):
        if isinstance(transform_result, str):
            transform_result = json.loads(transform_result)

        processed_data_s3_key = transform_result['processed_data_s3_key']
        logical_date = context['logical_date'] - timedelta(1)
        snowflake_hook = SnowflakeHook(snowflake_conn_id='snowflake')
        logger.info(f"Loading data into Snowflake for date: {logical_date.strftime('%Y-%m-%d')}")
        sql = f"""
        USE SCHEMA RAW_DATA;
        
        BEGIN;
        
        DELETE FROM BIKE_RENTALS_BY_HOUR
        WHERE USAGE_DATE = DATE('{logical_date.strftime('%Y-%m-%d')}');
        
        COPY INTO BIKE_RENTALS_BY_HOUR
        FROM @S3_STAGE/{processed_data_s3_key}
        FILE_FORMAT = (TYPE = 'PARQUET')
        MATCH_BY_COLUMN_NAME = 'CASE_INSENSITIVE'
        ON_ERROR = 'ABORT_STATEMENT';

        COMMIT;
        """
        snowflake_hook.run(sql)
        logger.info(f"Data copied into Snowflake from S3 key: {processed_data_s3_key}")
        logger.info(f"Load completed successfully")
    
    raw_data = extract_data()
    transform_result = transform_data(raw_data)
    load_data(transform_result)

dag_instance = bike_rental_pipeline()