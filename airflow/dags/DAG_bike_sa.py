import pendulum
import json
import pandas as pd
import pyarrow.parquet as pq
import io
import time
import requests
from datetime import timedelta
from airflow import DAG
from airflow.decorators import dag, task
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

@dag(
    dag_id="DAG_bike_sa",
    start_date=pendulum.datetime(2025, 6, 1, tz="UTC"),
    schedule="0 0 * * *",  # 한국 시간 오전 9시
    catchup=False,
    max_active_runs=1,
    default_args=default_args
)

def bike_dag():

    @task()
    def bike_collector(logical_date=None):
        # -------------------------------------------------
        # ① 환경 변수 및 S3 연결 초기화
        # -> ../config/connections.json에 관련 연결 정의
        # -> ../config/variables.json에 발급받은 API 키 값 정의
        # -------------------------------------------------
        API_KEY = Variable.get("SEOUL_API_KEY")
        s3_hook = S3Hook(aws_conn_id="aws_default")
        s3_conn = s3_hook.get_connection(s3_hook.aws_conn_id)
        bucket_name = json.loads(s3_conn.extra).get("bucket_name", "de6-team7")

        # -------------------------------------------------
        # ② 실행 날짜 계산 (어제 날짜 수집)
        # -> 한국 시간 기준 오전 9시에 실행
        # -------------------------------------------------
        execution_date = logical_date or pendulum.now("UTC")
        target_date = (execution_date - timedelta(days=1)).strftime("%Y%m%d")


        # -------------------------------------------------
        # ③ API로부터 전체 데이터 건수 조회 함수
        # -> get_all_rows 실행 시, total 인자에 전달
        # -------------------------------------------------
        def get_total_count(api_key, date, hour):
            base_url = "http://openapi.seoul.go.kr:8088"
            service = "tbCycleRentUseTimeInfo"
            url = f"{base_url}/{api_key}/json/{service}/1/1/{date}/{hour}"
            response = requests.get(url)
            response.raise_for_status()
            data = response.json()
            total_count = int(data['cycleRentUseTimeInfo']['list_total_count'])
            return total_count


        # -------------------------------------------------
        # ④ 전체 데이터를 페이징하여 조회하는 함수
        # -> 한 번에 1000건을 넘지 못하기 때문에, 1000건 씩 페이징이 필요 
        # -------------------------------------------------
        def get_all_rows(api_key, date, hour, total_count, page_size=1000):
            base_url = "http://openapi.seoul.go.kr:8088"
            service = "tbCycleRentUseTimeInfo"
            rows = []
            for start in range(1, total_count + 1, page_size):
                end = min(start + page_size - 1, total_count)
                url = f"{base_url}/{api_key}/json/{service}/{start}/{end}/{date}/{hour}"
                response = requests.get(url)
                response.raise_for_status()
                data = response.json()
                rows.extend(data['cycleRentUseTimeInfo']['row'])
                time.sleep(0.01)
            return rows

        # -------------------------------------------------
        # ⑤ 24시간 루프 돌며 각 시간대별 데이터 수집 및 raw json 파일 S3 저장
        # -------------------------------------------------
        for hour in range(24):
            try:
                # ⑤-1. 전체 건수 조회
                total_count = get_total_count(API_KEY, target_date, hour)

                # ⑤-2. 데이터 조회
                rows = get_all_rows(API_KEY, target_date, hour, total_count)

                # ⑤-3. S3 저장 경로 설정
                yyyy = target_date[:4]
                mm = target_date[4:6]
                dd = target_date[6:8]
                HH = f"{hour:02d}"
                s3_path = f"raw_data/bike/sa/{yyyy}/{mm}/{dd}/{HH}/data.json"

                # ⑤-4. S3 업로드
                json_data = json.dumps(rows, ensure_ascii=False, indent=4)
                s3_hook.load_string(
                    string_data=json_data,
                    key=s3_path,
                    bucket_name=bucket_name,
                    replace=True
                )
                print(f"✅ {s3_path} 업로드 성공")
                time.sleep(0.1)

            except Exception as e:
                print(f"❌ {hour}시 데이터 실패: {e}")

    @task()
    def bike_processor(logical_date=None):
        # -------------------------------------------------
        # ① S3 연결 및 변수 초기화
        # -------------------------------------------------
        s3_hook = S3Hook(aws_conn_id="aws_default")
        s3_conn = s3_hook.get_connection(s3_hook.aws_conn_id)
        bucket_name = json.loads(s3_conn.extra).get("bucket_name", "de6-team7")

        # -------------------------------------------------
        # ② 처리할 날짜 계산 (어제 날짜 기준)
        # -------------------------------------------------
        execution_date = logical_date or pendulum.now("UTC")
        target_date = (execution_date - timedelta(days=1)).strftime("%Y%m%d")

        yyyy = target_date[:4]
        mm = target_date[4:6]
        dd = target_date[6:8]

        raw_data = []

        # -------------------------------------------------
        # ③ 24시간 동안 저장된 원본(raw) 데이터를 S3에서 불러와서 append
        # -------------------------------------------------
        for hour in range(24):
            HH = f"{hour:02d}"
            s3_path = f"raw_data/bike/sa/{yyyy}/{mm}/{dd}/{HH}/data.json"
            try:
                # ③-1. S3에서 해당 시간대의 파일 가져오기
                s3_obj = s3_hook.get_key(
                    key=s3_path, 
                    bucket_name=bucket_name
                    )
                file_content = s3_obj.get()['Body'].read().decode('utf-8')
                json_data = json.loads(file_content)

                # ③-2. JSON → DataFrame 변환
                df = pd.DataFrame(json_data)
                raw_data.append(df)
                print(f"✅ {s3_path} 불러오기 성공")
            except Exception as e:
                print(f"❌ {s3_path} 불러오기 실패: {e}")

        # -------------------------------------------------
        # ④ 데이터 병합 및 전처리 
        # -> 앞서 취합한 데이터에 대한 전처리 진행 
        # -------------------------------------------------
        if raw_data:
            # ④-1. 24시간 데이터를 하나로 병합
            merged_df = pd.concat(raw_data, ignore_index=True)

            # ④-2. 데이터 컬럼 변환 및 정제
            transformed_df = pd.DataFrame({
                "USAGE_DATE": pd.to_datetime(merged_df["RENT_DT"]).dt.date,
                "USAGE_TIME": merged_df["RENT_HR"].astype(int),
                "STATION_ID": merged_df["RENT_ID"],
                "STATION_NAME": merged_df["RENT_NM"],
                "RENTAL_TYPE_CODE": merged_df["RENT_TYPE"],
                "GENDER": merged_df["GENDER_CD"],
                "AGE_GROUP": merged_df["AGE_TYPE"],
                "USAGE_COUNT": merged_df["USE_CNT"].astype(int),
                "CALORIES": pd.to_numeric(merged_df["EXER_AMT"], errors='coerce').fillna(0).astype(float).astype(int),
                "CARBON_EMISSION": merged_df["CARBON_AMT"].astype(float),
                "DISTANCE": merged_df["MOVE_METER"].astype(float),
                "DURATION": merged_df["MOVE_TIME"].astype(int)
            })

            # ④-3. 변환된 데이터 parquet 형식으로 메모리에 저장
            # -> 로컬에 저장하지 않기 위함.
            out_buffer = io.BytesIO()
            transformed_df.to_parquet(out_buffer, index=False)

            # ④-4. 전처리 데이터 S3 저장 경로 설정 (day 기준)
            processed_path = f"processed/bike/sa/{yyyy}/{mm}/{dd}/cleaned.parquet"

            # ④-5. S3에 업로드
            s3_hook.load_bytes(
                bytes_data=out_buffer.getvalue(),
                key=processed_path,
                bucket_name=bucket_name,
                replace=True
            )

            print(f"✅ 전처리 및 저장 완료: {processed_path}")
        else:
            print("❌ 불러온 데이터가 없습니다.")


    @task()
    def bike_loader(logical_date=None):
        # -------------------------------------------------
        # ① 처리할 날짜 계산 (어제 날짜 기준)
        # -> 한국 시간 기준 오전 9시에 실행
        # -------------------------------------------------
        execution_date = logical_date or pendulum.now("UTC")
        target_date = (execution_date - timedelta(days=1)).strftime("%Y%m%d")
        yyyy, mm, dd = target_date[:4], target_date[4:6], target_date[6:8]

        # -------------------------------------------------
        # ② Snowflake 연결 초기화
        # -> ../config/connections.json에 snowflake_conn 연결 정의
        # -------------------------------------------------
        snowflake_hook = SnowflakeHook(snowflake_conn_id="snowflake_conn")
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()

        # -------------------------------------------------
        # ③ S3 Stage 경로 설정
        # -> Snowflake에 사전 생성된 S3_STAGE 활용
        # -------------------------------------------------
        s3_path = f"processed/bike/sa/{yyyy}/{mm}/{dd}/"

        # -------------------------------------------------
        # ④ Snowflake COPY INTO 쿼리 생성
        # -> parquet 컬럼명 자동 매칭을 위해 MATCH_BY_COLUMN_NAME 사용
        # -------------------------------------------------
        copy_query = f"""
        COPY INTO RAW_DATA.BIKE_RENTALS_BY_HOUR_SA
        FROM @RAW_DATA.S3_STAGE/{s3_path}
        FILE_FORMAT = (TYPE = 'PARQUET')
        MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE
        ON_ERROR = CONTINUE
        """

        # -------------------------------------------------
        # ⑤ COPY INTO 실행
        # -------------------------------------------------
        cursor.execute(copy_query)
        cursor.close()

        print("✅ COPY INTO Snowflake 성공")

    # Task 
    extract_task = bike_collector()
    transform_task = bike_processor()
    load_task = bike_loader()

    extract_task >> transform_task >> load_task

dag = bike_dag()
