import sqlite3
import pandas as pd
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

# 환경변수
SQLITE_DB_NAME = "bike_data_raw_2025.db"
CHUNK_SIZE = 1_000_000  # 100만건씩 분할

SNOWFLAKE_USER = os.getenv("SNOWFLAKE_USER")
SNOWFLAKE_PASSWORD = os.getenv("SNOWFLAKE_PASSWORD")
SNOWFLAKE_ACCOUNT = os.getenv("SNOWFLAKE_ACCOUNT")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE")
SNOWFLAKE_DATABASE = os.getenv("SNOWFLAKE_DATABASE")
SNOWFLAKE_SCHEMA = os.getenv("SNOWFLAKE_SCHEMA")

stage_name = f"{SNOWFLAKE_DATABASE}.{SNOWFLAKE_SCHEMA}.bike_stage"

# SQLite → pandas 분할 읽기 및 parquet 분할 저장
def extract_and_transform():
    conn_sqlite = sqlite3.connect(SQLITE_DB_NAME)
    query = "SELECT * FROM bike_rent"
    chunk_iter = pd.read_sql_query(query, conn_sqlite, chunksize=CHUNK_SIZE)

    for idx, chunk in enumerate(chunk_iter, 1):
        print(f"[{idx}] chunk 시작: {len(chunk)} rows")

        df_snowflake = pd.DataFrame({
            "USAGE_DATE": pd.to_datetime(chunk["RENT_DT"]).dt.date,
            "USAGE_TIME": chunk["RENT_HR"].astype(int),
            "STATION_ID": chunk["RENT_ID"],
            "STATION_NAME": chunk["RENT_NM"],
            "RENTAL_TYPE_CODE": chunk["RENT_TYPE"],
            "GENDER": chunk["GENDER_CD"],
            "AGE_GROUP": chunk["AGE_TYPE"],
            "USAGE_COUNT": chunk["USE_CNT"].astype(int),
            "CALORIES": pd.to_numeric(chunk["EXER_AMT"], errors='coerce').fillna(0).astype(float).astype(int),
            "CARBON_EMISSION": chunk["CARBON_AMT"].astype(float),
            "DISTANCE": chunk["MOVE_METER"].astype(float),
            "DURATION": chunk["MOVE_TIME"].astype(int)
        })

        parquet_file = f"bike_data_raw_2025_part{idx}.parquet"
        df_snowflake.to_parquet(parquet_file, index=False)
        print(f"[{idx}] parquet 저장 완료: {parquet_file}")

    conn_sqlite.close()

# snowflake 연결 및 parquet 파일들 stage에 업로드
def upload_to_snowflake():
    ctx = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    cs = ctx.cursor()

    try:
        for file in os.listdir():
            if file.startswith("bike_data_raw_2025_part") and file.endswith(".parquet"):
                put_cmd = f"PUT file://{file} @{stage_name} AUTO_COMPRESS=TRUE"
                cs.execute(put_cmd)
                print(f"✅ 업로드 완료: {file}")
    finally:
        cs.close()
        ctx.close()

# snowflake stage에 업로드된 parquet파일들 필터링하여 읽어서 COPY INTO 
def copy_to_snowflake():
    ctx = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    cs = ctx.cursor()

    try:
        copy_cmd = f"""
            COPY INTO BIKE_RENTALS_BY_HOUR
            FROM @{stage_name}
            FILE_FORMAT = (TYPE = 'PARQUET')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;    
        """
        cs.execute(copy_cmd)
        print("✅ Snowflake 적재 완료") 

        # 적재 끝나면 stage cleanup
        cleanup_cmd = f"REMOVE @{stage_name}"
        cs.execute(cleanup_cmd)
        print("✅ Stage cleanup 완료")

    finally:
        cs.close()
        ctx.close()

if __name__ == "__main__":
    extract_and_transform()  # <-- 1단계: 데이터 변환 및 분할 저장
    upload_to_snowflake()    # <-- 2단계: Stage 업로드
    copy_to_snowflake()      # <-- 3단계: COPY INTO + cleanup

