import requests
import time
from datetime import datetime, timedelta
import sqlite3
import sys
from dotenv import load_dotenv
import os


# 환경변수
load_dotenv()
API_KEY = os.getenv("API_KEY")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")

# 슬랙 알림
def send_slack_alert(message, webhook_url):
    payload = {"text": message}
    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
    except Exception as e:
        print(f"슬랙 알림 전송 실패: {e}")

# 1. 전체 row 정보 가져오기
def get_total_count(api_key, date, hour):
    base_url = "http://openapi.seoul.go.kr:8088"
    service = "tbCycleRentUseTimeInfo"
    url = f"{base_url}/{api_key}/json/{service}/1/1/{date}/{hour}"
    response = requests.get(url)
    response.raise_for_status()
    data = response.json()
    total_count = int(data['cycleRentUseTimeInfo']['list_total_count'])
    return total_count

# 2. 전체 rows 가져오기
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
        if 'cycleRentUseTimeInfo' in data and 'row' in data['cycleRentUseTimeInfo']:
            rows.extend(data['cycleRentUseTimeInfo']['row'])
        time.sleep(0.05)
    return rows

# INSERT
def save_rows_to_sqlite(rows, conn, table_name="bike_rent"):
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            RENT_DT TEXT, RENT_HR TEXT, RENT_ID TEXT, RENT_NM TEXT,
            RENT_TYPE TEXT, GENDER_CD TEXT, AGE_TYPE TEXT, USE_CNT TEXT,
            EXER_AMT TEXT, CARBON_AMT TEXT, MOVE_METER TEXT, MOVE_TIME TEXT,
            START_INDEX INTEGER, END_INDEX INTEGER, RNUM TEXT
        )
    """)
    for row in rows:
        cursor.execute(f"""
            INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row.get('RENT_DT'), row.get('RENT_HR'), row.get('RENT_ID'), row.get('RENT_NM'),
            row.get('RENT_TYPE'), row.get('GENDER_CD'), row.get('AGE_TYPE'), row.get('USE_CNT'),
            row.get('EXER_AMT'), row.get('CARBON_AMT'), row.get('MOVE_METER'), row.get('MOVE_TIME'),
            row.get('START_INDEX'), row.get('END_INDEX'), row.get('RNUM')
        ))

# overall
def collect_and_save(api_key, slack_webhook_url=None, db_name="bike_data_raw_2025.db"):
    start_date = datetime(2025, 1, 1)                         # 수집 시작 날짜 ( 만약 특정 날짜에서 호출 시 에러 발생할 경우, 해당 날짜에서 부터 스크립트 재시작 가능하도록 ) 
    end_date = datetime.now() - timedelta(days=1)             # 오늘 날짜 기준 하루 전 ( 당일 데이터가 없는 경우가 존재 )
    days_back = (end_date - start_date).days + 1    

    conn = sqlite3.connect(db_name)

    for delta in range(days_back):
        target_date = (start_date + timedelta(days=delta)).strftime("%Y%m%d")
        try:
            print(f"[{target_date}] 날짜 전체 수집 시작")          # 날짜 단위별 transaction ( api 호출 에러 발생 시 해당 날짜 rollback 및 제어 가능 )
            conn.execute("BEGIN")

            for hour in range(24):
                print(f"  [{target_date} {hour}시] 수집 중...")
                total_count = get_total_count(api_key, target_date, str(hour))
                print(f"  → 총 데이터 수: {total_count}")
                rows = get_all_rows(api_key, target_date, str(hour), total_count)
                if rows:
                    save_rows_to_sqlite(rows, conn)

            conn.commit()
            print(f"[{target_date}] 데이터 커밋 완료")

        except Exception as e:
            conn.rollback()
            print(f"[{target_date}] 에러 발생으로 rollback 수행: {e}")
            if slack_webhook_url:
                send_slack_alert(f"{target_date} 수집 중 에러 발생: {e}", slack_webhook_url)
            conn.close()
            sys.exit(f"실패한 날짜: {target_date} → 프로그램 종료")

        time.sleep(0.1)



if __name__ == "__main__":
    collect_and_save(API_KEY, slack_webhook_url=SLACK_WEBHOOK_URL)
