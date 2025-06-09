from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
import requests
import sqlite3
import sys
import time
import xml.etree.ElementTree as ET


# .env로부터 환경변수 로드
load_dotenv()
API_KEY = os.getenv("API_KEY")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL")


# Slack 알림
def send_slack_alert(message, webhook_url):
    payload = {"text": message}
    try:
        response = requests.post(webhook_url, json=payload)
        response.raise_for_status()
    except Exception as e:
        print(f"슬랙 알림 전송 실패: {e}")


# 해당 날짜의 버스 데이터 XML 전체 row수 반환
def get_total_bus_rows(api_key, use_dt):
    url = f"http://openapi.seoul.go.kr:8088/{api_key}/xml/CardBusStatisticsServiceNew/1/1/{use_dt}"
    response = requests.get(url)
    response.raise_for_status()
    root = ET.fromstring(response.content)
    total_count = root.findtext("list_total_count")
    if total_count is None:
        raise ValueError(f"[{use_dt}] list_total_count 없음 (데이터가 없을 수 있음)")
    return int(total_count)


# 해당 날짜의 버스 데이터 XML 전체 row 수 반환
def get_bus_rows(api_key, use_dt, total_count, page_size=1000):
    rows = []
    for start in range(1, total_count + 1, page_size):
        end = min(start + page_size - 1, total_count)
        url = f"http://openapi.seoul.go.kr:8088/{api_key}/xml/CardBusStatisticsServiceNew/{start}/{end}/{use_dt}/"
        response = requests.get(url)
        response.raise_for_status()
        root = ET.fromstring(response.content)
        items = root.findall("row")
        for row in items:
            record = {child.tag: child.text for child in row}
            rows.append(record)
        time.sleep(0.05)
    return rows


# SQLite에 저장
def save_bus_rows_to_sqlite(rows, conn, table_name="bus_usage"):
    '''
        USE_YMD TEXT        # 사용일자
        RTE_ID TEXT         # 노선ID
        RTE_NO TEXT         # 노선번호
        RTE_NM TEXT         # 노선명
        STOPS_ID TEXT       # 표준버스정류장ID
        STOPS_ARS_NO TEXT   # 버스정류장ARS번호
        SBWY_STNS_NM TEXT   # 역명
        GTON_TNOPE TEXT     # 승차총승객수
        GTOFF_TNOPE TEXT    # 하차총승객수
        REG_YMD TEXT        # 등록일자
    '''
    cursor = conn.cursor()
    cursor.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            USE_YMD TEXT,
            RTE_ID TEXT,
            RTE_NO TEXT,
            RTE_NM TEXT,
            STOPS_ID TEXT,
            STOPS_ARS_NO TEXT,
            SBWY_STNS_NM TEXT,
            GTON_TNOPE TEXT,
            GTOFF_TNOPE TEXT,
            REG_YMD TEXT
        )
    """)
    for row in rows:
        cursor.execute(f"""
            INSERT INTO {table_name} VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            row.get('USE_YMD'),
            row.get('RTE_ID'),
            row.get('RTE_NO'),
            row.get('RTE_NM'),
            row.get('STOPS_ID'),
            row.get('STOPS_ARS_NO'),
            row.get('SBWY_STNS_NM'),
            row.get('GTON_TNOPE'),
            row.get('GTOFF_TNOPE'),
            row.get('REG_YMD')
        ))

# 전체 수집 로직
def collect_and_save_bus_data(api_key, slack_webhook_url=None, db_name="bus_data_raw_2025.db"):
    start_date = datetime(2025, 1, 1)
    end_date = datetime.now() - timedelta(days=1)
    days_back = (end_date - start_date).days + 1

    conn = sqlite3.connect(db_name)

    for delta in range(days_back):
        target_date = (start_date + timedelta(days=delta)).strftime("%Y%m%d")
        try:
            print(f"[{target_date}] 데이터 수집 시작")
            conn.execute("BEGIN")

            total_count = get_total_bus_rows(api_key, target_date)
            print(f"  → 총 {total_count}건")
            rows = get_bus_rows(api_key, target_date, total_count)
            if rows:
                save_bus_rows_to_sqlite(rows, conn)
                print(f"  → {len(rows)}건 저장 완료")

            conn.commit()
            print(f"[{target_date}] 커밋 완료")

        except Exception as e:
            conn.rollback()
            print(f"[{target_date}] 에러 발생 → rollback: {e}")
            if slack_webhook_url:
                send_slack_alert(f"[{target_date}] 버스 데이터 수집 중 오류 발생: {e}", slack_webhook_url)
            conn.close()
            sys.exit(f"❌ 실패한 날짜: {target_date} → 프로그램 종료")

        time.sleep(0.1)


# ✅ 실행 엔트리포인트
if __name__ == "__main__":
    collect_and_save_bus_data(API_KEY, slack_webhook_url=SLACK_WEBHOOK_URL)