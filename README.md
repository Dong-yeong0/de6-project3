<img src="https://capsule-render.vercel.app/api?type=wave&color=5cbcff&height=150&section=header&text=&fontSize=50&fontColor=ffffff" />

# 🚌 서울시 대중교통 & 공공자전거 이용량 파이프라인 구축 🚌

### 🚇 프로젝트 개요
서울특별시의 지하철, 버스, 공공자전거(따릉이) 데이터를 수집·분석하고, 시각화를 통해 유의미한 정보를 제공하는 데이터 파이프라인 구축 프로젝트입니다. 시간대별·지역별 이용 현황을 분석하여 교통 혼잡 해소 및 효율적인 교통 정책 수립의 기반을 마련합니다.

---

### 🛠️ 활용 기술 및 프레임워크
- **Language**: <img src="https://img.shields.io/badge/python-3670A0?style=for-the-badge&logo=python&logoColor=ffdd54"/> <img src="https://img.shields.io/badge/mysql-4479A1.svg?style=for-the-badge&logo=mysql&logoColor=white"/>
- **Data Warehouse & Storage**: <img src="https://img.shields.io/badge/Amazon%20S3-FF9900?style=for-the-badge&logo=amazons3&logoColor=white"/> <img src="https://img.shields.io/badge/snowflake-%2329B5E8.svg?style=for-the-badge&logo=snowflake&logoColor=white">
- **Workflow Management & Modeling**: <img src="https://img.shields.io/badge/Apache%20Airflow-017CEE?style=for-the-badge&logo=Apache%20Airflow&logoColor=white"> <img src="https://img.shields.io/badge/dbt-FF694B?style=for-the-badge&logo=dbt&logoColor=white">
- **Visualization**: <img src="https://img.shields.io/badge/apachesuperset-20A6C9?style=for-the-badge&logo=apachesuperset&logoColor=white">
- **DevOps**: <img src="https://img.shields.io/badge/docker-%230db7ed.svg?style=for-the-badge&logo=docker&logoColor=white"> <img src="https://img.shields.io/badge/github%20actions-%232671E5.svg?style=for-the-badge&logo=githubactions&logoColor=white">

---
### 📌 Team 7UP

| 팀원 | 담당 | 주요 작업 내용 |
| --- | --- | --- |
| **👤 정동영** | - 프로젝트 총괄<br>- ETL<br>- DBT<br>- PPT 제작 | - 협업 환경 세팅<br>- 데이터 모델링<br>- 공공자전거 ETL 파이프라인 구축<br>- ELT 파이프라인 설계 및 구현<br>- DBT 환경 설정 및 관리 |
| **👤 이승아** | - ETL<br>- Visualization<br>- 보고서 | - 서울시 공공자전거 2025년 데이터 적재<br>- 공공자전거 API 수집 파이프라인 구축<br>- 공공자전거 데이터 시각화 |
| **👤 김범준** | - ETL<br>- DBT<br>- 보고서 | - 서울시 3종 대중교통 위치 데이터 적재<br>- 대중교통 위치 데이터 API 수집 파이프라인 구축 |
| **👤 정준** | - ETL<br>- Visualization<br>- 발표 | - 서울시 버스 데이터 적재<br>- 버스 API 수집 파이프라인 구축<br>- 버스 데이터 시각화 |
| **👤 최시은** | - ETL<br>- Visualization<br>- PPT | - 서울시 지하철 데이터 적재<br>- 지하철 API 수집 파이프라인 구축<br>- 지하철 데이터 시각화 |


---  

### 📊 데이터 수집 및 처리 프로세스
1. **데이터 수집**
   - 서울시 Open API를 활용한 지하철, 버스, 자전거 데이터 수집
   - JSON, XML 형식 원본 데이터를 S3에 저장

2. **데이터 전처리 및 변환**
   - 날짜 포맷 통일, 필요 컬럼 추출, Parquet 형식으로 변환하여 저장

3. **데이터 적재**
   - Snowflake Storage Integration 및 Stage 기능을 통해 데이터 적재
   - 멱등성을 보장하는 데이터 로드 방식 적용 (logical_date 기반 COPY INTO)

4. **DBT 기반 모델링 (Dimension & Fact Tables)**
   - `dim_user`, `dim_station`, `dim_date` Dimension 테이블 생성
   - 이용량 기반 Fact 테이블 (`fact_bike_usage`, `fact_bus_usage`, `fact_subway_usage`) 관리

---

### 🔄 협업 환경 설정 및 자동화
- **GitHub Branch Protection 설정**
  - 메인 브랜치: 모든 팀원의 리뷰 및 팀장 승인 필수
  - 개발 브랜치: 팀장 승인 필수, 빠른 개발 및 리뷰 프로세스 구축

- **GitHub Actions를 통한 PR 자동화**
  - PR 생성 시 작성자 자동 Assignee 설정으로 효율적인 협업

---

### 🎨 시각화 결과
- **자전거**
  ![image](https://github.com/user-attachments/assets/7bc0fa9e-8474-40ba-a777-dfac65112cfd)
  ![image](https://github.com/user-attachments/assets/015a8775-a96d-447f-9f95-f821816010b3)
  ![image](https://github.com/user-attachments/assets/041d2faa-377b-48e6-b60c-17f8a6f770e6)

- **버스**

- **지하철**
  ![image](https://github.com/user-attachments/assets/61586ed4-492a-4237-8233-4746ac9816a2)
  ![image](https://github.com/user-attachments/assets/b6b9d384-0ff4-4991-894d-67430c0da297)

---

### 🚀 프로젝트 주요 성과
- Airflow와 DBT를 활용한 자동화된 ETL 및 ELT 데이터 파이프라인 구축
- Snowflake 활용으로 빠르고 안정적인 데이터 분석 환경 구축
- GitHub을 통한 효율적인 팀 협업 프로세스 확립
- Preset 기반 대시보드를 통해 직관적이고 시각적인 분석 결과 제공

---

<img src="https://capsule-render.vercel.app/api?type=wave&color=5cbcff&height=150&section=footer&text=7UP&fontSize=30&fontColor=ffffff" />
