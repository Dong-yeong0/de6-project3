# Airflow Docker Compose 설치 가이드

> 참조: Airflow 공식 Docker 문서

## Airflow Docker 설치 단계

1. Docker Compose 파일 다운로드

    * Linux/Mac 사용자

        ```shell
        curl -LfO 'https://airflow.apache.org/docs/apache-airflow/3.0.1/docker-compose.yaml'
        ```

    * Windows 사용자 (PowerShell)

        ```powershell
        Invoke-WebRequest -Uri https://airflow.apache.org/docs/apache-airflow/3.0.1/docker-compose.yaml -OutFile 'docker-compose.yaml'
        ```

2. 환경 설정

    ```bash
    # 필요한 디렉토리 생성
    mkdir -p ./dags ./logs ./plugins ./config
    
    # JWT 시크릿 키 생성 및 환경 변수 설정
    echo -e "AIRFLOW_UID=$(id -u)\nJWT_SECRET=$(openssl rand -hex 32)" > .env
    ```

    > Windows

    ```powershell
    # 필요한 디렉토리 생성
    New-Item -ItemType Directory -Force -Path "./dags", "./logs", "./plugins", "./config"

    # JWT 시크릿 키 생성 및 환경 변수 설정
    $AIRFLOW_UID = 1000
    $JWT_SECRET = -join ((48..57) + (65..70) | Get-Random -Count 32 | % { [char]$_ })
    "AIRFLOW_UID=1000`nJWT_SECRET=$JWT_SECRET" | Out-File -FilePath .env -Encoding utf8
    ```

    ```bash
    # docker-compose.yaml 파일 수정
    vim docker-compose.yaml
    ```

    `docker-compose.yml`에서 다음 설정을 확인/수정하세요

    ```yaml
    environment:
        &airflow-common-env
        AIRFLOW__CORE__LOAD_EXAMPLES: 'false'  # 예제 DAG 비활성화
        AIRFLOW__API_AUTH__JWT_SECRET: ${JWT_SECRET}  # JWT 인증을 위한 시크릿 키
    ```

3. Airflow 초기화

    ```bash
    docker compose up airflow-init
    ```

4. Airflow 서비스 시작

    ```bash
    docker compose up -d
    ```

5. Airflow 웹 인터페이스 접속
    서비스가 정상적으로 시작된 후, 웹 브라우저에서 [localhost:8080](http://localhost:8080)으로 접속하세요.

    * 기본 계정: `airflow`
    * 기본 비밀번호: `airflow`
