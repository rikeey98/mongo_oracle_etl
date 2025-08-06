#!/bin/bash
# ETL 실행 셸 스크립트 (Airflow에서 호출)

# 스크립트 디렉토리
SCRIPT_DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

# Python 환경 활성화 (virtualenv 사용 시)
# source $PROJECT_DIR/venv/bin/activate

# 환경 변수 설정
export PYTHONPATH=$PROJECT_DIR:$PYTHONPATH

# 로그 디렉토리 생성
LOG_DIR="$PROJECT_DIR/logs/$(date +%Y%m%d)"
mkdir -p $LOG_DIR

# ETL 실행
echo "Starting ETL process at $(date)"
echo "Arguments: $@"

# Python 스크립트 실행
python3 $PROJECT_DIR/scripts/run_etl.py "$@"

# 종료 코드 캡처
EXIT_CODE=$?

echo "ETL process completed at $(date) with exit code: $EXIT_CODE"

# 종료 코드 반환 (Airflow가 성공/실패 판단)
exit $EXIT_CODE