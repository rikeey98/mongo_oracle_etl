# airflow/dags/etl_dag.py
"""
Airflow DAG 정의
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# 기본 인자
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# DAG 정의
dag = DAG(
    'mongodb_to_oracle_etl',
    default_args=default_args,
    description='MongoDB to Oracle ETL Pipeline',
    schedule_interval='0 2 * * *',  # 매일 새벽 2시
    catchup=False,
    tags=['etl', 'mongodb', 'oracle'],
)

# Task 1: 이전 실패 Job 재시도
retry_failed_jobs = BashOperator(
    task_id='retry_failed_jobs',
    bash_command='/path/to/project/scripts/run_etl.sh --mode retry',
    dag=dag,
)

# Task 2: Incremental ETL 실행
incremental_etl = BashOperator(
    task_id='incremental_etl',
    bash_command='''
        /path/to/project/scripts/run_etl.sh \
        --source-collection sample_collection \
        --target-table sample_table \
        --mode incremental \
        --batch-size 1000
    ''',
    dag=dag,
)

# Task 3: 데이터 검증
def validate_data(**context):
    """데이터 검증 로직"""
    # 여기에 검증 로직 구현
    # 예: Oracle과 MongoDB 데이터 개수 비교
    pass

data_validation = PythonOperator(
    task_id='data_validation',
    python_callable=validate_data,
    dag=dag,
)

# Task 4: 오래된 Job 정리
cleanup_old_jobs = BashOperator(
    task_id='cleanup_old_jobs',
    bash_command='python3 /path/to/project/scripts/cleanup_jobs.py --days 30',
    dag=dag,
)

# Task 의존성 설정
retry_failed_jobs >> incremental_etl >> data_validation >> cleanup_old_jobs