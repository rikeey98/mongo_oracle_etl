# MongoDB to Oracle ETL Pipeline

MongoDB에서 Oracle Database로 데이터를 추출, 변환, 적재하는 강력하고 확장 가능한 ETL 파이프라인입니다.

## 📋 주요 특징

- **MongoDB Aggregation Pipeline 활용**: 효율적인 데이터 추출
- **Repository 패턴**: 깔끔한 데이터 접근 계층
- **강력한 에러 처리**: 계층별 커스텀 예외 및 재시도 메커니즘
- **Job 관리 시스템**: MongoDB 기반 Job 추적 및 모니터링
- **병렬 처리**: ThreadPoolExecutor를 통한 고성능 처리
- **Airflow 통합**: 스케줄링 및 오케스트레이션
- **포괄적인 로깅**: 날짜별 로그 파일 관리
- **단위 테스트**: 높은 테스트 커버리지

## 🚀 빠른 시작

### 사전 요구사항

- Python 3.8+
- MongoDB 4.4+
- Oracle Database 12c+
- Oracle Instant Client (cx_Oracle용)

### 설치

1. 저장소 클론:
```bash
git clone https://github.com/yourusername/mongo-oracle-etl.git
cd mongo-oracle-etl
```

2. 가상 환경 생성 및 활성화:
```bash
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
```

3. 의존성 설치:
```bash
pip install -r requirements.txt
pip install -e .
```

4. 환경 변수 설정:
```bash
cp .env.example .env
# .env 파일을 편집하여 데이터베이스 연결 정보 설정
```

### 실행

#### 기본 실행
```bash
python scripts/run_etl.py \
    --source-collection sample_collection \
    --mode incremental
```

#### 날짜 범위 지정
```bash
python scripts/run_etl.py \
    --source-collection sample_collection \
    --mode incremental \
    --date-from 2024-01-01 \
    --date-to 2024-01-31
```

#### Dry Run (실제 적재 없이 테스트)
```bash
python scripts/run_etl.py \
    --source-collection sample_collection \
    --mode full \
    --dry-run
```

## 📁 프로젝트 구조

```
mongo_oracle_etl/
├── config/                 # 설정 관리
│   ├── settings.py        # 환경 설정
│   ├── database.py        # DB 연결 관리
│   └── logging_config.py  # 로깅 설정
├── src/
│   ├── models/            # 데이터 모델
│   ├── repositories/      # Repository 패턴 구현
│   ├── transformers/      # 데이터 변환 로직
│   ├── etl/              # ETL 핵심 로직
│   ├── jobs/             # Job 관리
│   ├── exceptions/       # 커스텀 예외
│   └── utils/            # 유틸리티
├── tests/                # 테스트 코드
├── logs/                 # 로그 파일
└── scripts/              # 실행 스크립트
```

## 🔧 설정

### MongoDB Aggregation Pipeline

Pipeline을 동적으로 구성할 수 있습니다:

```python
from src.etl.extractor import MongoExtractor

extractor = MongoExtractor('collection_name')
pipeline = extractor.build_pipeline(
    match_filter={'status': 'active'},
    project_fields={'name': 1, 'value': 1},
    sort_by={'created_at': -1},
    limit=1000
)
```

### 커스텀 변환 규칙

비즈니스 로직에 맞는 변환 규칙을 정의할 수 있습니다:

```python
from src.transformers.data_transformer import DataTransformer

transformer = DataTransformer()

# 변환 규칙 등록
transformer.register_rule('name', lambda x: x.upper())

# 검증 규칙 등록
transformer.register_validator('age', lambda x: 0 < x < 120)
```

### Job 추적

모든 ETL 실행은 자동으로 추적됩니다:

```python
from src.jobs.job_tracker import JobTracker

tracker = JobTracker()

# 최근 Job 조회
recent_jobs = tracker.get_recent_jobs(
    job_type='etl_sample',
    status='completed',
    limit=10
)

# Job 상태 확인
job_status = tracker.get_job_status('job_id_123')
```

## 🧪 테스트

### 단위 테스트 실행
```bash
pytest tests/unit/ -v
```

### 통합 테스트 실행
```bash
pytest tests/integration/ -v
```

### 커버리지 리포트 생성
```bash
pytest tests/ --cov=src --cov-report=html
```

## 🚁 Airflow 통합

Airflow DAG 예시:

```python
from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

default_args = {
    'owner': 'data-team',
    'retries': 2,
    'retry_delay': timedelta(minutes=5)
}

dag = DAG(
    'mongodb_to_oracle_etl',
    default_args=default_args,
    schedule_interval='0 2 * * *',
    start_date=days_ago(1)
)

etl_task = BashOperator(
    task_id='run_etl',
    bash_command='/path/to/scripts/run_etl.sh --mode incremental',
    dag=dag
)
```

## 🐳 Docker 사용

### 개발 환경 구축
```bash
docker-compose up -d
```

### ETL 실행
```bash
docker-compose run etl-app python scripts/run_etl.py \
    --source-collection sample_collection \
    --mode incremental
```

## 📊 모니터링

### 로그 확인
로그는 `logs/YYYYMMDD/` 디렉토리에 일별로 저장됩니다:
```bash
tail -f logs/$(date +%Y%m%d)/etl.log
```

### Job 상태 모니터링
MongoDB의 Job 컬렉션을 통해 실시간 모니터링:
```javascript
// MongoDB Shell
db.etl_jobs.find({
    status: "running"
}).sort({ created_at: -1 })
```

## 🔐 보안

- 환경 변수를 통한 민감한 정보 관리
- `.env` 파일은 절대 커밋하지 않음
- 프로덕션 환경에서는 시크릿 관리 도구 사용 권장

## 🤝 기여

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 라이선스

이 프로젝트는 MIT 라이선스 하에 배포됩니다. 자세한 내용은 [LICENSE](LICENSE) 파일을 참조하세요.

## 🛠️ 트러블슈팅

### Oracle 연결 문제
```bash
# Oracle Instant Client 설치 확인
python -c "import cx_Oracle; print(cx_Oracle.version)"

# 환경 변수 설정
export LD_LIBRARY_PATH=/opt/oracle/instantclient:$LD_LIBRARY_PATH
```

### MongoDB 연결 타임아웃
```python
# config/settings.py에서 타임아웃 설정 조정
MONGODB = MongoDBConfig(
    # ...
    connection_timeout=10000,  # 10초
    server_selection_timeout=5000  # 5초
)
```

### 메모리 부족 문제
```python
# 배치 크기 조정
ETL_BATCH_SIZE=500  # 기본값 1000에서 감소
ETL_WORKERS=2  # 병렬 워커 수 감소
```

## 📈 성능 최적화

### 1. MongoDB 인덱스 최적화
```javascript
// 자주 사용되는 필드에 인덱스 생성
db.collection.createIndex({ "created_at": 1, "status": 1 })
db.collection.createIndex({ "_id": 1, "processed": 1 })
```

### 2. Oracle 벌크 작업
```python
# 단일 삽입 대신 벌크 삽입 사용
loader.load_batch(records, batch_size=1000)
```

### 3. 병렬 처리 튜닝
```python
# CPU 코어 수에 따라 워커 수 조정
import multiprocessing
ETL_WORKERS = multiprocessing.cpu_count()
```

## 🎯 사용 사례

### 1. 일일 증분 ETL
```bash
# Crontab 설정
0 2 * * * cd /path/to/project && ./scripts/run_etl.sh --mode incremental
```

### 2. 전체 데이터 마이그레이션
```bash
python scripts/run_etl.py \
    --source-collection all_data \
    --mode full \
    --batch-size 5000
```

### 3. 특정 조건 데이터 처리
```python
# 커스텀 파이프라인 생성
pipeline = [
    {'$match': {
        'category': 'premium',
        'value': {'$gte': 1000}
    }},
    {'$lookup': {
        'from': 'users',
        'localField': 'user_id',
        'foreignField': '_id',
        'as': 'user_info'
    }}
]
```

## 🔄 업데이트 내역

### v1.0.0 (2024-01-15)
- 초기 릴리스
- MongoDB에서 Oracle로의 기본 ETL 기능
- Job 추적 시스템
- Airflow 통합

### 계획된 기능
- [ ] 실시간 스트리밍 ETL (Change Streams)
- [ ] 웹 기반 모니터링 대시보드
- [ ] 더 많은 데이터 소스 지원 (PostgreSQL, MySQL)
- [ ] 데이터 품질 검증 프레임워크
- [ ] 자동 스키마 마이그레이션

## 📞 지원

- 이슈 리포트: [GitHub Issues](https://github.com/yourusername/mongo-oracle-etl/issues)
- 이메일: support@example.com
- 문서: [Wiki](https://github.com/yourusername/mongo-oracle-etl/wiki)

## 👥 팀

- 프로젝트 리드: Your Name
- 개발팀: Data Engineering Team

---

**Note**: 이 프로젝트는 지속적으로 개발 중입니다. 피드백과 기여를 환영합니다!