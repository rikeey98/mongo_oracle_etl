import os
from pathlib import Path
from typing import Dict, Any
from dataclasses import dataclass
from dotenv import load_dotenv

# .env 파일 로드
load_dotenv()

@dataclass
class MongoDBConfig:
    """MongoDB 연결 설정"""
    host: str
    port: int
    database: str
    username: str
    password: str
    auth_source: str = 'admin'
    
    @property
    def connection_string(self) -> str:
        if self.username and self.password:
            return f"mongodb://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}?authSource={self.auth_source}"
        return f"mongodb://{self.host}:{self.port}/{self.database}"

@dataclass
class OracleDBConfig:
    """Oracle DB 연결 설정"""
    host: str
    port: int
    service_name: str
    username: str
    password: str
    
    @property
    def dsn(self) -> str:
        return f"{self.host}:{self.port}/{self.service_name}"

@dataclass
class ETLConfig:
    """ETL 실행 설정"""
    batch_size: int = 1000
    max_retries: int = 3
    retry_delay: int = 5  # seconds
    parallel_workers: int = 4
    job_collection: str = 'etl_jobs'
    
class Settings:
    """전체 애플리케이션 설정"""
    
    # 프로젝트 경로
    BASE_DIR = Path(__file__).resolve().parent.parent
    LOG_DIR = BASE_DIR / 'logs'
    
    # 환경 설정
    ENV = os.getenv('ENV', 'development')
    DEBUG = os.getenv('DEBUG', 'True').lower() == 'true'
    
    # MongoDB 설정
    MONGODB = MongoDBConfig(
        host=os.getenv('MONGO_HOST', 'localhost'),
        port=int(os.getenv('MONGO_PORT', 27017)),
        database=os.getenv('MONGO_DATABASE', 'source_db'),
        username=os.getenv('MONGO_USERNAME', ''),
        password=os.getenv('MONGO_PASSWORD', ''),
        auth_source=os.getenv('MONGO_AUTH_SOURCE', 'admin')
    )
    
    # Oracle 설정
    ORACLE = OracleDBConfig(
        host=os.getenv('ORACLE_HOST', 'localhost'),
        port=int(os.getenv('ORACLE_PORT', 1521)),
        service_name=os.getenv('ORACLE_SERVICE', 'ORCL'),
        username=os.getenv('ORACLE_USERNAME', ''),
        password=os.getenv('ORACLE_PASSWORD', '')
    )
    
    # ETL 설정
    ETL = ETLConfig(
        batch_size=int(os.getenv('ETL_BATCH_SIZE', 1000)),
        max_retries=int(os.getenv('ETL_MAX_RETRIES', 3)),
        retry_delay=int(os.getenv('ETL_RETRY_DELAY', 5)),
        parallel_workers=int(os.getenv('ETL_WORKERS', 4)),
        job_collection=os.getenv('JOB_COLLECTION', 'etl_jobs')
    )
    
    # Aggregation Pipeline 설정
    PIPELINE_CONFIGS: Dict[str, Any] = {
        'default_limit': 10000,
        'timeout_ms': 30000,
        'allow_disk_use': True
    }
    
    @classmethod
    def get_config(cls) -> 'Settings':
        """설정 인스턴스 반환"""
        return cls()

settings = Settings()