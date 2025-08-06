from typing import Optional
from contextlib import contextmanager
import pymongo
from pymongo import MongoClient
import cx_Oracle
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy.pool import NullPool

from config.settings import settings

class MongoDBConnection:
    """MongoDB 연결 관리"""
    
    def __init__(self):
        self._client: Optional[MongoClient] = None
        self._database = None
    
    def connect(self) -> pymongo.database.Database:
        """MongoDB 연결"""
        if not self._client:
            self._client = MongoClient(
                settings.MONGODB.connection_string,
                serverSelectionTimeoutMS=5000,
                connectTimeoutMS=10000,
                maxPoolSize=50
            )
            self._database = self._client[settings.MONGODB.database]
        return self._database
    
    def close(self):
        """연결 종료"""
        if self._client:
            self._client.close()
            self._client = None
            self._database = None
    
    @contextmanager
    def get_session(self):
        """MongoDB 세션 컨텍스트 매니저"""
        session = self._client.start_session()
        try:
            yield session
        finally:
            session.end_session()

class OracleDBConnection:
    """Oracle DB 연결 관리"""
    
    def __init__(self):
        self._engine = None
        self._session_factory = None
    
    def connect(self):
        """Oracle DB 연결"""
        if not self._engine:
            # cx_Oracle 초기화
            cx_Oracle.init_oracle_client()
            
            # SQLAlchemy 엔진 생성
            connection_string = (
                f"oracle+cx_oracle://{settings.ORACLE.username}:"
                f"{settings.ORACLE.password}@{settings.ORACLE.dsn}"
            )
            
            self._engine = create_engine(
                connection_string,
                poolclass=NullPool,  # Airflow와 호환성
                echo=settings.DEBUG
            )
            
            self._session_factory = sessionmaker(bind=self._engine)
    
    @contextmanager
    def get_session(self) -> Session:
        """Oracle 세션 컨텍스트 매니저"""
        if not self._session_factory:
            self.connect()
        
        session = self._session_factory()
        try:
            yield session
            session.commit()
        except Exception:
            session.rollback()
            raise
        finally:
            session.close()
    
    def close(self):
        """연결 종료"""
        if self._engine:
            self._engine.dispose()
            self._engine = None
            self._session_factory = None

# 싱글톤 인스턴스
mongo_connection = MongoDBConnection()
oracle_connection = OracleDBConnection()