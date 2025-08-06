from dataclasses import dataclass
from typing import Optional
from datetime import datetime
from sqlalchemy import Column, String, Integer, DateTime, Boolean, JSON
from sqlalchemy.ext.declarative import declarative_base

from src.models.base import BaseModel

Base = declarative_base()

class OracleBaseModel(Base):
    """Oracle 테이블 기본 모델"""
    __abstract__ = True
    
    created_at = Column(DateTime, default=datetime.now)
    updated_at = Column(DateTime, default=datetime.now, onupdate=datetime.now)

class SampleTable(OracleBaseModel):
    """샘플 Oracle 테이블 모델"""
    __tablename__ = 'sample_table'
    
    id = Column(Integer, primary_key=True)
    mongo_id = Column(String(24), unique=True, nullable=False)
    name = Column(String(255), nullable=False)
    category = Column(String(100))
    status = Column(String(20), default='pending')
    metadata = Column(JSON)
    processed = Column(Boolean, default=False)
    
    def to_dict(self):
        return {
            'id': self.id,
            'mongo_id': self.mongo_id,
            'name': self.name,
            'category': self.category,
            'status': self.status,
            'metadata': self.metadata,
            'processed': self.processed,
            'created_at': self.created_at,
            'updated_at': self.updated_at
        }
