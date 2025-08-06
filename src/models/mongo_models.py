from dataclasses import dataclass, field
from typing import Dict, Any, Optional, List
from datetime import datetime
from bson import ObjectId

from src.models.base import BaseModel

@dataclass
class MongoDocument(BaseModel):
    """MongoDB 문서 기본 모델"""
    
    _id: Optional[ObjectId] = None
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'MongoDocument':
        """MongoDB 문서에서 모델 생성"""
        if '_id' in data and not isinstance(data['_id'], ObjectId):
            data['_id'] = ObjectId(data['_id'])
        return cls(**data)
    
    def to_mongo_dict(self) -> Dict[str, Any]:
        """MongoDB 저장용 딕셔너리 변환"""
        data = self.to_dict()
        # datetime 객체 처리
        for key, value in data.items():
            if isinstance(value, datetime):
                data[key] = value
        return data

@dataclass
class SampleDocument(MongoDocument):
    """샘플 MongoDB 문서 모델"""
    
    name: str = ""
    category: str = ""
    status: str = "pending"
    metadata: Dict[str, Any] = field(default_factory=dict)
    tags: List[str] = field(default_factory=list)
    processed: bool = False
    
    def validate(self) -> bool:
        """데이터 검증"""
        if not self.name:
            raise ValueError("Name is required")
        if self.status not in ['pending', 'processing', 'completed', 'failed']:
            raise ValueError(f"Invalid status: {self.status}")
        return True