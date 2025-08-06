from abc import ABC, abstractmethod
from typing import Dict, Any, Optional, List
from datetime import datetime
from dataclasses import dataclass, field, asdict

@dataclass
class BaseModel(ABC):
    """기본 모델 클래스"""
    
    created_at: Optional[datetime] = field(default_factory=datetime.now)
    updated_at: Optional[datetime] = field(default_factory=datetime.now)
    
    def to_dict(self) -> Dict[str, Any]:
        """모델을 딕셔너리로 변환"""
        return asdict(self)
    
    @classmethod
    @abstractmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'BaseModel':
        """딕셔너리에서 모델 생성"""
        pass
    
    def validate(self) -> bool:
        """데이터 검증"""
        return True
