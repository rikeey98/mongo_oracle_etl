from typing import Dict, Any, List, Optional, Callable
from datetime import datetime
import json

from src.models.mongo_models import SampleDocument
from src.models.oracle_models import SampleTable
from src.exceptions.custom_exceptions import TransformationError, ValidationError, MappingError
from config.logging_config import etl_logger

class DataTransformer:
    """데이터 변환기"""
    
    def __init__(self):
        self.logger = etl_logger
        self.transformation_rules = {}
        self.validators = {}
    
    def register_rule(self, field_name: str, transform_func: Callable):
        """변환 규칙 등록"""
        self.transformation_rules[field_name] = transform_func
    
    def register_validator(self, field_name: str, validator_func: Callable):
        """검증 규칙 등록"""
        self.validators[field_name] = validator_func
    
    def transform_document_to_record(
        self,
        mongo_doc: Dict[str, Any],
        mapping: Optional[Dict[str, str]] = None
    ) -> Dict[str, Any]:
        """
        MongoDB 문서를 Oracle 레코드로 변환
        
        Args:
            mongo_doc: MongoDB 문서
            mapping: 필드 매핑 딕셔너리
        
        Returns:
            Oracle 레코드용 딕셔너리
        """
        try:
            # 기본 매핑
            default_mapping = {
                '_id': 'mongo_id',
                'name': 'name',
                'category': 'category',
                'status': 'status',
                'metadata': 'metadata',
                'processed': 'processed'
            }
            
            mapping = mapping or default_mapping
            oracle_record = {}
            
            # 필드 매핑 및 변환
            for mongo_field, oracle_field in mapping.items():
                if mongo_field in mongo_doc:
                    value = mongo_doc[mongo_field]
                    
                    # ObjectId 처리
                    if mongo_field == '_id':
                        value = str(value)
                    
                    # 변환 규칙 적용
                    if oracle_field in self.transformation_rules:
                        value = self.transformation_rules[oracle_field](value)
                    
                    # 검증
                    if oracle_field in self.validators:
                        if not self.validators[oracle_field](value):
                            raise ValidationError(f"Validation failed for field: {oracle_field}")
                    
                    oracle_record[oracle_field] = value
            
            # metadata JSON 직렬화
            if 'metadata' in oracle_record and isinstance(oracle_record['metadata'], dict):
                oracle_record['metadata'] = json.dumps(oracle_record['metadata'])
            
            self.logger.debug(f"Transformed document {mongo_doc.get('_id')} to Oracle record")
            return oracle_record
            
        except Exception as e:
            self.logger.error(f"Transformation failed: {e}")
            raise TransformationError(f"Failed to transform document: {e}")
    
    def transform_batch(
        self,
        documents: List[Dict[str, Any]],
        mapping: Optional[Dict[str, str]] = None
    ) -> List[Dict[str, Any]]:
        """배치 변환"""
        transformed = []
        errors = []
        
        for doc in documents:
            try:
                record = self.transform_document_to_record(doc, mapping)
                transformed.append(record)
            except Exception as e:
                errors.append({
                    'document_id': str(doc.get('_id')),
                    'error': str(e)
                })
        
        if errors:
            self.logger.warning(f"Transformation errors: {len(errors)} documents failed")
            # 에러를 로깅하되 성공한 것들은 계속 처리
        
        self.logger.info(f"Transformed {len(transformed)} documents successfully")
        return transformed
    
    def apply_business_rules(self, record: Dict[str, Any]) -> Dict[str, Any]:
        """비즈니스 규칙 적용"""
        # 예시: 상태에 따른 처리
        if record.get('status') == 'pending':
            record['status'] = 'processing'
        
        # 예시: 카테고리별 추가 필드
        if record.get('category') == 'premium':
            record['priority'] = 'high'
        else:
            record['priority'] = 'normal'
        
        return record
    
    def enrich_data(self, record: Dict[str, Any], external_data: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """데이터 보강"""
        if external_data:
            record.update(external_data)
        
        # 타임스탬프 추가
        record['etl_timestamp'] = datetime.now()
        
        return record