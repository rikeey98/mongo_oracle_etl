from typing import Dict, Any, List, Optional
from datetime import datetime

from src.repositories.oracle_repo import OracleRepository
from src.models.oracle_models import SampleTable
from src.exceptions.custom_exceptions import LoadingError, DuplicateKeyError
from config.settings import settings
from config.logging_config import etl_logger

class OracleLoader:
    """Oracle DB 데이터 적재기"""
    
    def __init__(self, model_class=SampleTable):
        self.repository = OracleRepository(model_class)
        self.logger = etl_logger
        self.model_class = model_class
    
    def load_single(self, record: Dict[str, Any], upsert: bool = True) -> Any:
        """
        단일 레코드 적재
        
        Args:
            record: 적재할 레코드
            upsert: True면 upsert, False면 insert only
        
        Returns:
            레코드 ID
        """
        try:
            if upsert:
                # Upsert (mongo_id를 기준으로)
                return self.repository.upsert(record, unique_keys=['mongo_id'])
            else:
                # Insert only
                return self.repository.insert_one(record)
                
        except DuplicateKeyError as e:
            if not upsert:
                self.logger.warning(f"Duplicate key, skipping: {e}")
                return None
            raise
        except Exception as e:
            self.logger.error(f"Failed to load record: {e}")
            raise LoadingError(f"Loading failed: {e}")
    
    def load_batch(
        self,
        records: List[Dict[str, Any]],
        upsert: bool = True,
        continue_on_error: bool = True
    ) -> Dict[str, Any]:
        """
        배치 적재
        
        Args:
            records: 적재할 레코드 리스트
            upsert: True면 upsert, False면 insert only
            continue_on_error: 에러 발생 시 계속 진행 여부
        
        Returns:
            적재 결과 통계
        """
        stats = {
            'total': len(records),
            'success': 0,
            'failed': 0,
            'skipped': 0,
            'errors': []
        }
        
        for record in records:
            try:
                result = self.load_single(record, upsert)
                if result:
                    stats['success'] += 1
                else:
                    stats['skipped'] += 1
                    
            except Exception as e:
                stats['failed'] += 1
                stats['errors'].append({
                    'mongo_id': record.get('mongo_id'),
                    'error': str(e)
                })
                
                if not continue_on_error:
                    raise
        
        self.logger.info(
            f"Batch load completed - Success: {stats['success']}, "
            f"Failed: {stats['failed']}, Skipped: {stats['skipped']}"
        )
        
        return stats
    
    def validate_before_load(self, record: Dict[str, Any]) -> bool:
        """적재 전 데이터 검증"""
        required_fields = ['mongo_id', 'name']
        
        for field in required_fields:
            if field not in record or record[field] is None:
                self.logger.error(f"Required field missing: {field}")
                return False
        
        return True
    
    def post_load_actions(self, record_id: Any, record: Dict[str, Any]):
        """적재 후 처리"""
        # 예: MongoDB에 처리 완료 표시
        self.logger.debug(f"Post-load actions for record {record_id}")
        # 추가 로직 구현 가능