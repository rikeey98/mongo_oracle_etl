from typing import Dict, Any, List, Optional, Type
from datetime import datetime
from sqlalchemy.orm import Session
from sqlalchemy.exc import IntegrityError as SQLIntegrityError, SQLAlchemyError

from src.repositories.base import BaseRepository
from src.exceptions.custom_exceptions import OracleDBError, DuplicateKeyError, IntegrityError
from config.database import oracle_connection
from config.logging_config import etl_logger

class OracleRepository(BaseRepository):
    """Oracle Repository 구현"""
    
    def __init__(self, model_class: Type):
        self.model_class = model_class
        self.logger = etl_logger
    
    def find_one(self, filter_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """단일 레코드 조회"""
        try:
            with oracle_connection.get_session() as session:
                query = session.query(self.model_class)
                for key, value in filter_dict.items():
                    query = query.filter(getattr(self.model_class, key) == value)
                
                result = query.first()
                if result:
                    self.logger.debug(f"Found record in {self.model_class.__tablename__}")
                    return result.to_dict()
                return None
        except SQLAlchemyError as e:
            self.logger.error(f"Error finding record: {e}")
            raise OracleDBError(f"Failed to find record: {e}")
    
    def find_many(self, filter_dict: Dict[str, Any], limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """다중 레코드 조회"""
        try:
            with oracle_connection.get_session() as session:
                query = session.query(self.model_class)
                for key, value in filter_dict.items():
                    query = query.filter(getattr(self.model_class, key) == value)
                
                if limit:
                    query = query.limit(limit)
                
                results = query.all()
                self.logger.info(f"Found {len(results)} records in {self.model_class.__tablename__}")
                return [r.to_dict() for r in results]
        except SQLAlchemyError as e:
            self.logger.error(f"Error finding records: {e}")
            raise OracleDBError(f"Failed to find records: {e}")
    
    def insert_one(self, data: Dict[str, Any]) -> Any:
        """단일 레코드 삽입"""
        try:
            with oracle_connection.get_session() as session:
                record = self.model_class(**data)
                session.add(record)
                session.flush()
                record_id = record.id
                self.logger.debug(f"Inserted record with ID: {record_id}")
                return record_id
        except SQLIntegrityError as e:
            if 'unique constraint' in str(e).lower():
                self.logger.error(f"Duplicate key error: {e}")
                raise DuplicateKeyError(f"Duplicate key violation: {e}")
            else:
                self.logger.error(f"Integrity error: {e}")
                raise IntegrityError(f"Data integrity violation: {e}")
        except SQLAlchemyError as e:
            self.logger.error(f"Error inserting record: {e}")
            raise OracleDBError(f"Failed to insert record: {e}")
    
    def insert_many(self, data_list: List[Dict[str, Any]]) -> List[Any]:
        """다중 레코드 삽입"""
        try:
            with oracle_connection.get_session() as session:
                records = [self.model_class(**data) for data in data_list]
                session.bulk_save_objects(records, return_defaults=True)
                session.flush()
                
                ids = [r.id for r in records]
                self.logger.info(f"Inserted {len(ids)} records")
                return ids
        except SQLIntegrityError as e:
            self.logger.error(f"Integrity error in bulk insert: {e}")
            raise IntegrityError(f"Data integrity violation in bulk insert: {e}")
        except SQLAlchemyError as e:
            self.logger.error(f"Error in bulk insert: {e}")
            raise OracleDBError(f"Failed to perform bulk insert: {e}")
    
    def update_one(self, filter_dict: Dict[str, Any], update_dict: Dict[str, Any]) -> bool:
        """단일 레코드 업데이트"""
        try:
            with oracle_connection.get_session() as session:
                query = session.query(self.model_class)
                for key, value in filter_dict.items():
                    query = query.filter(getattr(self.model_class, key) == value)
                
                update_dict['updated_at'] = datetime.now()
                result = query.update(update_dict)
                session.flush()
                
                self.logger.debug(f"Updated {result} record(s)")
                return result > 0
        except SQLAlchemyError as e:
            self.logger.error(f"Error updating record: {e}")
            raise OracleDBError(f"Failed to update record: {e}")
    
    def upsert(self, data: Dict[str, Any], unique_keys: List[str]) -> Any:
        """레코드 업서트 (있으면 업데이트, 없으면 삽입)"""
        try:
            with oracle_connection.get_session() as session:
                # 고유 키로 기존 레코드 조회
                filter_dict = {key: data[key] for key in unique_keys if key in data}
                
                query = session.query(self.model_class)
                for key, value in filter_dict.items():
                    query = query.filter(getattr(self.model_class, key) == value)
                
                existing = query.first()
                
                if existing:
                    # 업데이트
                    data['updated_at'] = datetime.now()
                    for key, value in data.items():
                        if hasattr(existing, key):
                            setattr(existing, key, value)
                    session.flush()
                    self.logger.debug(f"Updated existing record: {existing.id}")
                    return existing.id
                else:
                    # 삽입
                    record = self.model_class(**data)
                    session.add(record)
                    session.flush()
                    self.logger.debug(f"Inserted new record: {record.id}")
                    return record.id
                    
        except SQLAlchemyError as e:
            self.logger.error(f"Error in upsert: {e}")
            raise OracleDBError(f"Failed to upsert record: {e}")
    
    def delete_one(self, filter_dict: Dict[str, Any]) -> bool:
        """단일 레코드 삭제"""
        try:
            with oracle_connection.get_session() as session:
                query = session.query(self.model_class)
                for key, value in filter_dict.items():
                    query = query.filter(getattr(self.model_class, key) == value)
                
                result = query.delete()
                session.flush()
                
                self.logger.debug(f"Deleted {result} record(s)")
                return result > 0
        except SQLAlchemyError as e:
            self.logger.error(f"Error deleting record: {e}")
            raise OracleDBError(f"Failed to delete record: {e}")(self, data_list: List[Dict[str, Any]]) -> List[Any]:
        """다중 문서/레코드 삽입"""
        pass
    
    @abstractmethod
    def update_one(self, filter_dict: Dict[str, Any], update_dict: Dict[str, Any]) -> bool:
        """단일 문서/레코드 업데이트"""
        pass
    
    @abstractmethod
    def delete_one(self, filter_dict: Dict[str, Any]) -> bool:
        """단일 문서/레코드 삭제"""
        pass
