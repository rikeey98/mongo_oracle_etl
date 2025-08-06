from abc import ABC, abstractmethod
from typing import Dict, Any, List, Optional, Generator

class BaseRepository(ABC):
    """Repository 기본 인터페이스"""
    
    @abstractmethod
    def find_one(self, filter_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """단일 문서/레코드 조회"""
        pass
    
    @abstractmethod
    def find_many(self, filter_dict: Dict[str, Any], limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """다중 문서/레코드 조회"""
        pass
    
    @abstractmethod
    def insert_one(self, data: Dict[str, Any]) -> Any:
        """단일 문서/레코드 삽입"""
        pass
    
    @abstractmethod
    def insert_many(self, data_list: List[Dict[str, Any]]) -> List[ObjectId]:
        """다중 문서 삽입"""
        try:
            result = self.collection.insert_many(data_list)
            self.logger.info(f"Inserted {len(result.inserted_ids)} documents")
            return result.inserted_ids
        except Exception as e:
            self.logger.error(f"Error inserting documents: {e}")
            raise MongoDBError(f"Failed to insert documents: {e}")
    
    def update_one(self, filter_dict: Dict[str, Any], update_dict: Dict[str, Any]) -> bool:
        """단일 문서 업데이트"""
        try:
            update_dict['updated_at'] = datetime.now()
            result = self.collection.update_one(
                filter_dict,
                {'$set': update_dict}
            )
            self.logger.debug(f"Updated {result.modified_count} document(s)")
            return result.modified_count > 0
        except Exception as e:
            self.logger.error(f"Error updating document: {e}")
            raise MongoDBError(f"Failed to update document: {e}")
    
    def bulk_update(self, updates: List[UpdateOne]) -> Dict[str, Any]:
        """벌크 업데이트"""
        try:
            result = self.collection.bulk_write(updates)
            self.logger.info(f"Bulk update - Modified: {result.modified_count}, Inserted: {result.inserted_count}")
            return {
                'modified_count': result.modified_count,
                'inserted_count': result.inserted_count,
                'deleted_count': result.deleted_count
            }
        except Exception as e:
            self.logger.error(f"Error in bulk update: {e}")
            raise MongoDBError(f"Failed to perform bulk update: {e}")
    
    def delete_one(self, filter_dict: Dict[str, Any]) -> bool:
        """단일 문서 삭제"""
        try:
            result = self.collection.delete_one(filter_dict)
            self.logger.debug(f"Deleted {result.deleted_count} document(s)")
            return result.deleted_count > 0
        except Exception as e:
            self.logger.error(f"Error deleting document: {e}")
            raise MongoDBError(f"Failed to delete document: {e}")