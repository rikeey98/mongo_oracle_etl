from typing import Dict, Any, List, Optional, Generator
from datetime import datetime
import pymongo
from pymongo import UpdateOne
from bson import ObjectId

from src.repositories.base import BaseRepository
from src.models.mongo_models import MongoDocument
from src.exceptions.custom_exceptions import MongoDBError, DataNotFoundError
from config.database import mongo_connection
from config.logging_config import etl_logger

class MongoRepository(BaseRepository):
    """MongoDB Repository 구현"""
    
    def __init__(self, collection_name: str):
        self.collection_name = collection_name
        self.db = mongo_connection.connect()
        self.collection = self.db[collection_name]
        self.logger = etl_logger
    
    def find_one(self, filter_dict: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """단일 문서 조회"""
        try:
            document = self.collection.find_one(filter_dict)
            if document:
                self.logger.debug(f"Found document in {self.collection_name}: {document.get('_id')}")
            return document
        except Exception as e:
            self.logger.error(f"Error finding document: {e}")
            raise MongoDBError(f"Failed to find document: {e}")
    
    def find_many(self, filter_dict: Dict[str, Any], limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """다중 문서 조회"""
        try:
            cursor = self.collection.find(filter_dict)
            if limit:
                cursor = cursor.limit(limit)
            
            documents = list(cursor)
            self.logger.info(f"Found {len(documents)} documents in {self.collection_name}")
            return documents
        except Exception as e:
            self.logger.error(f"Error finding documents: {e}")
            raise MongoDBError(f"Failed to find documents: {e}")
    
    def aggregate(self, pipeline: List[Dict[str, Any]], **kwargs) -> Generator[Dict[str, Any], None, None]:
        """Aggregation Pipeline 실행"""
        try:
            self.logger.info(f"Executing aggregation pipeline on {self.collection_name}")
            self.logger.debug(f"Pipeline: {pipeline}")
            
            # 기본 옵션 설정
            options = {
                'allowDiskUse': True,
                'maxTimeMS': 30000,
                **kwargs
            }
            
            cursor = self.collection.aggregate(pipeline, **options)
            
            count = 0
            for document in cursor:
                count += 1
                yield document
            
            self.logger.info(f"Aggregation completed. Processed {count} documents")
            
        except pymongo.errors.OperationFailure as e:
            self.logger.error(f"Pipeline execution failed: {e}")
            raise MongoDBError(f"Aggregation pipeline failed: {e}")
        except Exception as e:
            self.logger.error(f"Unexpected error in aggregation: {e}")
            raise MongoDBError(f"Aggregation error: {e}")
    
    def insert_one(self, data: Dict[str, Any]) -> ObjectId:
        """단일 문서 삽입"""
        try:
            result = self.collection.insert_one(data)
            self.logger.debug(f"Inserted document: {result.inserted_id}")
            return result.inserted_id
        except Exception as e:
            self.logger.error(f"Error inserting document: {e}")
            raise MongoDBError(f"Failed to insert document: {e}")
    
    def insert_many