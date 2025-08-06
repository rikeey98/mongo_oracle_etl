from typing import Dict, Any, List, Generator, Optional
from datetime import datetime

from src.repositories.mongo_repo import MongoRepository
from src.exceptions.custom_exceptions import ExtractionError, PipelineExecutionError
from config.settings import settings
from config.logging_config import etl_logger

class MongoExtractor:
    """MongoDB 데이터 추출기"""
    
    def __init__(self, collection_name: str):
        self.repository = MongoRepository(collection_name)
        self.logger = etl_logger
        self.batch_size = settings.ETL.batch_size
    
    def extract_with_pipeline(
        self,
        pipeline: List[Dict[str, Any]],
        batch_process: bool = True
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """
        Aggregation Pipeline을 사용한 데이터 추출
        
        Args:
            pipeline: MongoDB aggregation pipeline
            batch_process: 배치 처리 여부
        
        Yields:
            배치 단위의 문서 리스트
        """
        try:
            self.logger.info(f"Starting extraction with pipeline from {self.repository.collection_name}")
            
            # 배치 처리를 위한 버퍼
            batch = []
            total_count = 0
            
            # Pipeline 실행
            for document in self.repository.aggregate(pipeline):
                if batch_process:
                    batch.append(document)
                    
                    if len(batch) >= self.batch_size:
                        yield batch
                        total_count += len(batch)
                        self.logger.info(f"Extracted batch of {len(batch)} documents. Total: {total_count}")
                        batch = []
                else:
                    yield [document]
                    total_count += 1
            
            # 남은 배치 처리
            if batch:
                yield batch
                total_count += len(batch)
                self.logger.info(f"Extracted final batch of {len(batch)} documents. Total: {total_count}")
            
            self.logger.info(f"Extraction completed. Total documents: {total_count}")
            
        except Exception as e:
            self.logger.error(f"Extraction failed: {e}")
            raise ExtractionError(f"Failed to extract data: {e}")
    
    def build_pipeline(
        self,
        match_filter: Optional[Dict[str, Any]] = None,
        project_fields: Optional[Dict[str, int]] = None,
        sort_by: Optional[Dict[str, int]] = None,
        limit: Optional[int] = None,
        lookup: Optional[Dict[str, Any]] = None,
        custom_stages: Optional[List[Dict[str, Any]]] = None
    ) -> List[Dict[str, Any]]:
        """
        동적 Pipeline 생성
        
        Args:
            match_filter: $match 스테이지 필터
            project_fields: $project 스테이지 필드
            sort_by: $sort 스테이지 정렬
            limit: $limit 스테이지 제한
            lookup: $lookup 스테이지 설정
            custom_stages: 커스텀 스테이지 리스트
        
        Returns:
            Aggregation pipeline
        """
        pipeline = []
        
        # Match 스테이지
        if match_filter:
            pipeline.append({'$match': match_filter})
        
        # Lookup 스테이지 (Join)
        if lookup:
            pipeline.append({'$lookup': lookup})
        
        # Custom 스테이지
        if custom_stages:
            pipeline.extend(custom_stages)
        
        # Sort 스테이지
        if sort_by:
            pipeline.append({'$sort': sort_by})
        
        # Project 스테이지
        if project_fields:
            pipeline.append({'$project': project_fields})
        
        # Limit 스테이지
        if limit:
            pipeline.append({'$limit': limit})
        
        self.logger.debug(f"Built pipeline: {pipeline}")
        return pipeline
    
    def extract_by_date_range(
        self,
        start_date: datetime,
        end_date: datetime,
        date_field: str = 'created_at'
    ) -> Generator[List[Dict[str, Any]], None, None]:
        """날짜 범위로 데이터 추출"""
        pipeline = self.build_pipeline(
            match_filter={
                date_field: {
                    '$gte': start_date,
                    '$lte': end_date
                }
            }
        )
        
        yield from self.extract_with_pipeline(pipeline)