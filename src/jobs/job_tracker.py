from typing import Dict, Any, Optional, List
from datetime import datetime, timedelta
from enum import Enum
import traceback
from dataclasses import dataclass, asdict

from src.repositories.mongo_repo import MongoRepository
from src.exceptions.custom_exceptions import JobError, JobAlreadyRunningError
from config.settings import settings
from config.logging_config import etl_logger

class JobStatus(Enum):
    """Job 상태"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"
    RETRYING = "retrying"

@dataclass
class JobInfo:
    """Job 정보"""
    job_id: str
    job_type: str
    status: str
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None
    error_message: Optional[str] = None
    error_details: Optional[Dict[str, Any]] = None
    statistics: Optional[Dict[str, Any]] = None
    parameters: Optional[Dict[str, Any]] = None
    retry_count: int = 0
    max_retries: int = 3

class JobTracker:
    """Job 추적 관리"""
    
    def __init__(self):
        self.repository = MongoRepository(settings.ETL.job_collection)
        self.logger = etl_logger
    
    def create_job(
        self,
        job_type: str,
        parameters: Optional[Dict[str, Any]] = None
    ) -> str:
        """새 Job 생성"""
        try:
            # 동일 타입의 실행 중인 Job 확인
            running_jobs = self.repository.find_many({
                'job_type': job_type,
                'status': JobStatus.RUNNING.value
            })
            
            if running_jobs:
                raise JobAlreadyRunningError(
                    f"Job type '{job_type}' is already running",
                    details={'running_job_ids': [str(j['_id']) for j in running_jobs]}
                )
            
            # 새 Job 생성
            job_data = {
                'job_type': job_type,
                'status': JobStatus.PENDING.value,
                'created_at': datetime.now(),
                'parameters': parameters or {},
                'retry_count': 0,
                'max_retries': settings.ETL.max_retries
            }
            
            job_id = self.repository.insert_one(job_data)
            self.logger.info(f"Created new job: {job_id} (type: {job_type})")
            
            return str(job_id)
            
        except Exception as e:
            self.logger.error(f"Failed to create job: {e}")
            raise JobError(f"Failed to create job: {e}")
    
    def start_job(self, job_id: str) -> bool:
        """Job 시작"""
        try:
            update_data = {
                'status': JobStatus.RUNNING.value,
                'started_at': datetime.now()
            }
            
            result = self.repository.update_one(
                {'_id': job_id},
                update_data
            )
            
            if result:
                self.logger.info(f"Started job: {job_id}")
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to start job {job_id}: {e}")
            raise JobError(f"Failed to start job: {e}")
    
    def complete_job(
        self,
        job_id: str,
        statistics: Optional[Dict[str, Any]] = None
    ) -> bool:
        """Job 완료"""
        try:
            update_data = {
                'status': JobStatus.COMPLETED.value,
                'completed_at': datetime.now(),
                'statistics': statistics or {}
            }
            
            result = self.repository.update_one(
                {'_id': job_id},
                update_data
            )
            
            if result:
                self.logger.info(f"Completed job: {job_id}")
                if statistics:
                    self.logger.info(f"Job statistics: {statistics}")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to complete job {job_id}: {e}")
            raise JobError(f"Failed to complete job: {e}")
    
    def fail_job(
        self,
        job_id: str,
        error: Exception,
        can_retry: bool = True
    ) -> bool:
        """Job 실패 처리"""
        try:
            job = self.repository.find_one({'_id': job_id})
            if not job:
                raise JobError(f"Job not found: {job_id}")
            
            retry_count = job.get('retry_count', 0)
            max_retries = job.get('max_retries', settings.ETL.max_retries)
            
            # 재시도 가능 여부 확인
            if can_retry and retry_count < max_retries:
                status = JobStatus.RETRYING.value
            else:
                status = JobStatus.FAILED.value
            
            update_data = {
                'status': status,
                'completed_at': datetime.now(),
                'error_message': str(error),
                'error_details': {
                    'type': error.__class__.__name__,
                    'traceback': traceback.format_exc()
                },
                'retry_count': retry_count
            }
            
            result = self.repository.update_one(
                {'_id': job_id},
                update_data
            )
            
            if result:
                self.logger.error(f"Job failed: {job_id} - {error}")
                if status == JobStatus.RETRYING.value:
                    self.logger.info(f"Job will be retried: {job_id} (attempt {retry_count + 1}/{max_retries})")
            
            return result
            
        except Exception as e:
            self.logger.error(f"Failed to mark job as failed {job_id}: {e}")
            raise JobError(f"Failed to update job status: {e}")
    
    def get_job_status(self, job_id: str) -> Optional[Dict[str, Any]]:
        """Job 상태 조회"""
        try:
            job = self.repository.find_one({'_id': job_id})
            return job
        except Exception as e:
            self.logger.error(f"Failed to get job status: {e}")
            return None
    
    def get_recent_jobs(
        self,
        job_type: Optional[str] = None,
        status: Optional[str] = None,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """최근 Job 목록 조회"""
        filter_dict = {}
        if job_type:
            filter_dict['job_type'] = job_type
        if status:
            filter_dict['status'] = status
        
        # 최근 생성 순으로 정렬
        pipeline = [
            {'$match': filter_dict},
            {'$sort': {'created_at': -1}},
            {'$limit': limit}
        ]
        
        jobs = list(self.repository.aggregate(pipeline))
        return jobs
    
    def cleanup_old_jobs(self, days: int = 30) -> int:
        """오래된 Job 정리"""
        cutoff_date = datetime.now() - timedelta(days=days)
        
        # 완료된 오래된 Job 삭제
        result = self.repository.collection.delete_many({
            'status': {'$in': [JobStatus.COMPLETED.value, JobStatus.FAILED.value]},
            'completed_at': {'$lt': cutoff_date}
        })
        
        self.logger.info(f"Cleaned up {result.deleted_count} old jobs")
        return result.deleted_count
