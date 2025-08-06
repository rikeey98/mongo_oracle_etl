from typing import Dict, Any, Optional, Callable
from datetime import datetime
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import signal
import sys

from src.jobs.job_tracker import JobTracker, JobStatus
from src.etl.extractor import MongoExtractor
from src.transformers.data_transformer import DataTransformer
from src.etl.loader import OracleLoader
from src.exceptions.custom_exceptions import (
    ETLBaseException, RetryableError, ExceptionHandler
)
from config.settings import settings
from config.logging_config import etl_logger

class ETLJobManager:
    """ETL Job 실행 관리"""
    
    def __init__(self):
        self.job_tracker = JobTracker()
        self.logger = etl_logger
        self.is_running = False
        self._register_signal_handlers()
    
    def _register_signal_handlers(self):
        """시그널 핸들러 등록 (Graceful Shutdown)"""
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """시그널 처리"""
        self.logger.info(f"Received signal {signum}, shutting down gracefully...")
        self.is_running = False
        sys.exit(0)
    
    def run_etl_pipeline(
        self,
        source_collection: str,
        target_model: Any,
        pipeline: List[Dict[str, Any]],
        transform_mapping: Optional[Dict[str, str]] = None,
        job_parameters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        ETL 파이프라인 실행
        
        Args:
            source_collection: MongoDB 소스 컬렉션
            target_model: Oracle 타겟 모델
            pipeline: MongoDB aggregation pipeline
            transform_mapping: 필드 매핑
            job_parameters: Job 파라미터
        
        Returns:
            실행 결과 통계
        """
        job_id = None
        
        try:
            # Job 생성
            job_id = self.job_tracker.create_job(
                job_type=f"etl_{source_collection}_to_{target_model.__tablename__}",
                parameters=job_parameters
            )
            
            # Job 시작
            self.job_tracker.start_job(job_id)
            self.is_running = True
            
            # ETL 컴포넌트 초기화
            extractor = MongoExtractor(source_collection)
            transformer = DataTransformer()
            loader = OracleLoader(target_model)
            
            # 통계 초기화
            total_stats = {
                'extracted': 0,
                'transformed': 0,
                'loaded': 0,
                'failed': 0,
                'errors': [],
                'start_time': datetime.now()
            }
            
            # 병렬 처리 설정
            with ThreadPoolExecutor(max_workers=settings.ETL.parallel_workers) as executor:
                futures = []
                
                # 데이터 추출 및 처리
                for batch in extractor.extract_with_pipeline(pipeline):
                    if not self.is_running:
                        self.logger.info("ETL pipeline stopped by user")
                        break
                    
                    total_stats['extracted'] += len(batch)
                    
                    # 배치 처리를 비동기로 실행
                    future = executor.submit(
                        self._process_batch,
                        batch,
                        transformer,
                        loader,
                        transform_mapping
                    )
                    futures.append(future)
                    
                    # 메모리 관리를 위해 일정 수의 future만 유지
                    if len(futures) >= settings.ETL.parallel_workers * 2:
                        self._collect_futures(futures, total_stats)
                        futures = [f for f in futures if not f.done()]
                
                # 남은 future 수집
                self._collect_futures(futures, total_stats)
            
            # 실행 시간 계산
            total_stats['end_time'] = datetime.now()
            total_stats['duration'] = str(total_stats['end_time'] - total_stats['start_time'])
            
            # Job 완료
            self.job_tracker.complete_job(job_id, total_stats)
            
            self.logger.info(f"ETL pipeline completed successfully: {total_stats}")
            return total_stats
            
        except Exception as e:
            self.logger.error(f"ETL pipeline failed: {e}")
            
            if job_id:
                # 재시도 가능 여부 확인
                can_retry = ExceptionHandler.is_retryable(e)
                self.job_tracker.fail_job(job_id, e, can_retry)
                
                if can_retry:
                    retry_delay = ExceptionHandler.get_retry_delay(e)
                    self.logger.info(f"Will retry after {retry_delay} seconds")
                    time.sleep(retry_delay)
                    # 재시도 로직 (여기서는 예외를 다시 발생)
            
            raise
        
        finally:
            self.is_running = False
    
    def _process_batch(
        self,
        batch: List[Dict[str, Any]],
        transformer: DataTransformer,
        loader: OracleLoader,
        mapping: Optional[Dict[str, str]]
    ) -> Dict[str, Any]:
        """배치 처리"""
        try:
            # 변환
            transformed = transformer.transform_batch(batch, mapping)
            
            # 비즈니스 규칙 적용
            for i, record in enumerate(transformed):
                transformed[i] = transformer.apply_business_rules(record)
            
            # 적재
            load_stats = loader.load_batch(transformed, upsert=True)
            
            return {
                'transformed': len(transformed),
                'load_stats': load_stats
            }
            
        except Exception as e:
            self.logger.error(f"Batch processing failed: {e}")
            return {
                'transformed': 0,
                'load_stats': {'success': 0, 'failed': len(batch)},
                'error': str(e)
            }
    
    def _collect_futures(self, futures: List, stats: Dict[str, Any]):
        """Future 결과 수집"""
        for future in as_completed(futures):
            try:
                result = future.result()
                stats['transformed'] += result['transformed']
                stats['loaded'] += result['load_stats']['success']
                stats['failed'] += result['load_stats'].get('failed', 0)
                
                if 'error' in result:
                    stats['errors'].append(result['error'])
                    
            except Exception as e:
                self.logger.error(f"Future collection error: {e}")
                stats['failed'] += 1
                stats['errors'].append(str(e))
    
    def retry_failed_jobs(self, job_type: Optional[str] = None) -> int:
        """실패한 Job 재시도"""
        # 재시도 대상 Job 조회
        filter_dict = {
            'status': JobStatus.RETRYING.value,
            'retry_count': {'$lt': settings.ETL.max_retries}
        }
        
        if job_type:
            filter_dict['job_type'] = job_type
        
        jobs = self.job_tracker.repository.find_many(filter_dict)
        retry_count = 0
        
        for job in jobs:
            try:
                self.logger.info(f"Retrying job: {job['_id']}")
                
                # retry_count 증가
                self.job_tracker.repository.update_one(
                    {'_id': job['_id']},
                    {'retry_count': job['retry_count'] + 1}
                )
                
                # Job 재실행 (파라미터를 사용하여)
                # 실제 구현에서는 job['parameters']를 사용하여 파이프라인 재구성
                
                retry_count += 1
                
            except Exception as e:
                self.logger.error(f"Failed to retry job {job['_id']}: {e}")
        
        return retry_count