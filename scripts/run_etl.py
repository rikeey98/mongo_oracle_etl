#!/usr/bin/env python3
"""
ETL 메인 실행 스크립트
Airflow나 Cron에서 실행
"""
import sys
import os
import argparse
from datetime import datetime, timedelta
from pathlib import Path

# 프로젝트 경로 추가
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.jobs.job_manager import ETLJobManager
from src.models.oracle_models import SampleTable
from config.settings import settings
from config.logging_config import etl_logger

def parse_arguments():
    """명령줄 인자 파싱"""
    parser = argparse.ArgumentParser(description='MongoDB to Oracle ETL Pipeline')
    
    parser.add_argument(
        '--source-collection',
        type=str,
        required=True,
        help='Source MongoDB collection name'
    )
    
    parser.add_argument(
        '--target-table',
        type=str,
        default='sample_table',
        help='Target Oracle table name'
    )
    
    parser.add_argument(
        '--date-from',
        type=str,
        help='Start date (YYYY-MM-DD)'
    )
    
    parser.add_argument(
        '--date-to',
        type=str,
        help='End date (YYYY-MM-DD)'
    )
    
    parser.add_argument(
        '--batch-size',
        type=int,
        default=1000,
        help='Batch size for processing'
    )
    
    parser.add_argument(
        '--mode',
        choices=['full', 'incremental', 'retry'],
        default='incremental',
        help='ETL mode'
    )
    
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Dry run without actual data loading'
    )
    
    return parser.parse_args()

def build_pipeline(args) -> list:
    """Arguments 기반 Pipeline 생성"""
    pipeline = []
    
    # 날짜 필터
    if args.mode == 'incremental':
        date_filter = {}
        
        if args.date_from:
            date_from = datetime.strptime(args.date_from, '%Y-%m-%d')
            date_filter['$gte'] = date_from
        else:
            # 기본값: 어제
            date_filter['$gte'] = datetime.now() - timedelta(days=1)
        
        if args.date_to:
            date_to = datetime.strptime(args.date_to, '%Y-%m-%d')
            date_filter['$lte'] = date_to
        else:
            date_filter['$lte'] = datetime.now()
        
        pipeline.append({
            '$match': {
                'created_at': date_filter,
                'processed': {'$ne': True}
            }
        })
    
    # 정렬
    pipeline.append({'$sort': {'created_at': 1}})
    
    # 필요한 필드만 선택
    pipeline.append({
        '$project': {
            '_id': 1,
            'name': 1,
            'category': 1,
            'status': 1,
            'metadata': 1,
            'processed': 1,
            'created_at': 1,
            'updated_at': 1
        }
    })
    
    return pipeline

def main():
    """메인 실행 함수"""
    args = parse_arguments()
    
    try:
        etl_logger.info("=" * 50)
        etl_logger.info("Starting ETL Pipeline")
        etl_logger.info(f"Mode: {args.mode}")
        etl_logger.info(f"Source: {args.source_collection}")
        etl_logger.info(f"Target: {args.target_table}")
        etl_logger.info("=" * 50)
        
        # ETL Manager 초기화
        manager = ETLJobManager()
        
        if args.mode == 'retry':
            # 실패한 Job 재시도
            retry_count = manager.retry_failed_jobs()
            etl_logger.info(f"Retried {retry_count} failed jobs")
        else:
            # Pipeline 생성
            pipeline = build_pipeline(args)
            etl_logger.info(f"Pipeline: {pipeline}")
            
            # 필드 매핑 정의
            field_mapping = {
                '_id': 'mongo_id',
                'name': 'name',
                'category': 'category',
                'status': 'status',
                'metadata': 'metadata',
                'processed': 'processed'
            }
            
            # Job 파라미터
            job_parameters = {
                'mode': args.mode,
                'source_collection': args.source_collection,
                'target_table': args.target_table,
                'batch_size': args.batch_size,
                'dry_run': args.dry_run,
                'date_from': args.date_from,
                'date_to': args.date_to
            }
            
            # ETL 실행
            if not args.dry_run:
                stats = manager.run_etl_pipeline(
                    source_collection=args.source_collection,
                    target_model=SampleTable,
                    pipeline=pipeline,
                    transform_mapping=field_mapping,
                    job_parameters=job_parameters
                )
                
                # 결과 출력
                etl_logger.info("=" * 50)
                etl_logger.info("ETL Pipeline Completed")
                etl_logger.info(f"Extracted: {stats['extracted']}")
                etl_logger.info(f"Transformed: {stats['transformed']}")
                etl_logger.info(f"Loaded: {stats['loaded']}")
                etl_logger.info(f"Failed: {stats['failed']}")
                etl_logger.info(f"Duration: {stats['duration']}")
                etl_logger.info("=" * 50)
                
                # 에러가 있으면 출력
                if stats['errors']:
                    etl_logger.warning(f"Errors encountered: {len(stats['errors'])}")
                    for error in stats['errors'][:10]:  # 처음 10개만
                        etl_logger.warning(f"  - {error}")
                
                # 실패가 있으면 exit code 1
                if stats['failed'] > 0:
                    sys.exit(1)
            else:
                etl_logger.info("DRY RUN - No data was actually loaded")
        
        sys.exit(0)
        
    except Exception as e:
        etl_logger.error(f"ETL Pipeline failed with error: {e}", exc_info=True)
        sys.exit(1)

if __name__ == "__main__":
    main()
