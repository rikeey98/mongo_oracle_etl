import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime

from src.etl.extractor import MongoExtractor
from src.etl.loader import OracleLoader
from src.models.oracle_models import SampleTable
from src.exceptions.custom_exceptions import ExtractionError, LoadingError

class TestMongoExtractor(unittest.TestCase):
    """MongoExtractor 테스트"""
    
    def setUp(self):
        """테스트 설정"""
        with patch('src.etl.extractor.MongoRepository'):
            self.extractor = MongoExtractor('test_collection')
            self.extractor.repository = Mock()
    
    def test_extract_with_pipeline(self):
        """Pipeline 추출 테스트"""
        # Given
        pipeline = [{'$match': {'status': 'active'}}]
        mock_docs = [
            {'_id': f'id_{i}', 'data': f'value_{i}'}
            for i in range(5)
        ]
        
        self.extractor.repository.aggregate.return_value = iter(mock_docs)
        self.extractor.batch_size = 2
        
        # When
        batches = list(self.extractor.extract_with_pipeline(pipeline))
        
        # Then
        self.assertEqual(len(batches), 3)  # 2, 2, 1로 나뉨
        self.assertEqual(len(batches[0]), 2)
        self.assertEqual(len(batches[1]), 2)
        self.assertEqual(len(batches[2]), 1)
    
    def test_build_pipeline(self):
        """Pipeline 빌드 테스트"""
        # Given
        match_filter = {'status': 'active'}
        project_fields = {'name': 1, 'value': 1}
        sort_by = {'created_at': -1}
        limit = 100
        
        # When
        pipeline = self.extractor.build_pipeline(
            match_filter=match_filter,
            project_fields=project_fields,
            sort_by=sort_by,
            limit=limit
        )
        
        # Then
        self.assertEqual(len(pipeline), 4)
        self.assertEqual(pipeline[0], {'$match': match_filter})
        self.assertEqual(pipeline[1], {'$sort': sort_by})
        self.assertEqual(pipeline[2], {'$project': project_fields})
        self.assertEqual(pipeline[3], {'$limit': limit})
    
    def test_extract_by_date_range(self):
        """날짜 범위 추출 테스트"""
        # Given
        start_date = datetime(2024, 1, 1)
        end_date = datetime(2024, 1, 31)
        
        mock_docs = [{'_id': 'id_1', 'created_at': datetime(2024, 1, 15)}]
        self.extractor.repository.aggregate.return_value = iter(mock_docs)
        
        # When
        batches = list(self.extractor.extract_by_date_range(start_date, end_date))
        
        # Then
        self.assertEqual(len(batches), 1)
        # Pipeline이 날짜 필터를 포함하는지 확인
        called_pipeline = self.extractor.repository.aggregate.call_args[0][0]
        self.assertIn('$match', called_pipeline[0])
        self.assertIn('created_at', called_pipeline[0]['$match'])

class TestOracleLoader(unittest.TestCase):
    """OracleLoader 테스트"""
    
    def setUp(self):
        """테스트 설정"""
        with patch('src.etl.loader.OracleRepository'):
            self.loader = OracleLoader(SampleTable)
            self.loader.repository = Mock()
    
    def test_load_single_upsert(self):
        """단일 레코드 upsert 테스트"""
        # Given
        record = {'mongo_id': 'abc123', 'name': 'Test'}
        self.loader.repository.upsert.return_value = 1
        
        # When
        result = self.loader.load_single(record, upsert=True)
        
        # Then
        self.assertEqual(result, 1)
        self.loader.repository.upsert.assert_called_once_with(
            record,
            unique_keys=['mongo_id']
        )
    
    def test_load_single_insert_only(self):
        """단일 레코드 insert only 테스트"""
        # Given
        record = {'mongo_id': 'abc123', 'name': 'Test'}
        self.loader.repository.insert_one.return_value = 1
        
        # When
        result = self.loader.load_single(record, upsert=False)
        
        # Then
        self.assertEqual(result, 1)
        self.loader.repository.insert_one.assert_called_once_with(record)
    
    def test_load_batch(self):
        """배치 적재 테스트"""
        # Given
        records = [
            {'mongo_id': f'id_{i}', 'name': f'Test_{i}'}
            for i in range(3)
        ]
        
        # Mock load_single to simulate success
        with patch.object(self.loader, 'load_single') as mock_load:
            mock_load.side_effect = [1, 2, 3]
            
            # When
            stats = self.loader.load_batch(records)
            
            # Then
            self.assertEqual(stats['total'], 3)
            self.assertEqual(stats['success'], 3)
            self.assertEqual(stats['failed'], 0)
    
    def test_load_batch_with_errors(self):
        """에러가 있는 배치 적재 테스트"""
        # Given
        records = [
            {'mongo_id': f'id_{i}', 'name': f'Test_{i}'}
            for i in range(3)
        ]
        
        # Mock load_single to simulate partial failure
        with patch.object(self.loader, 'load_single') as mock_load:
            mock_load.side_effect = [1, Exception('Error'), 3]
            
            # When
            stats = self.loader.load_batch(records, continue_on_error=True)
            
            # Then
            self.assertEqual(stats['total'], 3)
            self.assertEqual(stats['success'], 2)
            self.assertEqual(stats['failed'], 1)
            self.assertEqual(len(stats['errors']), 1)
    
    def test_validate_before_load(self):
        """적재 전 검증 테스트"""
        # Given
        valid_record = {'mongo_id': 'abc123', 'name': 'Test'}
        invalid_record = {'category': 'test'}  # mongo_id, name 누락
        
        # When & Then
        self.assertTrue(self.loader.validate_before_load(valid_record))
        self.assertFalse(self.loader.validate_before_load(invalid_record))
