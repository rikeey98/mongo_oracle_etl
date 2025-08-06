import unittest
from unittest.mock import patch, Mock
from datetime import datetime

from src.jobs.job_manager import ETLJobManager
from src.models.oracle_models import SampleTable

class TestETLPipeline(unittest.TestCase):
    """통합 파이프라인 테스트"""
    
    def setUp(self):
        """테스트 설정"""
        self.manager = ETLJobManager()
    
    @patch('src.jobs.job_manager.MongoExtractor')
    @patch('src.jobs.job_manager.DataTransformer')
    @patch('src.jobs.job_manager.OracleLoader')
    @patch('src.jobs.job_manager.JobTracker')
    def test_run_etl_pipeline_success(
        self,
        mock_job_tracker,
        mock_loader_class,
        mock_transformer_class,
        mock_extractor_class
    ):
        """ETL 파이프라인 성공 테스트"""
        # Given
        mock_job_tracker.return_value.create_job.return_value = 'job_123'
        
        # Mock extractor
        mock_extractor = Mock()
        mock_extractor.extract_with_pipeline.return_value = [
            [{'_id': 'id_1', 'name': 'doc_1'}],
            [{'_id': 'id_2', 'name': 'doc_2'}]
        ]
        mock_extractor_class.return_value = mock_extractor
        
        # Mock transformer
        mock_transformer = Mock()
        mock_transformer.transform_batch.return_value = [
            {'mongo_id': 'id_1', 'name': 'doc_1'},
            {'mongo_id': 'id_2', 'name': 'doc_2'}
        ]
        mock_transformer.apply_business_rules.side_effect = lambda x: x
        mock_transformer_class.return_value = mock_transformer
        
        # Mock loader
        mock_loader = Mock()
        mock_loader.load_batch.return_value = {
            'success': 2,
            'failed': 0,
            'skipped': 0
        }
        mock_loader_class.return_value = mock_loader
        
        # When
        pipeline = [{'$match': {'status': 'active'}}]
        stats = self.manager.run_etl_pipeline(
            source_collection='test_collection',
            target_model=SampleTable,
            pipeline=pipeline
        )
        
        # Then
        self.assertEqual(stats['extracted'], 2)
        self.assertIn('start_time', stats)
        self.assertIn('end_time', stats)

if __name__ == '__main__':
    unittest.main()
        self.assertEqual(result, expected_doc)
        self.repo.collection.find_one.assert_called_once_with({'name': 'test'})
    
    def test_find_one_not_found(self):
        """find_one 문서 없음 테스트"""
        # Given
        self.repo.collection.find_one.return_value = None
        
        # When
        result = self.repo.find_one({'name': 'nonexistent'})
        
        # Then
        self.assertIsNone(result)
    
    def test_aggregate_pipeline(self):
        """aggregate pipeline 테스트"""
        # Given
        pipeline = [{'$match': {'status': 'active'}}]
        expected_docs = [
            {'_id': ObjectId(), 'status': 'active'},
            {'_id': ObjectId(), 'status': 'active'}
        ]
        
        mock_cursor = MagicMock()
        mock_cursor.__iter__.return_value = iter(expected_docs)
        self.repo.collection.aggregate.return_value = mock_cursor
        
        # When
        result = list(self.repo.aggregate(pipeline))
        
        # Then
        self.assertEqual(result, expected_docs)
        self.repo.collection.aggregate.assert_called_once()
    
    def test_insert_one_success(self):
        """insert_one 성공 테스트"""
        # Given
        doc = {'name': 'test', 'value': 123}
        expected_id = ObjectId()
        
        mock_result = Mock()
        mock_result.inserted_id = expected_id
        self.repo.collection.insert_one.return_value = mock_result
        
        # When
        result = self.repo.insert_one(doc)
        
        # Then
        self.assertEqual(result, expected_id)
        self.repo.collection.insert_one.assert_called_once_with(doc)
    
    def test_bulk_update(self):
        """bulk_update 테스트"""
        # Given
        from pymongo import UpdateOne
        updates = [
            UpdateOne({'_id': ObjectId()}, {'$set': {'status': 'updated'}})
        ]
        
        mock_result = Mock()
        mock_result.modified_count = 1
        mock_result.inserted_count = 0
        mock_result.deleted_count = 0
        self.repo.collection.bulk_write.return_value = mock_result
        
        # When
        result = self.repo.bulk_update(updates)
        
        # Then
        self.assertEqual(result['modified_count'], 1)
        self.repo.collection.bulk_write.assert_called_once_with(updates)

class TestOracleRepository(unittest.TestCase):
    """OracleRepository 테스트"""
    
    def setUp(self):
        """테스트 설정"""
        self.repo = OracleRepository(SampleTable)
        self.mock_session = Mock()
    
    @patch('src.repositories.oracle_repo.oracle_connection')
    def test_find_one_success(self, mock_connection):
        """find_one 성공 테스트"""
        # Given
        mock_connection.get_session.return_value.__enter__.return_value = self.mock_session
        
        mock_record = Mock()
        mock_record.to_dict.return_value = {'id': 1, 'name': 'test'}
        
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = mock_record
        self.mock_session.query.return_value = mock_query
        
        # When
        result = self.repo.find_one({'name': 'test'})
        
        # Then
        self.assertEqual(result, {'id': 1, 'name': 'test'})
    
    @patch('src.repositories.oracle_repo.oracle_connection')
    def test_insert_one_success(self, mock_connection):
        """insert_one 성공 테스트"""
        # Given
        mock_connection.get_session.return_value.__enter__.return_value = self.mock_session
        data = {'name': 'test', 'category': 'sample'}
        
        # When
        with patch.object(SampleTable, '__init__', return_value=None):
            mock_record = Mock()
            mock_record.id = 1
            
            with patch('src.repositories.oracle_repo.SampleTable', return_value=mock_record):
                result = self.repo.insert_one(data)
        
        # Then
        self.assertEqual(result, 1)
        self.mock_session.add.assert_called_once()
        self.mock_session.flush.assert_called_once()
    
    @patch('src.repositories.oracle_repo.oracle_connection')
    def test_upsert_update_existing(self, mock_connection):
        """upsert - 기존 레코드 업데이트 테스트"""
        # Given
        mock_connection.get_session.return_value.__enter__.return_value = self.mock_session
        
        existing_record = Mock()
        existing_record.id = 1
        
        mock_query = Mock()
        mock_query.filter.return_value = mock_query
        mock_query.first.return_value = existing_record
        self.mock_session.query.return_value = mock_query
        
        data = {'mongo_id': 'abc123', 'name': 'updated'}
        
        # When
        result = self.repo.upsert(data, unique_keys=['mongo_id'])
        
        # Then
        self.assertEqual(result, 1)
        self.mock_session.flush.assert_called_once()