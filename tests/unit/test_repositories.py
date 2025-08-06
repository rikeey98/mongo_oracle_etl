import unittest
from unittest.mock import Mock, patch, MagicMock
from datetime import datetime
from bson import ObjectId

from src.repositories.mongo_repo import MongoRepository
from src.repositories.oracle_repo import OracleRepository
from src.models.oracle_models import SampleTable
from src.exceptions.custom_exceptions import MongoDBError, OracleDBError

class TestMongoRepository(unittest.TestCase):
    """MongoRepository 테스트"""
    
    def setUp(self):
        """테스트 설정"""
        self.collection_name = 'test_collection'
        
        # Mock MongoDB connection
        with patch('src.repositories.mongo_repo.mongo_connection'):
            self.repo = MongoRepository(self.collection_name)
            self.repo.collection = Mock()
    
    def test_find_one_success(self):
        """find_one 성공 테스트"""
        # Given
        expected_doc = {'_id': ObjectId(), 'name': 'test'}
        self.repo.collection.find_one.return_value = expected_doc
        
        # When
        result = self.repo.find_one({'name': 'test'})
        
        # Then
        self.assertEqual(result['name'], 'Custom Title')
        self.assertEqual(result['category'], 'custom')
    
    def test_transform_with_validation(self):
        """검증 규칙이 있는 변환 테스트"""
        # Given
        self.transformer.register_validator(
            'name',
            lambda x: len(x) > 0 and len(x) < 100
        )
        
        mongo_doc = {
            '_id': '507f1f77bcf86cd799439011',
            'name': 'Valid Name',
            'category': 'test'
        }
        
        # When
        result = self.transformer.transform_document_to_record(mongo_doc)
        
        # Then
        self.assertEqual(result['name'], 'Valid Name')
    
    def test_transform_validation_failure(self):
        """검증 실패 테스트"""
        # Given
        self.transformer.register_validator(
            'name',
            lambda x: len(x) > 0 and len(x) < 5  # 매우 짧은 제한
        )
        
        mongo_doc = {
            '_id': '507f1f77bcf86cd799439011',
            'name': 'This name is too long',
            'category': 'test'
        }
        
        # When & Then
        with self.assertRaises(ValidationError):
            self.transformer.transform_document_to_record(mongo_doc)
    
    def test_transform_with_rules(self):
        """변환 규칙 적용 테스트"""
        # Given
        self.transformer.register_rule(
            'name',
            lambda x: x.upper()  # 대문자 변환
        )
        
        mongo_doc = {
            '_id': '507f1f77bcf86cd799439011',
            'name': 'lowercase name',
            'category': 'test'
        }
        
        # When
        result = self.transformer.transform_document_to_record(mongo_doc)
        
        # Then
        self.assertEqual(result['name'], 'LOWERCASE NAME')
    
    def test_transform_batch(self):
        """배치 변환 테스트"""
        # Given
        documents = [
            {'_id': f'id_{i}', 'name': f'doc_{i}', 'category': 'test'}
            for i in range(5)
        ]
        
        # When
        results = self.transformer.transform_batch(documents)
        
        # Then
        self.assertEqual(len(results), 5)
        for i, result in enumerate(results):
            self.assertEqual(result['mongo_id'], f'id_{i}')
            self.assertEqual(result['name'], f'doc_{i}')
    
    def test_apply_business_rules(self):
        """비즈니스 규칙 적용 테스트"""
        # Given
        record = {
            'name': 'Test',
            'status': 'pending',
            'category': 'premium'
        }
        
        # When
        result = self.transformer.apply_business_rules(record)
        
        # Then
        self.assertEqual(result['status'], 'processing')
        self.assertEqual(result['priority'], 'high')
    
    def test_enrich_data(self):
        """데이터 보강 테스트"""
        # Given
        record = {'name': 'Test', 'value': 100}
        external_data = {'source': 'external', 'additional_field': 'extra'}
        
        # When
        result = self.transformer.enrich_data(record, external_data)
        
        # Then
        self.assertEqual(result['source'], 'external')
        self.assertEqual(result['additional_field'], 'extra')
        self.assertIn('etl_timestamp', result)
        self.assertIsInstance(result['etl_timestamp'], datetime)