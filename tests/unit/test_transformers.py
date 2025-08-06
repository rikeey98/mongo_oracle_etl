import unittest
from unittest.mock import Mock, patch
import json
from datetime import datetime

from src.transformers.data_transformer import DataTransformer
from src.exceptions.custom_exceptions import ValidationError, TransformationError

class TestDataTransformer(unittest.TestCase):
    """DataTransformer 테스트"""
    
    def setUp(self):
        """테스트 설정"""
        self.transformer = DataTransformer()
    
    def test_transform_document_basic(self):
        """기본 문서 변환 테스트"""
        # Given
        mongo_doc = {
            '_id': '507f1f77bcf86cd799439011',
            'name': 'Test Document',
            'category': 'sample',
            'status': 'pending',
            'metadata': {'key': 'value'},
            'processed': False
        }
        
        # When
        result = self.transformer.transform_document_to_record(mongo_doc)
        
        # Then
        self.assertEqual(result['mongo_id'], '507f1f77bcf86cd799439011')
        self.assertEqual(result['name'], 'Test Document')
        self.assertEqual(result['category'], 'sample')
        self.assertEqual(json.loads(result['metadata']), {'key': 'value'})
    
    def test_transform_with_custom_mapping(self):
        """커스텀 매핑 변환 테스트"""
        # Given
        mongo_doc = {
            'title': 'Custom Title',
            'type': 'custom'
        }
        
        mapping = {
            'title': 'name',
            'type': 'category'
        }
        
        # When
        result = self.transformer.transform_document_to_record(mongo_doc, mapping)
        
        # Then