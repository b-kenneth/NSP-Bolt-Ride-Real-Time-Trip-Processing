"""
Unit tests for stream processor
"""
import json
import pytest
from moto import mock_dynamodb
from src.lambda.stream_processor.handler import lambda_handler

def test_lambda_handler_empty_records():
    """Test handler with empty records"""
    event = {'Records': []}
    context = {}
    
    result = lambda_handler(event, context)
    
    assert result['statusCode'] == 200
    body = json.loads(result['body'])
    assert body['processed_count'] == 0

def test_lambda_handler_with_records():
    """Test handler with mock records"""
    mock_record = {
        'kinesis': {
            'data': json.dumps({
                'event_type': 'trip_start',
                'trip_id': 'test-123'
            })
        }
    }
    
    event = {'Records': [mock_record]}
    context = {}
    
    result = lambda_handler(event, context)
    
    assert result['statusCode'] == 200
    body = json.loads(result['body'])
    assert body['processed_count'] == 1
