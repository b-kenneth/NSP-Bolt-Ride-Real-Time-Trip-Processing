"""
NSP Bolt Ride - Enhanced Stream Processor Lambda
Handles trip start and end events with validation, DLQ, and error handling
"""
import json
import boto3
from datetime import datetime, timedelta
import logging
import os
from decimal import Decimal
import uuid
from typing import Dict, Any, Optional, Tuple

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
sqs_client = boto3.client('sqs')

# Environment variables
table_name = os.environ.get('TABLE_NAME', 'nsp-bolt-trips-dev')
invalid_data_bucket = os.environ.get('INVALID_DATA_BUCKET', 'nsp-bolt-invalid-data-dev')
dlq_url = os.environ.get('DLQ_URL', '')

table = dynamodb.Table(table_name)

# Data validation schemas
TRIP_START_SCHEMA = {
    'required_fields': ['trip_id', 'pickup_location_id', 'dropoff_location_id', 'vendor_id', 
                       'pickup_datetime', 'estimated_dropoff_datetime', 'estimated_fare_amount'],
    'field_types': {
        'trip_id': str,
        'pickup_location_id': (int, float),
        'dropoff_location_id': (int, float),
        'vendor_id': (int, float),
        'pickup_datetime': str,
        'estimated_dropoff_datetime': str,
        'estimated_fare_amount': (int, float, Decimal)
    },
    'field_ranges': {
        'pickup_location_id': (1, 500),
        'dropoff_location_id': (1, 500),
        'vendor_id': (1, 10),
        'estimated_fare_amount': (0.01, 1000.0)
    }
}

TRIP_END_SCHEMA = {
    'required_fields': ['trip_id', 'dropoff_datetime', 'rate_code', 'passenger_count',
                       'trip_distance', 'fare_amount', 'tip_amount', 'payment_type', 'trip_type'],
    'field_types': {
        'trip_id': str,
        'dropoff_datetime': str,
        'rate_code': (int, float),
        'passenger_count': (int, float),
        'trip_distance': (int, float),
        'fare_amount': (int, float, Decimal),
        'tip_amount': (int, float, Decimal),
        'payment_type': (int, float),
        'trip_type': (int, float)
    },
    'field_ranges': {
        'rate_code': (1.0, 10.0),
        'passenger_count': (0, 8),
        'trip_distance': (0.0, 100.0),
        'fare_amount': (0.01, 1000.0),
        'tip_amount': (0.0, 200.0),
        'payment_type': (1, 5),
        'trip_type': (1, 3)
    }
}

class ValidationError(Exception):
    """Custom exception for validation errors"""
    def __init__(self, message: str, error_type: str, field: str = None):
        self.message = message
        self.error_type = error_type
        self.field = field
        super().__init__(self.message)

class ErrorCategory:
    """Error categorization for different handling strategies"""
    VALIDATION_ERROR = "VALIDATION_ERROR"
    SCHEMA_ERROR = "SCHEMA_ERROR"
    BUSINESS_RULE_ERROR = "BUSINESS_RULE_ERROR"
    SYSTEM_ERROR = "SYSTEM_ERROR"
    TRANSIENT_ERROR = "TRANSIENT_ERROR"
    POISON_PILL = "POISON_PILL"

def lambda_handler(event, context):
    """Process Kinesis stream events for trip start/end with enhanced error handling"""
    
    processed_count = 0
    error_count = 0
    completed_trips = 0
    validation_errors = 0
    archived_invalid = 0
    
    correlation_id = str(uuid.uuid4())
    logger.info(f"[{correlation_id}] Processing {len(event['Records'])} records")
    
    for record_index, record in enumerate(event['Records']):
        record_correlation_id = f"{correlation_id}-{record_index}"
        
        try:
            # Decode Kinesis record (base64 encoded)
            import base64
            payload_str = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            payload = json.loads(payload_str)
            
            logger.info(f"[{record_correlation_id}] Processing event: {payload.get('event_type')} for trip: {payload.get('trip_id')}")
            
            # Enhanced data validation
            validation_result = validate_event_data(payload, record_correlation_id)
            
            if not validation_result['is_valid']:
                # Archive invalid data to S3
                archive_result = archive_invalid_data(payload, record, validation_result, record_correlation_id)
                if archive_result:
                    archived_invalid += 1
                
                validation_errors += 1
                error_count += 1
                continue
            
            # Process based on event type (ORIGINAL WORKING LOGIC PRESERVED)
            if payload['event_type'] == 'trip_start':
                is_completed = process_trip_start(payload)
            elif payload['event_type'] == 'trip_end':
                is_completed = process_trip_end(payload)
            else:
                logger.warning(f"[{record_correlation_id}] Unknown event type: {payload.get('event_type')}")
                continue
            
            if is_completed:
                completed_trips += 1
                
            processed_count += 1
            
        except ValidationError as ve:
            error_details = handle_categorized_error(ve, ErrorCategory.VALIDATION_ERROR, record, record_correlation_id)
            archive_invalid_data(payload if 'payload' in locals() else {}, record, error_details, record_correlation_id)
            validation_errors += 1
            error_count += 1
            
        except json.JSONDecodeError as je:
            error_details = handle_categorized_error(je, ErrorCategory.SCHEMA_ERROR, record, record_correlation_id)
            send_to_dlq(record, error_details, record_correlation_id)
            error_count += 1
            
        except Exception as e:
            error_category = categorize_error(e)
            error_details = handle_categorized_error(e, error_category, record, record_correlation_id)
            
            if error_category in [ErrorCategory.TRANSIENT_ERROR, ErrorCategory.SYSTEM_ERROR]:
                send_to_dlq(record, error_details, record_correlation_id)
            
            error_count += 1
            continue
    
    # Enhanced logging with metrics
    logger.info(f"[{correlation_id}] Processing completed - Processed: {processed_count}, Errors: {error_count}, "
               f"Completed trips: {completed_trips}, Validation errors: {validation_errors}, "
               f"Archived invalid: {archived_invalid}")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed_count': processed_count,
            'error_count': error_count,
            'completed_trips': completed_trips,
            'validation_errors': validation_errors,
            'archived_invalid': archived_invalid,
            'correlation_id': correlation_id
        })
    }

def validate_event_data(payload: Dict[str, Any], correlation_id: str) -> Dict[str, Any]:
    """Comprehensive data validation with schema enforcement"""
    
    validation_result = {
        'is_valid': True,
        'errors': [],
        'warnings': []
    }
    
    try:
        # Basic structure validation
        if not isinstance(payload, dict):
            validation_result['is_valid'] = False
            validation_result['errors'].append({
                'type': 'STRUCTURE_ERROR',
                'message': 'Payload must be a JSON object',
                'field': 'root'
            })
            return validation_result
        
        # Event type validation
        event_type = payload.get('event_type')
        if not event_type:
            validation_result['is_valid'] = False
            validation_result['errors'].append({
                'type': 'MISSING_FIELD',
                'message': 'Missing required field: event_type',
                'field': 'event_type'
            })
            return validation_result
        
        # Schema selection based on event type
        if event_type == 'trip_start':
            schema = TRIP_START_SCHEMA
        elif event_type == 'trip_end':
            schema = TRIP_END_SCHEMA
        else:
            validation_result['is_valid'] = False
            validation_result['errors'].append({
                'type': 'INVALID_EVENT_TYPE',
                'message': f'Unknown event type: {event_type}',
                'field': 'event_type'
            })
            return validation_result
        
        # Required fields validation
        for field in schema['required_fields']:
            if field not in payload:
                validation_result['is_valid'] = False
                validation_result['errors'].append({
                    'type': 'MISSING_FIELD',
                    'message': f'Missing required field: {field}',
                    'field': field
                })
        
        # Data type validation
        for field, expected_types in schema['field_types'].items():
            if field in payload:
                value = payload[field]
                if not isinstance(value, expected_types):
                    validation_result['is_valid'] = False
                    validation_result['errors'].append({
                        'type': 'TYPE_ERROR',
                        'message': f'Field {field} has invalid type. Expected: {expected_types}, Got: {type(value)}',
                        'field': field
                    })
        
        # Range validation
        for field, (min_val, max_val) in schema.get('field_ranges', {}).items():
            if field in payload:
                try:
                    value = float(payload[field])
                    if not (min_val <= value <= max_val):
                        validation_result['is_valid'] = False
                        validation_result['errors'].append({
                            'type': 'RANGE_ERROR',
                            'message': f'Field {field} value {value} is outside valid range [{min_val}, {max_val}]',
                            'field': field
                        })
                except (ValueError, TypeError):
                    validation_result['warnings'].append({
                        'type': 'RANGE_CHECK_FAILED',
                        'message': f'Could not validate range for field {field}',
                        'field': field
                    })
        
        # Business rule validation
        business_validation = validate_business_rules(payload, event_type)
        validation_result['errors'].extend(business_validation['errors'])
        validation_result['warnings'].extend(business_validation['warnings'])
        
        if business_validation['errors']:
            validation_result['is_valid'] = False
        
        logger.info(f"[{correlation_id}] Validation result: {validation_result}")
        return validation_result
        
    except Exception as e:
        logger.error(f"[{correlation_id}] Validation error: {str(e)}")
        validation_result['is_valid'] = False
        validation_result['errors'].append({
            'type': 'VALIDATION_SYSTEM_ERROR',
            'message': f'Validation system error: {str(e)}',
            'field': 'system'
        })
        return validation_result

def validate_business_rules(payload: Dict[str, Any], event_type: str) -> Dict[str, Any]:
    """Validate business-specific rules"""
    
    result = {'errors': [], 'warnings': []}
    
    try:
        if event_type == 'trip_start':
            # Validate pickup datetime format
            pickup_dt = payload.get('pickup_datetime')
            if pickup_dt:
                try:
                    pickup_time = datetime.fromisoformat(pickup_dt.replace('Z', '+00:00'))
                    
                    # Check if pickup time is not too far in the future
                    if pickup_time > datetime.now() + timedelta(hours=1):
                        result['warnings'].append({
                            'type': 'FUTURE_PICKUP',
                            'message': 'Pickup time is more than 1 hour in the future',
                            'field': 'pickup_datetime'
                        })
                    
                    # Check if pickup time is not too old
                    if pickup_time < datetime.now() - timedelta(days=7):
                        result['warnings'].append({
                            'type': 'OLD_PICKUP',
                            'message': 'Pickup time is more than 7 days old',
                            'field': 'pickup_datetime'
                        })
                        
                except ValueError:
                    result['errors'].append({
                        'type': 'INVALID_DATETIME',
                        'message': 'Invalid pickup_datetime format',
                        'field': 'pickup_datetime'
                    })
        
        elif event_type == 'trip_end':
            # Validate dropoff datetime format
            dropoff_dt = payload.get('dropoff_datetime')
            if dropoff_dt:
                try:
                    dropoff_time = datetime.fromisoformat(dropoff_dt.replace('Z', '+00:00'))
                    
                    # Check if dropoff time is reasonable
                    if dropoff_time > datetime.now() + timedelta(minutes=30):
                        result['errors'].append({
                            'type': 'FUTURE_DROPOFF',
                            'message': 'Dropoff time cannot be in the future',
                            'field': 'dropoff_datetime'
                        })
                        
                except ValueError:
                    result['errors'].append({
                        'type': 'INVALID_DATETIME',
                        'message': 'Invalid dropoff_datetime format',
                        'field': 'dropoff_datetime'
                    })
            
            # Validate fare amount is reasonable
            fare_amount = payload.get('fare_amount')
            trip_distance = payload.get('trip_distance')
            if fare_amount and trip_distance:
                try:
                    fare = float(fare_amount)
                    distance = float(trip_distance)
                    
                    if distance > 0:
                        fare_per_mile = fare / distance
                        if fare_per_mile > 50:  # $50 per mile seems excessive
                            result['warnings'].append({
                                'type': 'HIGH_FARE_RATE',
                                'message': f'Fare per mile (${fare_per_mile:.2f}) seems unusually high',
                                'field': 'fare_amount'
                            })
                except (ValueError, ZeroDivisionError):
                    pass
        
        return result
        
    except Exception as e:
        result['errors'].append({
            'type': 'BUSINESS_RULE_ERROR',
            'message': f'Business rule validation error: {str(e)}',
            'field': 'system'
        })
        return result

def archive_invalid_data(payload: Dict[str, Any], record: Dict[str, Any], 
                        validation_result: Dict[str, Any], correlation_id: str) -> bool:
    """Archive invalid data to S3 with structured metadata"""
    
    try:
        # Create structured metadata
        metadata = {
            'timestamp': datetime.now().isoformat(),
            'correlation_id': correlation_id,
            'kinesis_metadata': {
                'sequence_number': record.get('kinesis', {}).get('sequenceNumber'),
                'partition_key': record.get('kinesis', {}).get('partitionKey'),
                'approximate_arrival_timestamp': record.get('kinesis', {}).get('approximateArrivalTimestamp')
            },
            'validation_errors': validation_result.get('errors', []),
            'validation_warnings': validation_result.get('warnings', []),
            'original_payload': payload,
            'error_category': 'VALIDATION_ERROR'
        }
        
        # Create S3 key with date partitioning
        date_str = datetime.now().strftime('%Y-%m-%d')
        hour_str = datetime.now().strftime('%H')
        s3_key = f"invalid-data/date={date_str}/hour={hour_str}/{correlation_id}.json"
        
        # Upload to S3
        s3_client.put_object(
            Bucket=invalid_data_bucket,
            Key=s3_key,
            Body=json.dumps(metadata, indent=2),
            ContentType='application/json',
            Metadata={
                'correlation-id': correlation_id,
                'error-count': str(len(validation_result.get('errors', []))),
                'event-type': payload.get('event_type', 'unknown')
            }
        )
        
        logger.info(f"[{correlation_id}] Archived invalid data to s3://{invalid_data_bucket}/{s3_key}")
        return True
        
    except Exception as e:
        logger.error(f"[{correlation_id}] Failed to archive invalid data: {str(e)}")
        return False

def categorize_error(error: Exception) -> str:
    """Categorize errors for appropriate handling strategies"""
    
    error_str = str(error).lower()
    error_type = type(error).__name__
    
    # Validation and schema errors
    if isinstance(error, (ValidationError, ValueError, TypeError)):
        return ErrorCategory.VALIDATION_ERROR
    
    if isinstance(error, json.JSONDecodeError):
        return ErrorCategory.SCHEMA_ERROR
    
    # DynamoDB specific errors
    if 'throttling' in error_str or 'provisionedthroughputexceeded' in error_str:
        return ErrorCategory.TRANSIENT_ERROR
    
    if 'conditionalcheckfailed' in error_str:
        return ErrorCategory.BUSINESS_RULE_ERROR
    
    # Network and timeout errors
    if any(keyword in error_str for keyword in ['timeout', 'connection', 'network', 'endpoint']):
        return ErrorCategory.TRANSIENT_ERROR
    
    # Persistent failures (poison pill detection)
    if 'malformed' in error_str or 'corrupt' in error_str:
        return ErrorCategory.POISON_PILL
    
    # Default to system error
    return ErrorCategory.SYSTEM_ERROR

def handle_categorized_error(error: Exception, category: str, record: Dict[str, Any], 
                           correlation_id: str) -> Dict[str, Any]:
    """Handle errors based on their category with appropriate strategies"""
    
    error_details = {
        'timestamp': datetime.now().isoformat(),
        'correlation_id': correlation_id,
        'error_category': category,
        'error_type': type(error).__name__,
        'error_message': str(error),
        'record_metadata': {
            'sequence_number': record.get('kinesis', {}).get('sequenceNumber'),
            'partition_key': record.get('kinesis', {}).get('partitionKey')
        }
    }
    
    if category == ErrorCategory.VALIDATION_ERROR:
        logger.warning(f"[{correlation_id}] Validation error: {str(error)}")
        # Validation errors are archived, not retried
        
    elif category == ErrorCategory.TRANSIENT_ERROR:
        logger.warning(f"[{correlation_id}] Transient error (will retry): {str(error)}")
        # These will be sent to DLQ for retry
        
    elif category == ErrorCategory.POISON_PILL:
        logger.error(f"[{correlation_id}] Poison pill detected: {str(error)}")
        # These need special handling to avoid blocking the stream
        
    elif category == ErrorCategory.SYSTEM_ERROR:
        logger.error(f"[{correlation_id}] System error: {str(error)}")
        # System errors should be investigated
        
    else:
        logger.error(f"[{correlation_id}] Uncategorized error: {str(error)}")
    
    return error_details

def send_to_dlq(record: Dict[str, Any], error_details: Dict[str, Any], correlation_id: str) -> bool:
    """Send failed records to Dead Letter Queue for retry or manual processing"""
    
    if not dlq_url:
        logger.warning(f"[{correlation_id}] DLQ URL not configured, cannot send failed record")
        return False
    
    try:
        # Prepare DLQ message with error context
        dlq_message = {
            'original_record': record,
            'error_details': error_details,
            'retry_count': 0,
            'max_retries': 3,
            'correlation_id': correlation_id
        }
        
        # Send to DLQ
        response = sqs_client.send_message(
            QueueUrl=dlq_url,
            MessageBody=json.dumps(dlq_message),
            MessageAttributes={
                'ErrorCategory': {
                    'StringValue': error_details.get('error_category', 'UNKNOWN'),
                    'DataType': 'String'
                },
                'CorrelationId': {
                    'StringValue': correlation_id,
                    'DataType': 'String'
                }
            }
        )
        
        logger.info(f"[{correlation_id}] Sent record to DLQ: {response['MessageId']}")
        return True
        
    except Exception as e:
        logger.error(f"[{correlation_id}] Failed to send record to DLQ: {str(e)}")
        return False

# ORIGINAL WORKING LOGIC PRESERVED BELOW - NO CHANGES

def process_trip_start(event_data):
    """Handle trip start event with idempotent upsert logic"""
    trip_id = event_data['trip_id']
    
    # Set TTL for 24 hours from now
    ttl_timestamp = int((datetime.now() + timedelta(hours=24)).timestamp())
    
    try:
        # Use conditional update to handle race conditions
        response = table.update_item(
            Key={'trip_id': trip_id},
            UpdateExpression='''
                SET trip_start_data = :start_data,
                    ttl_timestamp = if_not_exists(ttl_timestamp, :ttl),
                    pickup_datetime = :pickup_time
            ''',
            ExpressionAttributeValues={
                ':start_data': {
                    'pickup_location_id': int(event_data['pickup_location_id']),
                    'dropoff_location_id': int(event_data['dropoff_location_id']),
                    'vendor_id': int(event_data['vendor_id']),
                    'pickup_datetime': event_data['pickup_datetime'],
                    'estimated_dropoff_datetime': event_data['estimated_dropoff_datetime'],
                    'estimated_fare_amount': Decimal(str(event_data['estimated_fare_amount']))
                },
                ':ttl': ttl_timestamp,
                ':pickup_time': event_data['pickup_datetime']
            },
            ReturnValues='ALL_NEW'
        )
        
        # Check if trip is now complete
        item = response['Attributes']
        if 'trip_end_data' in item and 'trip_start_data' in item:
            mark_trip_complete(trip_id, item)
            logger.info(f"Trip {trip_id} completed after trip_start processing")
            return True
            
        logger.info(f"Processed trip start for {trip_id}")
        return False
        
    except Exception as e:
        logger.error(f"Error processing trip start {trip_id}: {str(e)}")
        raise

def process_trip_end(event_data):
    """Handle trip end event with idempotent upsert logic"""
    trip_id = event_data['trip_id']
    
    try:
        # Use conditional update to handle race conditions
        response = table.update_item(
            Key={'trip_id': trip_id},
            UpdateExpression='''
                SET trip_end_data = :end_data,
                    dropoff_datetime = :dropoff_time,
                    ttl_timestamp = if_not_exists(ttl_timestamp, :ttl)
            ''',
            ExpressionAttributeValues={
                ':end_data': {
                    'dropoff_datetime': event_data['dropoff_datetime'],
                    'rate_code': Decimal(str(event_data['rate_code'])),
                    'passenger_count': Decimal(str(event_data['passenger_count'])),
                    'trip_distance': Decimal(str(event_data['trip_distance'])),
                    'fare_amount': Decimal(str(event_data['fare_amount'])),
                    'tip_amount': Decimal(str(event_data['tip_amount'])),
                    'payment_type': Decimal(str(event_data['payment_type'])),
                    'trip_type': Decimal(str(event_data['trip_type']))
                },
                ':dropoff_time': event_data['dropoff_datetime'],
                ':ttl': int((datetime.now() + timedelta(hours=24)).timestamp())
            },
            ReturnValues='ALL_NEW'
        )
        
        # Check if trip is now complete
        item = response['Attributes']
        if 'trip_start_data' in item and 'trip_end_data' in item:
            mark_trip_complete(trip_id, item)
            logger.info(f"Trip {trip_id} completed after trip_end processing")
            return True
            
        logger.info(f"Processed trip end for {trip_id}")
        return False
        
    except Exception as e:
        logger.error(f"Error processing trip end {trip_id}: {str(e)}")
        raise

def mark_trip_complete(trip_id, item):
    """Mark trip as complete when both start and end data exist"""
    try:
        table.update_item(
            Key={'trip_id': trip_id},
            UpdateExpression='''
                SET is_complete = :complete, 
                    completion_datetime = :completion_time,
                    completion_date = :completion_date
            ''',
            ExpressionAttributeValues={
                ':complete': True,
                ':completion_time': item['dropoff_datetime'],
                ':completion_date': item['dropoff_datetime'][:10]  # Extract YYYY-MM-DD
            }
        )
        
        logger.info(f"Trip {trip_id} marked as complete at {item['dropoff_datetime']}")
        
    except Exception as e:
        logger.error(f"Error marking trip complete {trip_id}: {str(e)}")
        raise

def convert_decimals(obj):
    """Convert Decimal objects to float for JSON serialization"""
    if isinstance(obj, Decimal):
        return float(obj)
    elif isinstance(obj, dict):
        return {key: convert_decimals(value) for key, value in obj.items()}
    elif isinstance(obj, list):
        return [convert_decimals(item) for item in obj]
    return obj
