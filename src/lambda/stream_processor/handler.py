"""
NSP Bolt Ride - Stream Processor Lambda
Handles trip start and end events from Kinesis stream with robust error handling
"""
import json
import boto3
from datetime import datetime, timedelta
import logging
import os
from decimal import Decimal

logger = logging.getLogger()
logger.setLevel(logging.INFO)

# Initialize AWS clients
dynamodb = boto3.resource('dynamodb')
table_name = os.environ.get('TABLE_NAME', 'nsp-bolt-trips-dev')
table = dynamodb.Table(table_name)

def lambda_handler(event, context):
    """Process Kinesis stream events for trip start/end"""
    
    processed_count = 0
    error_count = 0
    completed_trips = 0
    
    logger.info(f"Processing {len(event['Records'])} records")
    
    for record in event['Records']:
        try:
            # Decode Kinesis record (base64 encoded)
            import base64
            payload_str = base64.b64decode(record['kinesis']['data']).decode('utf-8')
            payload = json.loads(payload_str)
            
            logger.info(f"Processing event: {payload.get('event_type')} for trip: {payload.get('trip_id')}")
            
            # Process based on event type
            if payload['event_type'] == 'trip_start':
                is_completed = process_trip_start(payload)
            elif payload['event_type'] == 'trip_end':
                is_completed = process_trip_end(payload)
            else:
                logger.warning(f"Unknown event type: {payload.get('event_type')}")
                continue
            
            if is_completed:
                completed_trips += 1
                
            processed_count += 1
            
        except Exception as e:
            logger.error(f"Error processing record: {str(e)}")
            logger.error(f"Record data: {record}")
            error_count += 1
            continue
    
    logger.info(f"Successfully processed {processed_count} records, {error_count} errors, {completed_trips} trips completed")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'processed_count': processed_count,
            'error_count': error_count,
            'completed_trips': completed_trips
        })
    }

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
