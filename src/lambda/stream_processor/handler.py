"""
NSP Bolt Ride - Stream Processor Lambda
Handles trip start and end events from Kinesis stream
"""
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Process Kinesis stream events for trip start/end
    """
    logger.info(f"Processing {len(event.get('Records', []))} records")
    
    processed_count = 0
    
    for record in event.get('Records', []):
        try:
            # Decode Kinesis record
            payload = json.loads(record['kinesis']['data'])
            
            # Log event processing
            logger.info(f"Processing event: {payload.get('event_type')} for trip: {payload.get('trip_id')}")
            
            processed_count += 1
            
        except Exception as e:
            logger.error(f"Error processing record: {str(e)}")
            continue
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Successfully processed {processed_count} records',
            'processed_count': processed_count
        })
    }
