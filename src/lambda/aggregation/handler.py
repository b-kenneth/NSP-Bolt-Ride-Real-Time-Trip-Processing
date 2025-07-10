"""
NSP Bolt Ride - Daily Aggregation Lambda
Computes daily KPIs from completed trips
"""
import json
import logging
from datetime import datetime, timedelta

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def lambda_handler(event, context):
    """
    Compute daily KPIs for completed trips
    """
    # Get target date (yesterday by default)
    target_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    if 'date' in event:
        target_date = event['date']
    
    logger.info(f"Processing aggregation for date: {target_date}")
    
    # Mock KPIs for testing
    kpis = {
        'date': target_date,
        'total_fare': 0.0,
        'count_trips': 0,
        'average_fare': 0.0,
        'max_fare': 0.0,
        'min_fare': 0.0
    }
    
    logger.info(f"Generated KPIs: {kpis}")
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': f'Successfully computed KPIs for {target_date}',
            'kpis': kpis
        })
    }
