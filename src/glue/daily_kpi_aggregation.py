"""
NSP Bolt Ride - Simple KPI Aggregation for Development
"""
import sys
import boto3
import json
from datetime import datetime, timedelta
import logging

# Simple argument handling
args = {}
for arg in sys.argv[1:]:
    if arg.startswith('--'):
        key_value = arg[2:].split('=', 1)
        if len(key_value) == 2:
            args[key_value[0]] = key_value[1]

# Configuration
DYNAMODB_TABLE = args.get('dynamodb_table_name', 'nsp-bolt-trips-dev')
S3_BUCKET = args.get('s3_output_bucket', 'nsp-bolt-analytics-dev')
TARGET_DATE = args.get('target_date', (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d'))

# Initialize clients
dynamodb = boto3.resource('dynamodb')
s3_client = boto3.client('s3')
table = dynamodb.Table(DYNAMODB_TABLE)

print(f"Starting KPI processing for {TARGET_DATE}")
print(f"DynamoDB table: {DYNAMODB_TABLE}")
print(f"S3 bucket: {S3_BUCKET}")

def scan_completed_trips(target_date):
    """Scan DynamoDB for completed trips"""
    print(f"Scanning for completed trips on {target_date}")
    
    completed_trips = []
    
    response = table.scan(
        FilterExpression='is_complete = :complete AND begins_with(completion_date, :date)',
        ExpressionAttributeValues={
            ':complete': True,
            ':date': target_date
        }
    )
    
    completed_trips.extend(response['Items'])
    
    # Handle pagination
    while 'LastEvaluatedKey' in response:
        response = table.scan(
            FilterExpression='is_complete = :complete AND begins_with(completion_date, :date)',
            ExpressionAttributeValues={
                ':complete': True,
                ':date': target_date
            },
            ExclusiveStartKey=response['LastEvaluatedKey']
        )
        completed_trips.extend(response['Items'])
    
    print(f"Found {len(completed_trips)} completed trips")
    return completed_trips

def compute_kpis(trips, target_date):
    """Compute KPIs from trips"""
    if not trips:
        return {
            "date": target_date,
            "total_fare": 0.0,
            "count_trips": 0,
            "average_fare": 0.0,
            "max_fare": 0.0,
            "min_fare": 0.0
        }
    
    fare_amounts = []
    for trip in trips:
        try:
            fare = float(trip['trip_end_data']['fare_amount'])
            if fare > 0:
                fare_amounts.append(fare)
        except:
            continue
    
    if not fare_amounts:
        return {
            "date": target_date,
            "total_fare": 0.0,
            "count_trips": 0,
            "average_fare": 0.0,
            "max_fare": 0.0,
            "min_fare": 0.0
        }
    
    total_fare = sum(fare_amounts)
    count_trips = len(fare_amounts)
    
    kpis = {
        "date": target_date,
        "total_fare": round(total_fare, 2),
        "count_trips": count_trips,
        "average_fare": round(total_fare / count_trips, 2),
        "max_fare": round(max(fare_amounts), 2),
        "min_fare": round(min(fare_amounts), 2)
    }
    
    print(f"Computed KPIs: {kpis}")
    return kpis

def write_to_s3(kpis, target_date):
    """Write KPIs to S3"""
    s3_key = f"kpi_metrics/date={target_date}/kpis.json"
    
    s3_client.put_object(
        Bucket=S3_BUCKET,
        Key=s3_key,
        Body=json.dumps(kpis, indent=2),
        ContentType='application/json'
    )
    
    s3_location = f"s3://{S3_BUCKET}/{s3_key}"
    print(f"KPIs written to: {s3_location}")
    return s3_location

# Main execution
try:
    trips = scan_completed_trips(TARGET_DATE)
    kpis = compute_kpis(trips, TARGET_DATE)
    s3_location = write_to_s3(kpis, TARGET_DATE)
    
    print("SUCCESS: KPI processing completed")
    print(f"Processed {kpis['count_trips']} trips")
    print(f"Output: {s3_location}")
    
except Exception as e:
    print(f"ERROR: {str(e)}")
    raise
