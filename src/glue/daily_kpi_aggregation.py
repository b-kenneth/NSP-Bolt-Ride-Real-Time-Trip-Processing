"""
NSP Bolt Ride - Interactive KPI Aggregation Script
Run this in AWS Glue Python Shell for debugging and testing
"""
import boto3
import json
from datetime import datetime, timedelta
import logging
import sys

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class InteractiveKPIProcessor:
    def __init__(self, dynamodb_table_name, s3_output_bucket, region='us-east-1'):
        """Initialize the KPI processor"""
        self.dynamodb_table_name = dynamodb_table_name
        self.s3_output_bucket = s3_output_bucket
        self.region = region
        
        # Initialize AWS clients
        self.dynamodb = boto3.resource('dynamodb', region_name=region)
        self.s3_client = boto3.client('s3', region_name=region)
        self.table = self.dynamodb.Table(dynamodb_table_name)
        
        logger.info(f"Initialized KPI processor for table: {dynamodb_table_name}")
        logger.info(f"S3 output bucket: {s3_output_bucket}")
    
    def get_target_date(self, date_str=None):
        """Get target date for processing"""
        if date_str:
            return date_str
        else:
            # Default to yesterday
            yesterday = datetime.now() - timedelta(days=1)
            return yesterday.strftime('%Y-%m-%d')
    
    def scan_completed_trips(self, target_date):
        """Scan DynamoDB for completed trips on target date"""
        logger.info(f"Scanning for completed trips on {target_date}")
        
        completed_trips = []
        
        try:
            # Scan with filter
            response = self.table.scan(
                FilterExpression='is_complete = :complete AND begins_with(completion_date, :date)',
                ExpressionAttributeValues={
                    ':complete': True,
                    ':date': target_date
                }
            )
            
            completed_trips.extend(response['Items'])
            
            # Handle pagination
            while 'LastEvaluatedKey' in response:
                logger.info(f"Continuing scan from: {response['LastEvaluatedKey']}")
                response = self.table.scan(
                    FilterExpression='is_complete = :complete AND begins_with(completion_date, :date)',
                    ExpressionAttributeValues={
                        ':complete': True,
                        ':date': target_date
                    },
                    ExclusiveStartKey=response['LastEvaluatedKey']
                )
                completed_trips.extend(response['Items'])
            
            logger.info(f"Found {len(completed_trips)} completed trips for {target_date}")
            return completed_trips
            
        except Exception as e:
            logger.error(f"Error scanning DynamoDB: {str(e)}")
            raise
    
    def extract_fare_amounts(self, trips):
        """Extract fare amounts from trip data"""
        logger.info("Extracting fare amounts from trip data")
        
        fare_amounts = []
        
        for trip in trips:
            try:
                # Extract fare from trip_end_data
                if 'trip_end_data' in trip and 'fare_amount' in trip['trip_end_data']:
                    fare_amount = trip['trip_end_data']['fare_amount']
                    
                    # Handle different data types (Decimal, float, string)
                    if hasattr(fare_amount, '__float__'):
                        fare_value = float(fare_amount)
                    else:
                        fare_value = float(str(fare_amount))
                    
                    if fare_value > 0:
                        fare_amounts.append(fare_value)
                        logger.debug(f"Trip {trip['trip_id']}: ${fare_value}")
                    else:
                        logger.warning(f"Trip {trip['trip_id']}: Invalid fare amount {fare_value}")
                else:
                    logger.warning(f"Trip {trip['trip_id']}: Missing fare_amount in trip_end_data")
                    
            except Exception as e:
                logger.error(f"Error extracting fare for trip {trip.get('trip_id', 'unknown')}: {str(e)}")
                continue
        
        logger.info(f"Extracted {len(fare_amounts)} valid fare amounts")
        return fare_amounts
    
    def compute_kpis(self, fare_amounts, target_date):
        """Compute KPIs from fare amounts"""
        logger.info(f"Computing KPIs for {target_date}")
        
        if not fare_amounts:
            logger.warning("No fare amounts available for KPI computation")
            return {
                "date": target_date,
                "total_fare": 0.0,
                "count_trips": 0,
                "average_fare": 0.0,
                "max_fare": 0.0,
                "min_fare": 0.0
            }
        
        # Compute KPIs
        total_fare = sum(fare_amounts)
        count_trips = len(fare_amounts)
        average_fare = total_fare / count_trips
        max_fare = max(fare_amounts)
        min_fare = min(fare_amounts)
        
        kpis = {
            "date": target_date,
            "total_fare": round(total_fare, 2),
            "count_trips": count_trips,
            "average_fare": round(average_fare, 2),
            "max_fare": round(max_fare, 2),
            "min_fare": round(min_fare, 2)
        }
        
        logger.info(f"Computed KPIs: {kpis}")
        return kpis
    
    def write_kpis_to_s3(self, kpis, target_date):
        """Write KPIs to S3 with date partitioning"""
        logger.info(f"Writing KPIs to S3 bucket: {self.s3_output_bucket}")
        
        try:
            # Create S3 key with date partitioning
            s3_key = f"kpi_metrics/date={target_date}/kpis.json"
            
            # Convert KPIs to JSON
            kpi_json = json.dumps(kpis, indent=2)
            
            # Upload to S3
            self.s3_client.put_object(
                Bucket=self.s3_output_bucket,
                Key=s3_key,
                Body=kpi_json,
                ContentType='application/json'
            )
            
            s3_location = f"s3://{self.s3_output_bucket}/{s3_key}"
            logger.info(f"Successfully wrote KPIs to {s3_location}")
            return s3_location
            
        except Exception as e:
            logger.error(f"Error writing to S3: {str(e)}")
            raise
    
    def debug_data_structure(self, target_date, limit=5):
        """Debug data structure in DynamoDB"""
        logger.info(f"Debugging data structure for {target_date}")
        
        try:
            # Get sample completed trips
            response = self.table.scan(
                FilterExpression='is_complete = :complete',
                ExpressionAttributeValues={':complete': True},
                Limit=limit
            )
            
            logger.info(f"Sample completed trips structure:")
            for i, item in enumerate(response['Items']):
                logger.info(f"Trip {i+1}:")
                logger.info(f"  trip_id: {item.get('trip_id')}")
                logger.info(f"  is_complete: {item.get('is_complete')}")
                logger.info(f"  completion_date: {item.get('completion_date')}")
                logger.info(f"  completion_datetime: {item.get('completion_datetime')}")
                
                if 'trip_end_data' in item:
                    logger.info(f"  trip_end_data keys: {list(item['trip_end_data'].keys())}")
                    if 'fare_amount' in item['trip_end_data']:
                        fare = item['trip_end_data']['fare_amount']
                        logger.info(f"  fare_amount: {fare} (type: {type(fare)})")
                
                logger.info("  ---")
                
        except Exception as e:
            logger.error(f"Error debugging data structure: {str(e)}")
    
    def process_kpis(self, target_date=None):
        """Main processing function"""
        target_date = self.get_target_date(target_date)
        
        logger.info(f"Starting KPI processing for {target_date}")
        
        try:
            # Debug data structure first
            self.debug_data_structure(target_date)
            
            # Step 1: Scan for completed trips
            completed_trips = self.scan_completed_trips(target_date)
            
            # Step 2: Extract fare amounts
            fare_amounts = self.extract_fare_amounts(completed_trips)
            
            # Step 3: Compute KPIs
            kpis = self.compute_kpis(fare_amounts, target_date)
            
            # Step 4: Write to S3
            s3_location = self.write_kpis_to_s3(kpis, target_date)
            
            logger.info(f"KPI processing completed successfully!")
            logger.info(f"Output location: {s3_location}")
            
            return {
                'status': 'success',
                'kpis': kpis,
                's3_location': s3_location,
                'trips_processed': len(completed_trips)
            }
            
        except Exception as e:
            logger.error(f"KPI processing failed: {str(e)}")
            return {
                'status': 'error',
                'error': str(e)
            }

# Main execution function
def main():
    """Main function to run in Glue Python Shell"""
    
    # Configuration - Update these values for your environment
    DYNAMODB_TABLE_NAME = "nsp-bolt-trips-dev"  # Update with your table name
    S3_OUTPUT_BUCKET = "nsp-bolt-analytics-dev-1"  # Update with your bucket name (without account ID suffix)
    TARGET_DATE = "2024-05-25"  # Update with your target date or None for yesterday
    
    logger.info("Starting Interactive KPI Processing")
    logger.info(f"DynamoDB Table: {DYNAMODB_TABLE_NAME}")
    logger.info(f"S3 Bucket: {S3_OUTPUT_BUCKET}")
    logger.info(f"Target Date: {TARGET_DATE}")
    
    try:
        # Initialize processor
        processor = InteractiveKPIProcessor(
            dynamodb_table_name=DYNAMODB_TABLE_NAME,
            s3_output_bucket=S3_OUTPUT_BUCKET
        )
        
        # Process KPIs
        result = processor.process_kpis(TARGET_DATE)
        
        # Print results
        print("\n" + "="*50)
        print("PROCESSING RESULTS")
        print("="*50)
        print(json.dumps(result, indent=2))
        
        if result['status'] == 'success':
            print(f"\n✅ SUCCESS: KPIs computed and saved to S3")
            print(f"📊 Trips processed: {result['trips_processed']}")
            print(f"📍 S3 location: {result['s3_location']}")
        else:
            print(f"\n❌ ERROR: {result['error']}")
            
    except Exception as e:
        logger.error(f"Main execution failed: {str(e)}")
        print(f"\n❌ FATAL ERROR: {str(e)}")

# Run the script
if __name__ == "__main__":
    main()
