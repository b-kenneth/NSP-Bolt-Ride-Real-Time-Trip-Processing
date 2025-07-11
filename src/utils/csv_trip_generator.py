"""
CSV-based Trip Data Generator for NSP Bolt Ride
Ensures complete data exhaustion with proper record tracking
"""
import json
import boto3
import pandas as pd
import random
import time
import logging
from datetime import datetime
import argparse
import os

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class CSVTripDataGenerator:
    def __init__(self, stream_name, region='us-east-1', trip_start_csv=None, trip_end_csv=None):
        """
        Initialize the generator with CSV data sources
        """
        self.kinesis = boto3.client('kinesis', region_name=region)
        self.stream_name = stream_name
        
        # Load CSV data into lists for proper exhaustion
        self.trip_start_data = pd.read_csv(trip_start_csv).to_dict('records') if trip_start_csv else []
        self.trip_end_data = pd.read_csv(trip_end_csv).to_dict('records') if trip_end_csv else []
        
        # Track original counts for statistics
        self.original_start_count = len(self.trip_start_data)
        self.original_end_count = len(self.trip_end_data)
        
        # Statistics
        self.events_sent = 0
        self.errors = 0
        self.start_events_sent = 0
        self.end_events_sent = 0
        
        logger.info(f"Loaded {self.original_start_count} trip_start records")
        logger.info(f"Loaded {self.original_end_count} trip_end records")
        logger.info(f"Total events to process: {self.original_start_count + self.original_end_count}")
    
    def csv_row_to_trip_start_event(self, row):
        """Convert CSV row to trip_start event format"""
        return {
            "event_type": "trip_start",
            "trip_id": str(row['trip_id']),
            "pickup_location_id": int(row['pickup_location_id']),
            "dropoff_location_id": int(row['dropoff_location_id']),
            "vendor_id": int(row['vendor_id']),
            "pickup_datetime": str(row['pickup_datetime']),
            "estimated_dropoff_datetime": str(row['estimated_dropoff_datetime']),
            "estimated_fare_amount": float(row['estimated_fare_amount'])
        }
    
    def csv_row_to_trip_end_event(self, row):
        """Convert CSV row to trip_end event format"""
        return {
            "event_type": "trip_end",
            "trip_id": str(row['trip_id']),
            "dropoff_datetime": str(row['dropoff_datetime']),
            "rate_code": float(row['rate_code']),
            "passenger_count": float(row['passenger_count']),
            "trip_distance": float(row['trip_distance']),
            "fare_amount": float(row['fare_amount']),
            "tip_amount": float(row['tip_amount']),
            "payment_type": float(row['payment_type']),
            "trip_type": float(row['trip_type'])
        }
    
    def send_event_to_kinesis(self, event_data):
        """Send event to Kinesis stream"""
        try:
            response = self.kinesis.put_record(
                StreamName=self.stream_name,
                Data=json.dumps(event_data),
                PartitionKey=event_data['trip_id']
            )
            
            logger.info(f"✅ Sent {event_data['event_type']} for trip {event_data['trip_id']}")
            self.events_sent += 1
            
            if event_data['event_type'] == 'trip_start':
                self.start_events_sent += 1
            else:
                self.end_events_sent += 1
                
            return response
            
        except Exception as e:
            logger.error(f"❌ Error sending event: {str(e)}")
            self.errors += 1
            return None
    
    def get_next_event(self):
        """
        Get next event with proper data exhaustion:
        - Randomly select from available data
        - Remove selected record from list
        - Maintain 70/30 ordering preference
        """
        trip_start_available = len(self.trip_start_data) > 0
        trip_end_available = len(self.trip_end_data) > 0
        
        # If no data left, return None
        if not trip_start_available and not trip_end_available:
            return None
        
        # If only one type available, use it
        if trip_start_available and not trip_end_available:
            # Randomly select and remove from trip_start_data
            selected_index = random.randint(0, len(self.trip_start_data) - 1)
            row = self.trip_start_data.pop(selected_index)  # Remove from list
            return self.csv_row_to_trip_start_event(row)
        
        if trip_end_available and not trip_start_available:
            # Randomly select and remove from trip_end_data
            selected_index = random.randint(0, len(self.trip_end_data) - 1)
            row = self.trip_end_data.pop(selected_index)  # Remove from list
            return self.csv_row_to_trip_end_event(row)
        
        # Both types available - apply 70/30 ordering logic
        if random.random() < 0.7:
            # Prefer trip_start (70% chance)
            selected_index = random.randint(0, len(self.trip_start_data) - 1)
            row = self.trip_start_data.pop(selected_index)  # Remove from list
            return self.csv_row_to_trip_start_event(row)
        else:
            # Out-of-order: send trip_end first (30% chance)
            selected_index = random.randint(0, len(self.trip_end_data) - 1)
            row = self.trip_end_data.pop(selected_index)  # Remove from list
            return self.csv_row_to_trip_end_event(row)
    
    def get_remaining_counts(self):
        """Get remaining record counts"""
        return {
            'trip_start_remaining': len(self.trip_start_data),
            'trip_end_remaining': len(self.trip_end_data),
            'total_remaining': len(self.trip_start_data) + len(self.trip_end_data)
        }
    
    def simulate_streaming(self, delay_min=0.5, delay_max=3.0, batch_size=1):
        """
        Simulate streaming data with complete exhaustion
        """
        logger.info(f"🚀 Starting CSV data streaming simulation...")
        logger.info(f"Stream: {self.stream_name}")
        logger.info(f"Total records to process: {self.original_start_count + self.original_end_count}")
        logger.info(f"Delay range: {delay_min}-{delay_max} seconds")
        logger.info(f"Batch size: {batch_size}")
        
        start_time = datetime.now()
        batch_count = 0
        
        while True:
            batch_events = []
            
            # Collect batch of events
            for _ in range(batch_size):
                event = self.get_next_event()
                if event is None:
                    break
                batch_events.append(event)
            
            # If no events left, break
            if not batch_events:
                break
            
            # Send batch of events
            for event in batch_events:
                self.send_event_to_kinesis(event)
            
            batch_count += 1
            remaining = self.get_remaining_counts()
            
            # Progress logging
            logger.info(f"📊 Batch {batch_count}: Sent {len(batch_events)} events")
            logger.info(f"   Remaining: {remaining['trip_start_remaining']} starts, {remaining['trip_end_remaining']} ends")
            
            # Random delay between batches
            delay = random.uniform(delay_min, delay_max)
            time.sleep(delay)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        self.log_final_statistics(duration)
    
    def simulate_burst_traffic(self, burst_size=50, burst_delay=10):
        """
        Simulate burst traffic with complete data exhaustion
        """
        logger.info(f"🚀 Starting burst traffic simulation...")
        logger.info(f"Total records to process: {self.original_start_count + self.original_end_count}")
        logger.info(f"Burst size: {burst_size}, Burst delay: {burst_delay}s")
        
        start_time = datetime.now()
        burst_count = 0
        
        while True:
            burst_events = []
            
            # Collect burst of events
            for _ in range(burst_size):
                event = self.get_next_event()
                if event is None:
                    break
                burst_events.append(event)
            
            if not burst_events:
                break
            
            # Send burst rapidly
            burst_count += 1
            remaining = self.get_remaining_counts()
            
            logger.info(f"💥 Burst {burst_count}: Sending {len(burst_events)} events...")
            logger.info(f"   Remaining after burst: {remaining['trip_start_remaining']} starts, {remaining['trip_end_remaining']} ends")
            
            for event in burst_events:
                self.send_event_to_kinesis(event)
                time.sleep(0.1)  # Small delay within burst
            
            # Wait before next burst (if more data available)
            remaining_after = self.get_remaining_counts()
            if remaining_after['total_remaining'] > 0:
                logger.info(f"⏳ Waiting {burst_delay}s before next burst...")
                time.sleep(burst_delay)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        self.log_final_statistics(duration)
    
    def log_final_statistics(self, duration):
        """Log comprehensive final statistics"""
        logger.info("✅ Data generation completed!")
        logger.info(f"📈 Final Statistics:")
        logger.info(f"   - Original trip_start records: {self.original_start_count}")
        logger.info(f"   - Original trip_end records: {self.original_end_count}")
        logger.info(f"   - Total original records: {self.original_start_count + self.original_end_count}")
        logger.info(f"   - Trip_start events sent: {self.start_events_sent}")
        logger.info(f"   - Trip_end events sent: {self.end_events_sent}")
        logger.info(f"   - Total events sent: {self.events_sent}")
        logger.info(f"   - Errors: {self.errors}")
        logger.info(f"   - Duration: {duration:.2f} seconds")
        logger.info(f"   - Average rate: {self.events_sent/duration:.2f} events/second")
        
        # Data exhaustion verification
        remaining = self.get_remaining_counts()
        logger.info(f"   - Remaining trip_start: {remaining['trip_start_remaining']}")
        logger.info(f"   - Remaining trip_end: {remaining['trip_end_remaining']}")
        
        if remaining['total_remaining'] == 0:
            logger.info("🎉 SUCCESS: All CSV data completely exhausted!")
        else:
            logger.warning(f"⚠️  WARNING: {remaining['total_remaining']} records not processed!")

def main():
    """Command line interface for the generator"""
    parser = argparse.ArgumentParser(description='CSV Trip Data Generator for Kinesis')
    parser.add_argument('--stream', required=True, help='Kinesis stream name')
    parser.add_argument('--trip-start-csv', required=True, help='Path to trip_start CSV file')
    parser.add_argument('--trip-end-csv', required=True, help='Path to trip_end CSV file')
    parser.add_argument('--mode', choices=['stream', 'burst'], default='stream', help='Simulation mode')
    parser.add_argument('--delay-min', type=float, default=0.5, help='Minimum delay between events')
    parser.add_argument('--delay-max', type=float, default=3.0, help='Maximum delay between events')
    parser.add_argument('--batch-size', type=int, default=1, help='Events per batch')
    parser.add_argument('--burst-size', type=int, default=50, help='Events per burst (burst mode)')
    parser.add_argument('--burst-delay', type=int, default=10, help='Delay between bursts (burst mode)')
    parser.add_argument('--region', default='us-east-1', help='AWS region')
    
    args = parser.parse_args()
    
    # Initialize generator
    generator = CSVTripDataGenerator(
        stream_name=args.stream,
        region=args.region,
        trip_start_csv=args.trip_start_csv,
        trip_end_csv=args.trip_end_csv
    )
    
    # Run simulation based on mode
    if args.mode == 'stream':
        generator.simulate_streaming(
            delay_min=args.delay_min,
            delay_max=args.delay_max,
            batch_size=args.batch_size
        )
    elif args.mode == 'burst':
        generator.simulate_burst_traffic(
            burst_size=args.burst_size,
            burst_delay=args.burst_delay
        )

if __name__ == "__main__":
    main()
