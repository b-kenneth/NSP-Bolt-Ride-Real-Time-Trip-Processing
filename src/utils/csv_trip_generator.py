"""
CSV-based Trip Data Generator for NSP Bolt Ride
Simulates realistic streaming data from CSV files with out-of-order arrival patterns
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
        
        Args:
            stream_name: Kinesis stream name
            region: AWS region
            trip_start_csv: Path to trip_start CSV file
            trip_end_csv: Path to trip_end CSV file
        """
        self.kinesis = boto3.client('kinesis', region_name=region)
        self.stream_name = stream_name
        
        # Load CSV data
        self.trip_start_data = pd.read_csv(trip_start_csv) if trip_start_csv else None
        self.trip_end_data = pd.read_csv(trip_end_csv) if trip_end_csv else None
        
        # Track processed indices
        self.trip_start_index = 0
        self.trip_end_index = 0
        
        # Statistics
        self.events_sent = 0
        self.errors = 0
        
        logger.info(f"Loaded {len(self.trip_start_data)} trip_start records")
        logger.info(f"Loaded {len(self.trip_end_data)} trip_end records")
    
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
            return response
            
        except Exception as e:
            logger.error(f"❌ Error sending event: {str(e)}")
            self.errors += 1
            return None
    
    def get_next_event(self):
        """
        Get next event with realistic ordering logic:
        - 70% chance trip_start comes before trip_end
        - 30% chance of out-of-order arrival
        - Exhaust data from both files
        """
        trip_start_available = self.trip_start_index < len(self.trip_start_data)
        trip_end_available = self.trip_end_index < len(self.trip_end_data)
        
        # If no data left, return None
        if not trip_start_available and not trip_end_available:
            return None
        
        # If only one type available, use it
        if trip_start_available and not trip_end_available:
            row = self.trip_start_data.iloc[self.trip_start_index]
            self.trip_start_index += 1
            return self.csv_row_to_trip_start_event(row)
        
        if trip_end_available and not trip_start_available:
            row = self.trip_end_data.iloc[self.trip_end_index]
            self.trip_end_index += 1
            return self.csv_row_to_trip_end_event(row)
        
        # Both types available - apply ordering logic
        # 70% chance trip_start comes first, 30% chance out-of-order
        if random.random() < 0.7:
            # Prefer trip_start
            row = self.trip_start_data.iloc[self.trip_start_index]
            self.trip_start_index += 1
            return self.csv_row_to_trip_start_event(row)
        else:
            # Out-of-order: send trip_end first
            row = self.trip_end_data.iloc[self.trip_end_index]
            self.trip_end_index += 1
            return self.csv_row_to_trip_end_event(row)
    
    def simulate_streaming(self, delay_min=0.5, delay_max=3.0, batch_size=1):
        """
        Simulate streaming data with realistic delays
        
        Args:
            delay_min: Minimum delay between events (seconds)
            delay_max: Maximum delay between events (seconds)
            batch_size: Number of events to send in each batch
        """
        logger.info(f"🚀 Starting CSV data streaming simulation...")
        logger.info(f"Stream: {self.stream_name}")
        logger.info(f"Delay range: {delay_min}-{delay_max} seconds")
        logger.info(f"Batch size: {batch_size}")
        
        start_time = datetime.now()
        
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
            
            # Random delay between batches
            delay = random.uniform(delay_min, delay_max)
            logger.info(f"📊 Sent batch of {len(batch_events)} events. Waiting {delay:.2f}s...")
            time.sleep(delay)
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        logger.info("✅ Streaming simulation completed!")
        logger.info(f"📈 Statistics:")
        logger.info(f"   - Total events sent: {self.events_sent}")
        logger.info(f"   - Errors: {self.errors}")
        logger.info(f"   - Duration: {duration:.2f} seconds")
        logger.info(f"   - Average rate: {self.events_sent/duration:.2f} events/second")
    
    def simulate_burst_traffic(self, burst_size=50, burst_delay=10):
        """
        Simulate burst traffic patterns
        
        Args:
            burst_size: Number of events per burst
            burst_delay: Delay between bursts (seconds)
        """
        logger.info(f"🚀 Starting burst traffic simulation...")
        logger.info(f"Burst size: {burst_size}, Burst delay: {burst_delay}s")
        
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
            logger.info(f"💥 Sending burst {burst_count + 1} with {len(burst_events)} events...")
            for event in burst_events:
                self.send_event_to_kinesis(event)
                time.sleep(0.1)  # Small delay within burst
            
            burst_count += 1
            
            # Wait before next burst
            if self.get_next_event() is not None:  # Check if more data available
                logger.info(f"⏳ Waiting {burst_delay}s before next burst...")
                time.sleep(burst_delay)
            else:
                break
        
        logger.info(f"✅ Burst simulation completed! Sent {burst_count} bursts")

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
