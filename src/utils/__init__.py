"""
NSP Bolt Ride - Utility Functions
"""

def validate_trip_event(event_data):
    """
    Validate trip event data structure
    """
    required_fields = ['trip_id', 'event_type']
    
    for field in required_fields:
        if field not in event_data:
            return False, f"Missing required field: {field}"
    
    return True, "Valid event"

def format_datetime(dt_string):
    """
    Format datetime string for consistent processing
    """
    try:
        dt = datetime.fromisoformat(dt_string.replace('Z', '+00:00'))
        return dt.strftime('%Y-%m-%d %H:%M:%S')
    except Exception as e:
        return dt_string
