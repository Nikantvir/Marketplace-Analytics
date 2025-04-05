from datetime import datetime, timedelta
import pytz

def get_date_ranges(is_initial_load=False):
    """
    Get appropriate date ranges based on whether it's an initial load or regular update
    
    For initial load: Returns dates for the past year
    For regular updates: Returns dates for past hour
    """
    now = datetime.now(pytz.UTC)
    
    if is_initial_load:
        # For initial load, get data for the past year
        end_date = now
        start_date = end_date - timedelta(days=365)
        return [(start_date, end_date)]
    else:
        # For regular updates, get recent data
        # Using 20 minutes window to ensure overlap and no data gaps
        end_date = now
        start_date = end_date - timedelta(minutes=20)
        return [(start_date, end_date)]

def get_date_chunks(start_date, end_date, chunk_days=7):
    """
    Split a date range into chunks to avoid API limits
    Returns list of (start_date, end_date) tuples
    """
    chunks = []
    current_start = start_date
    
    while current_start < end_date:
        current_end = min(current_start + timedelta(days=chunk_days), end_date)
        chunks.append((current_start, current_end))
        current_start = current_end
    
    return chunks

def format_date(date):
    """Format date as YYYY-MM-DD"""
    return date.strftime("%Y-%m-%d")

def format_datetime(date):
    """Format datetime as YYYY-MM-DDThh:mm:ss.000Z"""
    return date.strftime("%Y-%m-%dT%H:%M:%S.000Z")