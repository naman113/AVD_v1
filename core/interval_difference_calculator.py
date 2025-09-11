from typing import Dict, Any, Optional
from datetime import datetime, timedelta
import logging
import re

class IntervalDifferenceCalculator:
    """
    Calculates differences between intervals instead of consecutive readings.
    
    Example with 5-minute intervals:
    - Interval 1 (12:00-12:05): Store last reading of interval
    - Interval 2 (12:05-12:10): Calculate difference from interval 1's last reading
    """
    
    def __init__(self):
        # In-memory storage: {topic:device_id: IntervalData}
        self._interval_data = {}
    
    def process_reading(self, topic: str, device_id: str, current_row: Dict[str, Any], 
                       frequency_minutes: int) -> Optional[Dict[str, Any]]:
        """
        Process a reading for interval-based difference calculation.
        
        Returns:
        - None: If no difference to calculate (first interval or within current interval)
        - Dict: Difference row to insert to database
        """
        
        cache_key = f"{topic}:{device_id}"
        timestamp = self._extract_timestamp(current_row)
        
        if not timestamp:
            logging.warning(f"[INTERVAL] No valid timestamp found for {cache_key}")
            return None
        
        current_interval = self._calculate_interval_boundary(timestamp, frequency_minutes)
        numeric_data = self._extract_numeric_fields(current_row)
        
        if not numeric_data:
            logging.debug(f"[INTERVAL] No numeric fields found for {cache_key}")
            return None
        
        # Initialize or get existing interval data
        if cache_key not in self._interval_data:
            self._interval_data[cache_key] = {
                'current_interval': current_interval,
                'current_reading': numeric_data,
                'previous_interval_reading': None,
                'frequency_minutes': frequency_minutes,
                'last_timestamp': timestamp,
                'previous_timestamp': None
            }
            logging.info(f"[INTERVAL] Initialized interval tracking for {cache_key} at interval {current_interval}")
            return None
        
        interval_info = self._interval_data[cache_key]
        
        # Check if we're still in the same interval
        if interval_info['current_interval'] == current_interval:
            # Update current reading (keep the latest reading of this interval)
            interval_info['current_reading'] = numeric_data
            interval_info['last_timestamp'] = timestamp
            logging.debug(f"[INTERVAL] Updated reading for {cache_key} in interval {current_interval}")
            return None
        
        # We've moved to a new interval - calculate difference
        if interval_info['previous_interval_reading'] is not None:
            # Calculate differences
            diff_row = self._calculate_interval_difference(
                topic, device_id, current_row, 
                interval_info['current_reading'], 
                interval_info['previous_interval_reading'],
                interval_info['current_interval']
            )
            
            # Move current to previous, start new interval
            interval_info['previous_interval_reading'] = interval_info['current_reading'].copy()
            interval_info['previous_timestamp'] = interval_info['last_timestamp']
            interval_info['current_interval'] = current_interval
            interval_info['current_reading'] = numeric_data
            interval_info['last_timestamp'] = timestamp
            
            logging.info(f"[INTERVAL] Calculated interval difference for {cache_key}: "
                        f"interval {interval_info['current_interval']} -> {current_interval}")
            return diff_row
        else:
            # This is the second interval, but we need previous interval data
            interval_info['previous_interval_reading'] = interval_info['current_reading'].copy()
            interval_info['previous_timestamp'] = interval_info['last_timestamp']
            interval_info['current_interval'] = current_interval
            interval_info['current_reading'] = numeric_data
            interval_info['last_timestamp'] = timestamp
            
            logging.info(f"[INTERVAL] Set previous interval baseline for {cache_key} at interval {current_interval}")
            return None
    
    def _extract_timestamp(self, row: Dict[str, Any]) -> Optional[datetime]:
        """Extract and parse timestamp from row data."""
        
        # Try different timestamp fields
        timestamp_fields = ['ts', 'Time', 'timestamp', 'Date']
        
        for field in timestamp_fields:
            if field in row and row[field]:
                try:
                    timestamp_str = str(row[field])
                    
                    # Handle different timestamp formats
                    # Format 1: HHMMSS (like "020702" for 02:07:02)
                    if len(timestamp_str) == 6 and timestamp_str.isdigit():
                        hours = int(timestamp_str[:2])
                        minutes = int(timestamp_str[2:4])
                        seconds = int(timestamp_str[4:6])
                        
                        # Use current date with parsed time
                        now = datetime.now()
                        return now.replace(hour=hours, minute=minutes, second=seconds, microsecond=0)
                    
                    # Format 2: ISO format or other standard formats
                    # Add more parsing logic as needed based on your data format
                    
                except (ValueError, TypeError) as e:
                    logging.debug(f"[INTERVAL] Failed to parse timestamp {timestamp_str}: {e}")
                    continue
        
        # Fallback: use current time if no valid timestamp found
        logging.debug("[INTERVAL] Using current time as fallback timestamp")
        return datetime.now()
    
    def _calculate_interval_boundary(self, timestamp: datetime, frequency_minutes: int) -> str:
        """Calculate the interval boundary that this timestamp belongs to."""
        
        # Round down to the nearest interval boundary
        total_minutes = timestamp.hour * 60 + timestamp.minute
        interval_start_minutes = (total_minutes // frequency_minutes) * frequency_minutes
        
        hours = interval_start_minutes // 60
        minutes = interval_start_minutes % 60
        
        # Create interval boundary timestamp
        interval_start = timestamp.replace(hour=hours, minute=minutes, second=0, microsecond=0)
        
        # Return as string for easy comparison
        return interval_start.strftime("%Y-%m-%dT%H:%M")
    
    def _extract_numeric_fields(self, row: Dict[str, Any]) -> Dict[str, float]:
        """Extract and convert numeric fields from row data."""
        
        numeric_data = {}
        
        for key, value in row.items():
            # Skip metadata fields
            if key in ['topic', 'DeviceID', 'Date', 'Time', 'ts', 'ingested_at']:
                continue
            
            # Try to convert to numeric
            numeric_value = self._try_convert_to_numeric(value)
            if numeric_value is not None:
                numeric_data[key] = numeric_value
        
        return numeric_data
    
    def _try_convert_to_numeric(self, value: Any) -> Optional[float]:
        """Try to convert a value to numeric (float). Returns None if not possible."""
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                cleaned = value.strip()
                if cleaned:
                    return float(cleaned)
            except (ValueError, TypeError):
                pass
        return None
    
    def _calculate_interval_difference(self, topic: str, device_id: str, current_row: Dict[str, Any],
                                     current_interval_reading: Dict[str, float],
                                     previous_interval_reading: Dict[str, float],
                                     interval_boundary: str) -> Dict[str, Any]:
        """Calculate differences between interval readings."""
        
        cache_key = f"{topic}:{device_id}"
        interval_info = self._interval_data[cache_key]
        
        diff_row = {
            'topic': topic,
            'DeviceID': device_id,
            'Date': current_row.get('Date'),
            'Time': current_row.get('Time'),
            'ts': current_row.get('ts'),
            'interval_boundary': interval_boundary,
            # Add start P0 value and time (from previous interval)
            'start_P0_value': previous_interval_reading.get('P0', 0.0),
            'start_P0_time': interval_info.get('previous_timestamp', datetime.now()).strftime('%H%M%S') if interval_info.get('previous_timestamp') else '',
            # Add end P0 value and time (from current interval)
            'end_P0_value': current_interval_reading.get('P0', 0.0),
            'end_P0_time': interval_info.get('last_timestamp', datetime.now()).strftime('%H%M%S') if interval_info.get('last_timestamp') else ''
        }
        
        # Calculate differences for each numeric field
        for field, current_value in current_interval_reading.items():
            if field in previous_interval_reading:
                difference = current_value - previous_interval_reading[field]
                diff_row[field] = difference
        
        return diff_row
    
    def get_statistics(self) -> Dict[str, Any]:
        """Get statistics about interval tracking."""
        stats = {
            'total_devices_tracked': len(self._interval_data),
            'devices': {}
        }
        
        for cache_key, data in self._interval_data.items():
            stats['devices'][cache_key] = {
                'current_interval': data['current_interval'],
                'frequency_minutes': data['frequency_minutes'],
                'last_timestamp': data['last_timestamp'].isoformat() if data['last_timestamp'] else None,
                'has_previous_baseline': data['previous_interval_reading'] is not None,
                'numeric_fields': list(data['current_reading'].keys()) if data['current_reading'] else []
            }
        
        return stats
