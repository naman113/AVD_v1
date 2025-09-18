# ThresholdManager Optimized Caching

## Overview
The ThresholdManager has been optimized to minimize database calls by implementing efficient 30-minute caching.

## Caching Strategy

### Initial Load
- When ThresholdManager is initialized, it fetches ALL thresholds from database once
- All thresholds are stored in memory using composite keys: `(company_id, device_id, parameter)`

### Cache Usage
- Subsequent threshold lookups use in-memory cache (no database calls)
- Cache is thread-safe using locks for concurrent access
- Fast lookups: O(1) time complexity for single parameter lookup

### Automatic Refresh
- Cache automatically refreshes every 30 minutes
- Only refreshes when a lookup is made after expiration
- On refresh error, keeps using stale data (graceful degradation)

### Manual Control
- `force_refresh()` - manually refresh cache
- `get_cache_stats()` - get cache statistics for monitoring

## Performance Benefits

### Before (Old Implementation)
```python
# Every lookup could hit the database
threshold = get_device_thresholds(1, 36, 'P0')  # DB query
threshold = get_device_thresholds(1, 36, 'P1')  # Another DB query
threshold = get_device_thresholds(1, 37, 'P0')  # Another DB query
```

### After (Optimized Implementation)
```python
# One DB query every 30 minutes, all lookups use cache
threshold = get_device_thresholds(1, 36, 'P0')  # Cache lookup (fast)
threshold = get_device_thresholds(1, 36, 'P1')  # Cache lookup (fast)  
threshold = get_device_thresholds(1, 37, 'P0')  # Cache lookup (fast)
```

# Company and device mapping for database integration
company_config:
  # Map MQTT topics to company IDs
  topic_company_mapping:
    Pengvin1: 1
    Gree1: 1
    Energy1: 1
```

## Database Call Reduction
- **Before**: Potentially hundreds of DB calls per minute
- **After**: 1 DB call every 30 minutes maximum
- **Result**: ~99% reduction in database load

## Device ID Handling
The system now uses MQTT device IDs directly as database device IDs:
- MQTT message: `{"DeviceID": "36", "P0": 25.5}`
- Database lookup: `company_id=1, device_id=36, parameter='P0'`
- No device ID mapping required - direct 1:1 correspondence

## Methods

### `get_device_thresholds(company_id, device_id, parameter)`
Returns threshold for specific parameter:
```python
{
    'lower_threshold': 10.0,
    'higher_threshold': 40.0
}
```

### `get_all_device_thresholds(company_id, device_id)`
Returns all thresholds for a device:
```python
{
    'P0': {'lower_threshold': 10.0, 'higher_threshold': 40.0},
    'P1': {'lower_threshold': 25.0, 'higher_threshold': 75.0},
    # ... more parameters
}
```

### `get_cache_stats()`
Returns cache statistics:
```python
{
    'total_entries': 24,
    'cache_age_seconds': 450.2,
    'cache_age_minutes': 7.5,
    'next_refresh_in_seconds': 1349.8,
    'last_refresh_time': 1694951234.5,
    'cache_duration_minutes': 30
}
```

## Configuration
The cache duration can be modified in `ThresholdManager.__init__()`:
```python
self.cache_duration = 1800  # 30 minutes (can be changed)
```

## Thread Safety
The implementation is fully thread-safe and can be used in multi-threaded applications like the MQTT alert monitor.
