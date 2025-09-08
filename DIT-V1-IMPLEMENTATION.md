# DIT V1 Parameter-Based Routing Implementation

## Overview
Implemented automatic parameter-count-based routing for the unified MQTT ingestor. Messages are now automatically routed to different database tables based on the number of parameters they contain.

## Key Features

### 1. Parameter-Specific Patterns
- **4 parameters**: `DeviceID`, `Date`, `Time`, `P0` → `{topic}_4` table
- **5 parameters**: `DeviceID`, `Date`, `Time`, `P0`, `P1` → `{topic}_5` table  
- **9 parameters**: `DeviceID`, `Date`, `Time`, `P0`, `P1`, `P2`, `P3`, `P4`, `P5` → `{topic}_9` table
- **Array enveloped**: Messages with `d` and `ts` structure → `{topic}_array_enveloped` table

### 2. Automatic Table Generation
- `Gree1` topic with 4 parameters → `gree1_4` table
- `Gree1` topic with 9 parameters → `gree1_9` table
- `Energy1` topic with 5 parameters → `energy1_5` table

### 3. Enhanced Pattern Matching
- Prioritizes exact parameter count matches
- Handles both flat message structures and nested `d` structures
- Maintains backward compatibility with existing patterns

## Configuration Changes

### Updated Patterns
```yaml
patterns:
  - name: flat_series_4_params
    match:
      keys: [DeviceID, Date, Time, P0]
    table: '{topic}_4'
    columns: auto
    
  - name: flat_series_5_params
    match:
      keys: [DeviceID, Date, Time, P0, P1]
    table: '{topic}_5'
    columns: auto
    
  - name: flat_series_9_params
    match:
      keys: [DeviceID, Date, Time, P0, P1, P2, P3, P4, P5]
    table: '{topic}_9'
    columns: auto
```

### Simplified Routes
```yaml
routes:
  - topic: Gree1
    mqtt_server: server1
    auto_discover: true
    device_ids:
      - pattern: '*'
        pattern_name: auto
        
  - topic: Energy1
    mqtt_server: server1
    auto_discover: true
    device_ids:
      - pattern: '*'
        pattern_name: auto
```

## Code Changes

### 1. Enhanced PatternMatcher (`core/patterns.py`)
- Added exact match scoring (1000 points for exact matches)
- Improved handling of nested message structures
- Better support for different message formats

### 2. Updated TableManager (`core/table_manager.py`)
- Parameter-count-aware table name generation
- Predefined table names for standard parameter counts (4, 5, 9)
- Maintains compatibility with existing table override functionality

### 3. Improved Router (`core/router.py`)
- Pattern-first routing approach
- Better integration between pattern matching and table resolution
- Enhanced logging for debugging

## Testing

### Automated Tests
- `test_parameter_routing.py`: Verifies pattern matching works correctly
- `test_table_generation.py`: Confirms table name generation is accurate

### Test Results
- ✅ 4-parameter messages correctly match `flat_series_4_params`
- ✅ 5-parameter messages correctly match `flat_series_5_params`
- ✅ 9-parameter messages correctly match `flat_series_9_params`
- ✅ Array enveloped messages correctly match `array_enveloped`
- ✅ Table names generated correctly for all topics

## Benefits

1. **Automatic Organization**: Data is automatically organized by parameter count
2. **No Manual Configuration**: No need to manually specify device ID mappings
3. **Scalable**: Works with any topic and automatically handles new devices
4. **Clean Tables**: Separate tables for different message structures
5. **Performance**: Optimized pattern matching with exact match prioritization

## Usage Example

When a message like this arrives on the `Gree1` topic:
```json
{
  "DeviceID": "device_123",
  "Date": "2024-01-01",
  "Time": "12:00:00", 
  "P0": 25.5,
  "P1": 30.2,
  "P2": 22.1,
  "P3": 28.7,
  "P4": 31.3,
  "P5": 26.9
}
```

It will automatically:
1. Be matched to the `flat_series_9_params` pattern
2. Be inserted into the `gree1_9` table
3. Have all columns auto-created as needed

## Deployment

The implementation is ready for deployment on the `DIT-V1` branch. All changes are backward compatible and include comprehensive testing.
