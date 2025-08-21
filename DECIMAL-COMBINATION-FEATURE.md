# Decimal Combination Feature Summary

## Overview
Added automatic decimal combination transformation specifically for Energy1 topic data that comes in as separate integer and fractional parts.

## Problem Solved
Energy data sometimes arrives as:
- `P0 = 12345` (integer part)
- `P1 = 81723` (fractional part)

But needs to be stored as:
- `P0 = 12345.81723` (combined decimal)

## Implementation

### 1. Configuration Changes
```yaml
- name: flat_series_5_params
  match:
    keys: [DeviceID, Date, Time, P0, P1]
  table: '{topic}_5'
  columns: auto
  transformations:
    - name: combine_decimal_parts
      condition:
        topic: Energy1
      action:
        type: combine_decimal
        integer_field: P0
        fractional_field: P1
        target_field: P0
        remove_fractional: true
```

### 2. New Components
- **DataTransformer**: Handles message transformations
- **Transformation Types**: combine_decimal, scale_value, rename_field, remove_field
- **Conditional Logic**: Apply transformations only when conditions are met

### 3. Processing Flow
1. Message arrives on Energy1 topic
2. Pattern matcher identifies `flat_series_5_params` pattern
3. DataTransformer checks condition (topic = Energy1)
4. Combines P0 and P1 into single decimal value
5. Removes P1 field (as configured)
6. Data inserted into `energy1_5` table

## Examples

### Input Message
```json
{
  "DeviceID": "energy_meter_001",
  "Date": "2024-08-21",
  "Time": "14:30:15",
  "P0": 12345,
  "P1": 81723
}
```

### Transformed Message
```json
{
  "DeviceID": "energy_meter_001", 
  "Date": "2024-08-21",
  "Time": "14:30:15",
  "P0": 12345.81723
}
```

### Database Record
```
energy1_5 table:
id | topic   | DeviceID        | Date       | Time     | P0           | ingested_at
1  | Energy1 | energy_meter_001| 2024-08-21 | 14:30:15 | 12345.81723  | 2024-08-21 14:30:15
```

## Key Features

### Smart Decimal Handling
- Automatically determines decimal places from fractional part length
- `P0=987, P1=65` → `P0=987.65` (2 decimal places)
- `P0=12345, P1=81723` → `P0=12345.81723` (5 decimal places)

### Topic-Specific Application
- Only applies to Energy1 topic
- Gree1 and other topics are unaffected
- Conditional logic prevents accidental transformations

### Flexible Configuration
- Can be enabled/disabled per pattern
- Supports multiple transformation types
- Extensible for future transformation needs

## Testing
- ✅ Unit tests for decimal combination logic
- ✅ Integration tests with pattern matching
- ✅ Simulation of real-world scenarios
- ✅ Verification of topic-specific behavior

## Benefits
1. **Data Quality**: Decimal values stored correctly for analysis
2. **Query Simplicity**: No need to manually combine P0 and P1 in SQL
3. **Storage Efficiency**: Fewer columns needed (P1 removed)
4. **Automatic Processing**: No manual intervention required
5. **Backward Compatibility**: Existing data processing unchanged

## Deployment Ready
The feature is fully implemented, tested, and ready for production use on the DIT-V1 branch.
