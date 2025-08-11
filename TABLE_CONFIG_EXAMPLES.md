# Table Configuration Examples

This document shows examples of the new `table_config` format for automatic table creation.

## Basic Examples

### 1. Auto-create table with specified name
```yaml
- pattern: '103'
  pattern_name: auto
  table_config:
    name: gree1_4
    auto_create: true
    version_on_conflict: true
```

### 2. Auto-generate table name based on message structure
```yaml
- pattern: '104'
  pattern_name: auto
  table_config:
    auto_create: true
    version_on_conflict: true
```

### 3. Use existing table without auto-creation
```yaml
- pattern: '105'
  pattern_name: auto
  table_config:
    name: existing_table
    auto_create: false
```

### 4. Create table but don't version on conflicts
```yaml
- pattern: '106'
  pattern_name: auto
  table_config:
    name: gree1_special
    auto_create: true
    version_on_conflict: false
```

## Migration from table_override

### Old format:
```yaml
- pattern: '103'
  pattern_name: auto
  table_override: gree1_4
```

### New format:
```yaml
- pattern: '103'
  pattern_name: auto
  table_config:
    name: gree1_4
    auto_create: true
    version_on_conflict: true
```

### Old format (auto-generate):
```yaml
- pattern: '*'
  pattern_name: auto
  table_override: null
```

### New format (auto-generate):
```yaml
- pattern: '*'
  pattern_name: auto
  table_config:
    auto_create: true
    version_on_conflict: true
```

## Configuration Options

| Option | Type | Default | Description |
|--------|------|---------|-------------|
| `name` | string | auto-generated | Explicit table name to use |
| `auto_create` | boolean | true | Whether to auto-create tables if they don't exist |
| `version_on_conflict` | boolean | true | Create versioned tables (_v1, _v2) when schema conflicts occur |

## How Table Names are Generated

When `name` is not specified, table names are auto-generated using this pattern:

1. **For specific device patterns:** `{topic}_{pattern}_{column_count}`
   - Example: `gree1_103_5` (topic=Gree1, pattern=103, 5 data columns)

2. **For wildcard patterns:** `{topic}_auto_{column_count}`
   - Example: `gree1_auto_7` (topic=Gree1, wildcard pattern, 7 data columns)

3. **Similar table detection:** If a table with similar structure already exists, it will be reused
   - Similar means 80% column overlap between existing and required columns

## Schema Conflict Resolution

When a table already exists but has incompatible schema:

1. **If `version_on_conflict: true`:** Create a new versioned table
   - `gree1_4` → `gree1_4_v1` → `gree1_4_v2`

2. **If `version_on_conflict: false`:** Use the existing table as-is
   - May result in data loss if columns don't match

## Column Auto-Discovery

Columns are automatically discovered from MQTT message structure:

### Flat Message Structure:
```json
{
  "DeviceID": "103",
  "Temperature": 25.5,
  "Humidity": 60,
  "Status": "online"
}
```
**Columns:** `DeviceID` (string), `Temperature` (float), `Humidity` (int), `Status` (string)

### Array Enveloped Structure:
```json
{
  "d": {
    "DeviceID": ["103"],
    "Temperature": [25.5],
    "Humidity": [60]
  },
  "ts": "2025-08-11T10:30:00Z"
}
```
**Columns:** `DeviceID` (string), `Temperature` (float), `Humidity` (int), `ts` (string)

## Type Mapping

| Python Type | PostgreSQL Type |
|-------------|-----------------|
| `int` | INTEGER |
| `float` | DOUBLE PRECISION |
| `str` | TEXT |
| `bool` | BOOLEAN |
| `dict`, `list` | JSONB |

## Backward Compatibility

The system maintains backward compatibility with the old `table_override` format:

- Existing configurations continue to work
- `table_override: value` is automatically converted to `table_config: {name: value, auto_create: true, version_on_conflict: true}`
- `table_override: null` is converted to `table_config: {auto_create: true, version_on_conflict: true}`

Use the migration tool to convert your configuration:
```bash
python tools/config_migrator.py unified_config.yml
```
