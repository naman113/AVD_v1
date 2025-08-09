# Device Mapper Feature

The Device Mapper provides a centralized way to track which device IDs for which topics are stored in which database tables, with optional device naming.

## Features

- **Automatic Registration**: Devices are automatically registered when messages are processed
- **Device Naming**: Optional friendly names for devices (e.g., "Kitchen AC Unit" instead of just device ID "91")
- **Topic/Table Mapping**: Track which table contains data for each device/topic combination
- **Pattern Tracking**: Record which pattern was used for routing each device
- **Statistics**: Get counts of devices per topic, table, named vs unnamed devices
- **CLI Interface**: Command-line tool for managing device mappings

## Database Schema

The device mapper creates a `device_mapper` table with the following columns:

| Column | Type | Description |
|--------|------|-------------|
| id | Integer | Primary key |
| topic | String | MQTT topic name |
| device_id | String | Device identifier |
| table_name | String | Database table storing device data |
| device_name | String | Optional friendly name |
| pattern_name | String | Pattern used for routing |
| first_seen | DateTime | When device was first seen |
| last_seen | DateTime | When device was last seen |
| message_count | Integer | Number of messages processed |

Unique constraint on (topic, device_id) ensures no duplicates.

## Usage

### Automatic Registration

Devices are automatically registered when the ingestor processes messages containing a `DeviceID` field:

```python
# In your router, devices are automatically registered
router = Router(db, patterns, device_mapper)
# When messages are processed, devices are registered automatically
```

### CLI Tool

Use the CLI tool to manage device mappings:

```bash
# List all devices
python tools/device_mapper_cli.py list

# List devices for a specific topic
python tools/device_mapper_cli.py list --topic Gree1

# List devices in a specific table
python tools/device_mapper_cli.py list --table gree1_9

# Find a specific device
python tools/device_mapper_cli.py find Gree1 91

# Set a friendly name for a device
python tools/device_mapper_cli.py name Gree1 91 "Kitchen AC Unit"

# Show statistics
python tools/device_mapper_cli.py stats

# Export data as JSON
python tools/device_mapper_cli.py export --format json

# Export data as CSV
python tools/device_mapper_cli.py export --format csv
```

### Programmatic Access

```python
from core.device_mapper import DeviceMapper

# Initialize mapper
mapper = DeviceMapper(database_uri)

# Find a device
device = mapper.find_device("Gree1", "91")
if device:
    print(f"Device in table: {device['table_name']}")
    print(f"Device name: {device['device_name']}")

# Set device name
mapper.set_device_name("Gree1", "91", "Kitchen AC Unit")

# Get all devices for a topic
devices = mapper.find_devices_by_topic("Gree1")

# Get all devices in a table
devices = mapper.find_devices_by_table("gree1_9")

# Get statistics
stats = mapper.get_stats()
print(f"Total devices: {stats['total_devices']}")
```

## Benefits

1. **Device Discovery**: Easily find which table contains data for any device
2. **Device Management**: Set friendly names to make devices more identifiable
3. **Analytics**: Get insights into device distribution across topics and tables
4. **Troubleshooting**: Quickly locate device data when investigating issues
5. **Monitoring**: Track when devices were first/last seen and message counts

## Integration

The device mapper integrates seamlessly with the existing unified ingestor:

- **Zero Configuration**: Works out of the box with existing setup
- **Automatic Registration**: No manual setup required for device registration
- **Optional Naming**: Device names are optional and don't affect core functionality
- **Backward Compatible**: Existing functionality remains unchanged

## Example Output

```bash
$ python tools/device_mapper_cli.py list --topic Gree1

Devices for topic 'Gree1':

  Topic: Gree1
    1 -> gree1_9 [auto] (22 msgs)
    2 (Living Room AC) -> gree1_9 [auto] (31 msgs)
    3 -> gree1_9 [auto] (42 msgs)
    91 (Kitchen AC Unit) -> gree1_4 [auto] (27 msgs)
    92 -> gree1_4 [auto] (28 msgs)
    95 -> gree1_4 [auto] (42 msgs)
```

```bash
$ python tools/device_mapper_cli.py stats

Device Mapper Statistics:
  Total devices: 45
  Named devices: 5
  Unnamed devices: 40

  Devices per topic:
    Gree1: 35
    Energy1: 8
    Weighing: 2

  Devices per table:
    gree1_9: 20
    gree1_4: 15
    energy1_5: 8
    weighing_7: 2
```
