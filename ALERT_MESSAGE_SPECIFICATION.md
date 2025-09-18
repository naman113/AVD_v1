# Alert Message Specification

This document describes the format and structure of alert messages published by the AVD Alert Monitor system. This specification is intended for developers building services to consume and process these alert messages.

## MQTT Configuration

### Publishing Details
- **MQTT Broker**: `s3550ec0.ala.us-east-1.emqxsl.com`
- **Port**: `8883` (SSL/TLS)
- **Topic**: `alerts/monitoring`
- **QoS**: `1` (At least once delivery)
- **Credentials**: `Alert/Alert` (username/password)
- **CA Certificate**: Required for SSL connection

### Connection Settings
- **Client ID Prefix**: `AlertPublisher`
- **Keep Alive**: `60` seconds
- **SSL/TLS**: Enabled with CA certificate validation

## Message Format

### JSON Structure
```json
{
  "timestamp": "2025-09-18T00:16:41.547263",
  "topic": "Pengvin1",
  "device_id": "35",
  "violations": [
    {
      "parameter": "P0",
      "value": 886229.0,
      "threshold": 4.073,
      "type": "high"
    },
    {
      "parameter": "P1",
      "value": -1.362309,
      "threshold": 0.0,
      "type": "low"
    }
  ]
}
```

### Field Specifications

#### Root Level Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `timestamp` | string | Yes | ISO 8601 formatted timestamp when alert was generated |
| `topic` | string | Yes | Original MQTT topic where the sensor data was received |
| `device_id` | string | Yes | Device identifier as received from MQTT message |
| `violations` | array | Yes | Array of violation objects (at least 1 violation) |

#### Violation Object Fields

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `parameter` | string | Yes | Sensor parameter name (e.g., "P0", "P1", "P2", "P3") |
| `value` | number | Yes | Actual sensor value that caused the violation |
| `threshold` | number | Yes | Threshold value that was exceeded |
| `type` | string | Yes | Violation type: "high" or "low" |

### Data Types and Constraints

#### Timestamp
- **Format**: ISO 8601 with microseconds
- **Example**: `"2025-09-18T00:16:41.547263"`
- **Timezone**: Local system timezone

#### Topic
- **Format**: String
- **Examples**: `"Pengvin1"`, `"Gree1"`
- **Note**: Maps to company ID via configuration

#### Device ID
- **Format**: String (numeric)
- **Examples**: `"35"`, `"18"`, `"29"`
- **Note**: Corresponds to database device_id

#### Parameter Names
- **Format**: String
- **Common Values**: `"P0"`, `"P1"`, `"P2"`, `"P3"`, `"P4"`, `"P5"`
- **Case**: Uppercase (as received from MQTT)

#### Values and Thresholds
- **Type**: Number (float)
- **Precision**: Up to 6 decimal places
- **Range**: Can be positive, negative, or zero

#### Violation Types
- **Values**: `"high"` or `"low"`
- **"high"**: Value exceeds maximum threshold
- **"low"**: Value is below minimum threshold

## Database Schema Recommendations

### Alerts Table
```sql
CREATE TABLE alerts (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP WITH TIME ZONE NOT NULL,
    topic VARCHAR(100) NOT NULL,
    device_id VARCHAR(50) NOT NULL,
    violation_count INTEGER NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    processed BOOLEAN DEFAULT FALSE
);
```

### Alert Violations Table
```sql
CREATE TABLE alert_violations (
    id SERIAL PRIMARY KEY,
    alert_id INTEGER REFERENCES alerts(id),
    parameter VARCHAR(10) NOT NULL,
    value NUMERIC(15, 6) NOT NULL,
    threshold NUMERIC(15, 6) NOT NULL,
    violation_type VARCHAR(10) NOT NULL CHECK (violation_type IN ('high', 'low')),
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

## Message Processing Guidelines

### Parsing Steps
1. **JSON Validation**: Ensure valid JSON structure
2. **Schema Validation**: Verify all required fields are present
3. **Data Type Validation**: Confirm field types match specification
4. **Business Logic Validation**: Validate violation types and values

### Error Handling
- **Invalid JSON**: Log error, increment failure counter
- **Missing Fields**: Log warning, attempt partial processing if possible
- **Invalid Data Types**: Log error, skip invalid violations
- **Database Errors**: Implement retry logic with exponential backoff

### Deduplication
- Consider implementing deduplication logic based on:
  - Device ID + Timestamp + Parameter combination
  - Time window-based deduplication (e.g., same violation within 1 minute)

## Example Messages

### Single Violation
```json
{
  "timestamp": "2025-09-18T00:16:41.547263",
  "topic": "Pengvin1",
  "device_id": "35",
  "violations": [
    {
      "parameter": "P0",
      "value": 886229.0,
      "threshold": 4.073,
      "type": "high"
    }
  ]
}
```

### Multiple Violations
```json
{
  "timestamp": "2025-09-18T04:11:07.039570",
  "topic": "Gree1",
  "device_id": "18",
  "violations": [
    {
      "parameter": "P0",
      "value": 29.0,
      "threshold": 0,
      "type": "high"
    },
    {
      "parameter": "P1",
      "value": 50.0,
      "threshold": 0,
      "type": "high"
    },
    {
      "parameter": "P2",
      "value": 58.0,
      "threshold": 0,
      "type": "high"
    }
  ]
}
```

### Low Threshold Violation
```json
{
  "timestamp": "2025-09-18T00:15:40.070964",
  "topic": "Pengvin1",
  "device_id": "9",
  "violations": [
    {
      "parameter": "P1",
      "value": -1.362309,
      "threshold": 0.0,
      "type": "low"
    }
  ]
}
```

## Integration Considerations

### MQTT Client Configuration
```python
# Example Python MQTT client setup
import paho.mqtt.client as mqtt
import json
import ssl

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe("alerts/monitoring", qos=1)

def on_message(client, userdata, msg):
    try:
        alert_data = json.loads(msg.payload.decode())
        process_alert(alert_data)
    except json.JSONDecodeError as e:
        print(f"JSON decode error: {e}")

client = mqtt.Client(client_id="AlertConsumer")
client.username_pw_set("Alert", "Alert")
client.tls_set(ca_certs="path/to/ca.crt", tls_version=ssl.PROTOCOL_TLS)
client.on_connect = on_connect
client.on_message = on_message
client.connect("s3550ec0.ala.us-east-1.emqxsl.com", 8883, 60)
client.loop_forever()
```

### Performance Considerations
- **Message Volume**: Expect high-frequency messages during sensor anomalies
- **Batch Processing**: Consider batching database insertions for better performance
- **Indexing**: Index on timestamp, device_id, and topic for efficient queries
- **Partitioning**: Consider time-based table partitioning for large volumes

### Monitoring and Alerting
- Monitor message processing rate
- Set up alerts for processing failures
- Track database insertion success/failure rates
- Monitor MQTT connection health

## Changelog

### Version 1.0 (2025-09-18)
- Initial specification
- Defined JSON message structure
- Added database schema recommendations
- Included integration examples

## Contact and Support

For questions about this specification or the alert monitoring system:
- Repository: https://github.com/naman113/AVD_v1
- Branch: config-from-table
