# Alert Message Template

This document describes the format of alert messages published by the MQTT Alert Monitor system to the `alerts/monitoring` topic.

## Overview

When the alert monitor detects threshold violations, it publishes JSON-formatted alert messages that can be consumed by downstream systems for notifications, logging, or further processing.

## Message Structure

### Topic
- **Published to**: `alerts/monitoring` (configurable in `config.yml`)
- **QoS**: 1 (at least once delivery)
- **Retain**: false

### JSON Schema

```json
{
  "timestamp": "2025-09-10T14:32:15.123456",
  "topic": "string",
  "device_id": "string",
  "violations": [
    {
      "parameter": "string",
      "value": number,
      "threshold": number,
      "type": "string"
    }
  ]
}
```

## Field Descriptions

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `timestamp` | string | ISO 8601 formatted timestamp when alert was generated | `"2025-09-10T14:32:15.123456"` |
| `topic` | string | Original MQTT topic where violation was detected | `"Gree1"`, `"Pengvin1"`, `"Energy1"` |
| `device_id` | string | Device identifier from the original message | `"22"`, `"96"`, `"energy_meter_001"` |
| `violations` | array | List of threshold violations detected | See violation object below |

### Violation Object

| Field | Type | Description | Example |
|-------|------|-------------|---------|
| `parameter` | string | Parameter name that violated threshold | `"P0"`, `"P1"`, `"Temperature"` |
| `value` | number | Actual value that triggered the violation | `25.7`, `150`, `0` |
| `threshold` | number | Threshold value that was exceeded/undercut | `0`, `100`, `30.0` |
| `type` | string | Type of violation: `"low"` or `"high"` | `"low"`, `"high"` |

## Example Messages

### Single Parameter Violation

```json
{
  "timestamp": "2025-09-10T14:32:15.123456",
  "topic": "Gree1",
  "device_id": "22",
  "violations": [
    {
      "parameter": "P0",
      "value": 25.5,
      "threshold": 0,
      "type": "high"
    }
  ]
}
```

### Multiple Parameter Violations

```json
{
  "timestamp": "2025-09-10T14:32:15.789012",
  "topic": "Energy1", 
  "device_id": "energy_meter_001",
  "violations": [
    {
      "parameter": "P0",
      "value": -5.2,
      "threshold": 0,
      "type": "low"
    },
    {
      "parameter": "P1",
      "value": 150.7,
      "threshold": 100,
      "type": "high"
    }
  ]
}
```

### Complex Device Violation

```json
{
  "timestamp": "2025-09-10T14:33:01.456789",
  "topic": "Pengvin1",
  "device_id": "sensor_103",
  "violations": [
    {
      "parameter": "Temperature",
      "value": 85.3,
      "threshold": 80.0,
      "type": "high"
    },
    {
      "parameter": "Humidity",
      "value": 15.2,
      "threshold": 20.0,
      "type": "low"
    },
    {
      "parameter": "Pressure",
      "value": 1025.8,
      "threshold": 1020.0,
      "type": "high"
    }
  ]
}
```

## Subscribing to Alerts

### Python Example (using paho-mqtt)

```python
import json
import paho.mqtt.client as mqtt

def on_connect(client, userdata, flags, rc):
    print(f"Connected with result code {rc}")
    client.subscribe("alerts/monitoring")

def on_message(client, userdata, msg):
    try:
        alert = json.loads(msg.payload.decode())
        
        # Process alert
        device_id = alert['device_id']
        topic = alert['topic']
        violation_count = len(alert['violations'])
        
        print(f"ALERT: {topic}/{device_id} - {violation_count} violations")
        
        for violation in alert['violations']:
            param = violation['parameter']
            value = violation['value']
            threshold = violation['threshold']
            vtype = violation['type']
            print(f"  {param}: {value} ({vtype} threshold {threshold})")
            
    except json.JSONDecodeError as e:
        print(f"Failed to parse alert message: {e}")

client = mqtt.Client()
client.on_connect = on_connect
client.on_message = on_message

# Configure connection (adjust for your setup)
client.username_pw_set("your_username", "your_password")
client.connect("your_broker", 8883, 60)
client.loop_forever()
```


## Configuration Reference

The alert monitoring behavior is controlled by the `alerts` section in `config.yml`:

```yaml
alerts:
  enabled: true
  mqtt_server: alert_publisher
  alert_topic: alerts/monitoring
  thresholds:
    Pengvin1:
      default:
        P0: {low: 0, high: 100}
        P1: {low: 0, high: 100}
    Gree1:
      default:
        P0: {low: 0, high: 0}  # Any non-zero triggers alert
    Energy1:
      default:
        P0: {low: 0, high: 1000}
        P1: {low: 0, high: 500}
```

## Alert Frequency

- **Real-time**: Alerts are published immediately when violations are detected
- **Per-message**: Each MQTT message that contains violations triggers an alert
- **No aggregation**: Multiple violations in the same message are included in a single alert
- **No rate limiting**: Currently no built-in rate limiting (consider implementing in subscriber)

## Integration Considerations

### Downstream Processing
- **Database logging**: Store alerts in a database for historical analysis
- **Notification systems**: Send emails, SMS, or push notifications
- **Dashboard updates**: Real-time dashboard alerts
- **Escalation**: Implement escalation rules based on violation severity

### Error Handling
- Handle JSON parsing errors gracefully
- Implement reconnection logic for MQTT subscriber
- Consider dead letter queue for failed alert processing

### Performance
- Alerts are published with QoS 1 for reliability
- Consider implementing alert deduplication if needed
- Monitor alert volume to prevent system overload

## Version History

- **v1.0** (2025-09-10): Initial alert message template with basic threshold violations
