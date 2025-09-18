#!/usr/bin/env python3
"""
Simple MQTT Alert Monitor
Subscribes to MQTT topics, checks thresholds, and publishes alerts
"""

import json
import time
import logging
from typing import Dict, Any, Optional
from datetime import datetime
from core.config_loader import ConfigLoader
from core.mqtt_hub import MQTTHub, SingleClient
from core.threshold_manager import ThresholdManager

class AlertMonitor:
    def __init__(self, config_path: str = 'config.yml'):
        self.config_loader = ConfigLoader(config_path)
        self.hub = MQTTHub({})
        self.alert_publisher = None
        self.threshold_manager = None
        
        logging.basicConfig(level=logging.INFO, 
                          format='%(asctime)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        
        self.load_config()

    def load_config(self):
        """Load configuration and setup MQTT connections"""
        cfg = self.config_loader.get()
        
        # Check if alerts are enabled
        alerts_config = cfg.get('alerts', {})
        if not alerts_config.get('enabled', False):
            self.logger.info("Alert monitoring is disabled in configuration")
            return
        
        # Initialize threshold manager
        try:
            db_config = cfg.get('database', {})
            self.threshold_manager = ThresholdManager(db_config)
            self.logger.info("ThresholdManager initialized successfully")
        except Exception as e:
            self.logger.error(f"Failed to initialize ThresholdManager: {e}")
            self.logger.info("Continuing without database thresholds...")
        
        # Setup alert publisher using specified mqtt_server
        alert_server_name = alerts_config.get('mqtt_server')
        mqtt_servers = cfg.get('mqtt_servers', {})
        
        if alert_server_name and alert_server_name in mqtt_servers:
            server_config = mqtt_servers[alert_server_name]
            alert_conn = {
                'broker': server_config.get('broker'),
                'port': server_config.get('port', 8883),
                'username': server_config.get('username'),
                'password': server_config.get('password'),
                'ca_cert': server_config.get('ca_cert'),
                'client_id_prefix': 'AlertMonitor',
                'keepalive': server_config.get('keepalive', 60),
            }
            
            self.alert_publisher = SingleClient(alert_conn)
            self.alert_publisher.start()
            self.logger.info(f"Alert publisher connected to {alert_conn['broker']}")
        
        # Setup subscriptions for monitored topics
        self.setup_subscriptions(cfg)

    def setup_subscriptions(self, cfg):
        """Setup MQTT subscriptions"""
        self.hub.clear_all()
        routes = cfg.get('routes', [])
        mqtt_servers = cfg.get('mqtt_servers', {})
        
        for route in routes:
            topic = route['topic']
            
            # Get MQTT server config for this route
            mqtt_server_name = route.get('mqtt_server')
            if not mqtt_server_name or mqtt_server_name not in mqtt_servers:
                self.logger.warning(f"Skipping topic {topic}: invalid MQTT server")
                continue
                
            server_config = mqtt_servers[mqtt_server_name]
            conn = {
                'broker': server_config.get('broker'),
                'port': server_config.get('port', 8883),
                'username': server_config.get('username'),
                'password': server_config.get('password'),
                'ca_cert': server_config.get('ca_cert'),
                'client_id_prefix': 'AlertMonitor',
                'keepalive': server_config.get('keepalive', 60),
            }
            
            # Subscribe to topic with proper callback wrapper
            def create_handler(topic):
                def handler(mqtt_topic, data):
                    # Create a mock message object that has the required attributes
                    class MockMessage:
                        def __init__(self, topic, payload):
                            self.topic = topic
                            self.payload = json.dumps(data).encode('utf-8') if isinstance(data, dict) else str(data).encode('utf-8')
                    
                    mock_msg = MockMessage(mqtt_topic, data)
                    self.process_message(None, None, mock_msg)
                return handler
            
            self.hub.add_sub(topic, None, create_handler(topic), conn)
            self.logger.info(f"Subscribed to: {topic} on {conn['broker']}")

    def process_message(self, client, userdata, msg):
        """Process incoming MQTT message and check for violations"""
        try:
            self.logger.info(f"� PROCESSING MESSAGE:")
            self.logger.info(f"   Raw message: {msg.payload}")
            
            # Parse the message
            data = json.loads(msg.payload.decode())
            self.logger.info(f"   Parsed data: {data}")
            
            # Get device ID from message
            device_id_mqtt = data.get('device_id') or data.get('deviceId') or data.get('DeviceID')
            if not device_id_mqtt:
                self.logger.warning(f"❌ No device_id found in message")
                return
                
            self.logger.info(f"   MQTT Device ID: {device_id_mqtt}")
            
            # Get configuration and show what's loaded
            cfg = self.config_loader.get()
            self.logger.info(f"📋 LOADED CONFIG:")
            self.logger.info(f"   Company config: {cfg.get('company_config', {})}")
            self.logger.info(f"   Topic company mapping: {cfg.get('company_config', {}).get('topic_company_mapping', {})}")
            
            # Get company ID from topic mapping
            topic_company_mapping = cfg.get('company_config', {}).get('topic_company_mapping', {})
            company_id = topic_company_mapping.get(msg.topic)
            self.logger.info(f"   Topic: {msg.topic}")
            self.logger.info(f"   Mapped Company ID: {company_id}")
            
            if not company_id:
                self.logger.warning(f"❌ No company mapping found for topic: {msg.topic}")
                return
            
            # Convert device_id_mqtt to integer for database lookup
            try:
                db_device_id = int(device_id_mqtt)
                self.logger.info(f"   DB Device ID (converted): {db_device_id}")
            except (ValueError, TypeError):
                self.logger.error(f"❌ Cannot convert device_id to int: {device_id_mqtt}")
                return
            
            # Get thresholds from database
            thresholds = self.threshold_manager.get_thresholds(db_device_id, company_id)
            if not thresholds:
                self.logger.warning(f"❌ No thresholds found for device {db_device_id}")
                return
            
            # Check for violations
            violations = []
            sensor_data = data.get('data', data)  # Handle different message formats
            self.logger.info(f"🔍 CHECKING VIOLATIONS:")
            self.logger.info(f"   Sensor data: {sensor_data}")
            self.logger.info(f"   Available thresholds: {list(thresholds.keys()) if thresholds else 'None'}")
            
            # Create case-insensitive threshold lookup
            threshold_lookup = {}
            if thresholds:
                for key, value in thresholds.items():
                    threshold_lookup[key.lower()] = (key, value)  # Store both original key and config
            
            for sensor_name, sensor_value in sensor_data.items():
                # Skip non-sensor fields
                if sensor_name.lower() in ['deviceid', 'date', 'time']:
                    continue
                
                # Case-insensitive threshold lookup
                sensor_key_lower = sensor_name.lower()
                if sensor_key_lower in threshold_lookup:
                    original_key, threshold_config = threshold_lookup[sensor_key_lower]
                    
                    # Convert sensor value to float for comparison
                    try:
                        sensor_value_float = float(sensor_value)
                    except (ValueError, TypeError):
                        self.logger.warning(f"   Cannot convert sensor value to float: {sensor_name}={sensor_value}")
                        continue
                    
                    self.logger.info(f"   Checking {sensor_name} (matches {original_key}): value={sensor_value_float}, config={threshold_config}")
                    
                    # Check minimum threshold
                    if 'min_value' in threshold_config and sensor_value_float < threshold_config['min_value']:
                        violation = {
                            "parameter": sensor_name,
                            "value": sensor_value_float,
                            "threshold": threshold_config['min_value'],
                            "type": "low"
                        }
                        violations.append(violation)
                        self.logger.info(f"   🚨 VIOLATION: {sensor_name} below minimum: {sensor_value_float} < {threshold_config['min_value']}")
                    
                    # Check maximum threshold
                    if 'max_value' in threshold_config and sensor_value_float > threshold_config['max_value']:
                        violation = {
                            "parameter": sensor_name,
                            "value": sensor_value_float,
                            "threshold": threshold_config['max_value'],
                            "type": "high"
                        }
                        violations.append(violation)
                        self.logger.info(f"   🚨 VIOLATION: {sensor_name} above maximum: {sensor_value_float} > {threshold_config['max_value']}")
                else:
                    self.logger.info(f"   No threshold config for sensor: {sensor_name}")
            
            # Publish alert if violations found
            if violations:
                self.logger.info(f"🚨 FOUND {len(violations)} VIOLATIONS - Publishing alert...")
                self.publish_alert(msg.topic, device_id_mqtt, violations)
            else:
                self.logger.info(f"✅ NO VIOLATIONS DETECTED")
                
        except json.JSONDecodeError as e:
            self.logger.error(f"❌ JSON decode error: {e}")
        except Exception as e:
            self.logger.error(f"❌ Error processing message: {e}")
            import traceback
            self.logger.error(traceback.format_exc())

    def publish_alert(self, topic: str, device_id_mqtt: str, violations: list):
        """Publish alert message"""
        self.logger.info(f"📢 PUBLISHING ALERT:")
        self.logger.info(f"   Topic: {topic}")
        self.logger.info(f"   Device ID: {device_id_mqtt}")
        self.logger.info(f"   Violations: {violations}")
        
        if not self.alert_publisher:
            self.logger.error("❌ Alert publisher not initialized!")
            return
            
        # Create alert in the required format
        alert = {
            'timestamp': datetime.now().isoformat(),
            'topic': topic,
            'device_id': device_id_mqtt,
            'violations': violations
        }
        
        try:
            cfg = self.config_loader.get()
            alert_topic = cfg.get('alerts', {}).get('alert_topic', 'alerts/monitoring')
            
            self.logger.info(f"   Publishing to topic: {alert_topic}")
            self.logger.info(f"   Alert payload: {json.dumps(alert, indent=2)}")
            
            result = self.alert_publisher.client.publish(
                alert_topic,
                json.dumps(alert),
                qos=1
            )
            
            self.logger.info(f"   Publish result: {result}")
            self.logger.info(f"🚨 Alert published successfully: {alert_topic} - {topic}/{device_id_mqtt} - {len(violations)} violations")
            
        except Exception as e:
            self.logger.error(f"❌ Failed to publish alert: {e}")
            import traceback
            self.logger.error(traceback.format_exc())

    def run(self):
        """Main loop"""
        self.logger.info("Starting MQTT Alert Monitor with Database Thresholds...")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Stopping...")
        finally:
            self.hub.stop_all()
            if self.alert_publisher:
                self.alert_publisher.stop()
            if self.threshold_manager:
                self.threshold_manager.close()

if __name__ == '__main__':
    monitor = AlertMonitor()
    monitor.run()
