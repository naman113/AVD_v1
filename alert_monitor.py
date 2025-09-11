#!/usr/bin/env python3
"""
Simple MQTT Alert Monitor
Subscribes to MQTT topics, checks thresholds, and publishes alerts
"""

import json
import time
import logging
from typing import Dict, Any
from datetime import datetime
from core.config_loader import ConfigLoader
from core.mqtt_hub import MQTTHub, SingleClient

class AlertMonitor:
    def __init__(self, config_path: str = 'config.yml'):
        self.config_loader = ConfigLoader(config_path)
        self.hub = MQTTHub({})
        self.alert_publisher = None
        
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
        alerts_config = cfg.get('alerts', {})
        thresholds = alerts_config.get('thresholds', {})
        
        for route in routes:
            topic = route['topic']
            
            # Only monitor topics that have thresholds defined
            if topic not in thresholds:
                continue
            
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
            
            # Subscribe to topic
            self.hub.add_sub(topic, None, lambda t, d, topic=topic: self.process_message(topic, d), conn)
            self.logger.info(f"Subscribed to: {topic} on {conn['broker']}")

    def process_message(self, topic: str, data: Any):
        """Process message and check thresholds"""
        if not isinstance(data, dict):
            return
            
        device_id = str(data.get('DeviceID', 'unknown'))
        
        # Get thresholds from config
        cfg = self.config_loader.get()
        alerts_config = cfg.get('alerts', {})
        thresholds = alerts_config.get('thresholds', {})
        topic_thresholds = thresholds.get(topic, {})
        
        if not topic_thresholds:
            return
        
        # Get device-specific or default thresholds
        device_thresholds = topic_thresholds.get(f'device_{device_id}', topic_thresholds.get('default', {}))
        
        violations = []
        
        # Check each parameter against thresholds
        for param, value in data.items():
            if param not in device_thresholds:
                continue
                
            try:
                # Convert value to float for comparison
                numeric_value = float(value)
                threshold = device_thresholds[param]
                low = threshold.get('low')
                high = threshold.get('high')
                
                if low is not None and numeric_value < low:
                    violations.append({
                        'parameter': param,
                        'value': numeric_value,
                        'threshold': low,
                        'type': 'low'
                    })
                elif high is not None and numeric_value > high:
                    violations.append({
                        'parameter': param,
                        'value': numeric_value,
                        'threshold': high,
                        'type': 'high'
                    })
                    
            except (ValueError, TypeError):
                continue
        
        # Publish alert if violations found
        if violations:
            self.publish_alert(topic, device_id, violations)

    def publish_alert(self, topic: str, device_id: str, violations: list):
        """Publish alert message"""
        if not self.alert_publisher:
            return
            
        alert = {
            'timestamp': datetime.now().isoformat(),
            'topic': topic,
            'device_id': device_id,
            'violations': violations
        }
        
        try:
            cfg = self.config_loader.get()
            alert_topic = cfg.get('alerts', {}).get('alert_topic', 'alerts/monitoring')
            
            self.alert_publisher.client.publish(
                alert_topic,
                json.dumps(alert),
                qos=1
            )
            self.logger.info(f"Alert: {alert_topic} - {topic}/{device_id} - {len(violations)} violations")
        except Exception as e:
            self.logger.error(f"Failed to publish alert: {e}")

    def run(self):
        """Main loop"""
        self.logger.info("Starting MQTT Alert Monitor...")
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self.logger.info("Stopping...")
        finally:
            self.hub.stop_all()
            if self.alert_publisher:
                self.alert_publisher.stop()

if __name__ == '__main__':
    monitor = AlertMonitor()
    monitor.run()
