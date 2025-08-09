#!/usr/bin/env python3
"""
Interactive Gree1 topic simulator.
- Reads broker settings from unified_ingestor/unified_config.yml
- Creates/publishes random CT payloads to topic "Gree1" wit        for n in range(count):
            # Ensure connection before each publish
            client = ensure_connected(client, b)
            
            payload = build_payload(device_id, p_fields)
            import json
            body = json.dumps(payload)
            
            # Publish with retry logic
            max_publish_retries = 3
            published = False
            
            for retry in range(max_publish_retries):
                rc = client.publish(topic, payload=body, qos=0, retain=False)[0]
                if rc == mqtt.MQTT_ERR_SUCCESS:
                    print(f"Published to {topic}: DeviceID={payload['DeviceID']} fields={len(payload)-3}")
                    published = True
                    break
                else:
                    print(f"Publish failed rc={rc}, retrying... ({retry + 1}/{max_publish_retries})")
                    if rc in [mqtt.MQTT_ERR_NO_CONN, mqtt.MQTT_ERR_CONN_LOST]:
                        # Connection issue, force reconnect
                        try:
                            client.disconnect()
                        except:
                            pass
                        client = connect_client(b)
                    time.sleep(1)
            
            if not published:
                print(f"Failed to publish message {n+1} after {max_publish_retries} attempts")
            
            if interval_ms > 0 and n+1 < count:
                time.sleep(interval_ms/1000.0).85
- Random P fields count between 3 and 15 (P0..Pn)
- Keeps Date/Time like real devices (DDMMYYYY / HHMMSS as strings)
- TLS & auth are taken from mqtt_defaults; per-route creds override if present

Usage: run and follow prompts.
"""
from __future__ import annotations
import os
import ssl
import sys
import time
import random
import string
from dataclasses import dataclass
import argparse
from typing import Any, Dict, Optional

import yaml
import paho.mqtt.client as mqtt

ROOT = os.path.dirname(os.path.dirname(__file__))
CONFIG_PATH = os.path.join(ROOT, "unified_config.yml")

@dataclass
class BrokerCfg:
    host: str
    port: int
    username: Optional[str]
    password: Optional[str]
    ca_cert: Optional[str]
    client_id_prefix: str
    keepalive: int


def load_config() -> Dict[str, Any]:
    with open(CONFIG_PATH, "r") as f:
        return yaml.safe_load(f)


def select_broker_for_topic(cfg: Dict[str, Any], topic: str) -> BrokerCfg:
    # Find the route for this topic
    route = None
    for r in cfg.get("routes", []):
        if r.get("topic") == topic:
            route = r
            break
    
    if not route:
        raise ValueError(f"No route found for topic '{topic}'")
    
    # Get mqtt_server reference
    mqtt_server_name = route.get("mqtt_server")
    if not mqtt_server_name:
        raise ValueError(f"Route for topic '{topic}' missing mqtt_server reference")
    
    # Get server configuration
    mqtt_servers = cfg.get("mqtt_servers", {})
    if mqtt_server_name not in mqtt_servers:
        raise ValueError(f"Unknown mqtt_server '{mqtt_server_name}' referenced by topic '{topic}'")
    
    server_config = mqtt_servers[mqtt_server_name]
    
    # Route-level overrides take priority
    username = route.get("username", server_config.get("username"))
    password = route.get("password", server_config.get("password"))
    
    return BrokerCfg(
        host=route.get("broker", server_config.get("broker", "localhost")),
        port=int(route.get("port", server_config.get("port", 1883))),
        username=username,
        password=password,
        ca_cert=route.get("ca_cert", server_config.get("ca_cert")),
        client_id_prefix=server_config.get("client_id_prefix", "Sim"),
        keepalive=int(server_config.get("keepalive", 60)),
    )


def build_payload(device_id: int, p_fields: int) -> Dict[str, Any]:
    # Enforce p_fields between 3 and 15
    p_fields = max(3, min(15, p_fields))
    # Simulate values as strings, like devices do
    now = time.localtime()
    date_str = f"{now.tm_mday:02d}{now.tm_mon:02d}{now.tm_year:04d}"
    time_str = f"{now.tm_hour:02d}{now.tm_min:02d}{now.tm_sec:02d}"
    payload: Dict[str, Any] = {
        "DeviceID": str(device_id),
        "Date": int(date_str),  # many devices send Date as int
        "Time": time_str,
    }
    for i in range(p_fields):
        # create realistic small integers in string form
        payload[f"P{i}"] = str(random.randint(0, 500))
    return payload


def connect_client(b: BrokerCfg) -> mqtt.Client:
    cid = f"{b.client_id_prefix}-sim-{''.join(random.choices(string.ascii_lowercase+string.digits, k=6))}"
    client = mqtt.Client(client_id=cid, clean_session=True)
    if b.username:
        client.username_pw_set(b.username, b.password)
    if b.ca_cert and os.path.exists(b.ca_cert):
        context = ssl.create_default_context()
        context.load_verify_locations(cafile=b.ca_cert)
        client.tls_set_context(context)
        client.tls_insecure_set(False)
    
    # Connect with retry logic
    max_retries = 3
    for attempt in range(max_retries):
        try:
            print(f"Connecting to {b.host}:{b.port}... (attempt {attempt + 1})")
            client.connect(b.host, b.port, b.keepalive)
            client.loop_start()  # Start the network loop
            time.sleep(1)  # Give it a moment to establish
            return client
        except Exception as e:
            print(f"Connection attempt {attempt + 1} failed: {e}")
            if attempt == max_retries - 1:
                raise
            time.sleep(2)
    
    return client

def ensure_connected(client: mqtt.Client, b: BrokerCfg) -> mqtt.Client:
    """Ensure client is connected, reconnect if needed"""
    if not client.is_connected():
        print("Client disconnected, reconnecting...")
        try:
            client.disconnect()
        except:
            pass
        return connect_client(b)
    return client


def prompt_int(prompt: str, default: Optional[int] = None, min_v: Optional[int] = None, max_v: Optional[int] = None) -> int:
    while True:
        raw = input(f"{prompt}{' ['+str(default)+']' if default is not None else ''}: ").strip()
        if not raw and default is not None:
            return default
        try:
            val = int(raw)
            if min_v is not None and val < min_v:
                print(f"Must be >= {min_v}")
                continue
            if max_v is not None and val > max_v:
                print(f"Must be <= {max_v}")
                continue
            return val
        except ValueError:
            print("Enter a number.")


def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Gree1 topic simulator")
    ap.add_argument("--device", type=int, help="DeviceID (35..85)")
    ap.add_argument("--pfields", type=int, help="Number of P fields 3..15")
    ap.add_argument("--count", type=int, default=0, help="Number of messages to send (non-interactive if >0)")
    ap.add_argument("--interval", type=int, default=0, help="Interval in ms between messages (non-interactive)")
    return ap.parse_args()


def main() -> int:
    args = parse_args()
    cfg = load_config()
    topic = "Gree1"
    b = select_broker_for_topic(cfg, topic)

    print("Gree1 simulator. DeviceIDs will be 35..85. Press Ctrl+C to stop.")
    client = connect_client(b)

    # Non-interactive one-shot when --count > 0
    if args.count and args.count > 0:
        device_id = args.device if args.device is not None else random.randint(35, 85)
        p_fields = args.pfields if args.pfields is not None else random.randint(3, 15)
        count = max(1, min(1000, int(args.count)))
        interval_ms = max(0, min(60000, int(args.interval)))
        for n in range(count):
            # Ensure connection before each publish
            client = ensure_connected(client, b)
            
            payload = build_payload(device_id, p_fields)
            import json
            body = json.dumps(payload)
            
            # Publish with retry logic
            max_publish_retries = 3
            published = False
            
            for retry in range(max_publish_retries):
                rc = client.publish(topic, payload=body, qos=0, retain=False)[0]
                if rc == mqtt.MQTT_ERR_SUCCESS:
                    print(f"Published to {topic}: DeviceID={payload['DeviceID']} fields={len(payload)-3}")
                    published = True
                    break
                else:
                    print(f"Publish failed rc={rc}, retrying... ({retry + 1}/{max_publish_retries})")
                    if rc in [mqtt.MQTT_ERR_NO_CONN, mqtt.MQTT_ERR_CONN_LOST]:
                        # Connection issue, force reconnect
                        try:
                            client.disconnect()
                        except:
                            pass
                        client = connect_client(b)
                    time.sleep(1)
            
            if not published:
                print(f"Failed to publish message {n+1} after {max_publish_retries} attempts")
            
            if interval_ms > 0 and n+1 < count:
                time.sleep(interval_ms/1000.0)
        client.disconnect()
        return 0

    # Interactive loop
    while True:
        device_id = prompt_int("DeviceID (35..85)", default=random.randint(35, 85), min_v=35, max_v=85)
        p_fields = prompt_int("How many P fields? (3..15)", default=random.randint(3, 15), min_v=3, max_v=15)
        count = prompt_int("How many messages to send?", default=1, min_v=1, max_v=1000)
        interval_ms = prompt_int("Interval ms between messages", default=500, min_v=0, max_v=60000)

        for n in range(count):
            payload = build_payload(device_id, p_fields)
            body = yaml.safe_dump(payload, default_flow_style=False)
            # Devices typically send JSON; our ingestor parses JSON or key/value YAML-like?
            # We’ll publish JSON to be safe.
            import json
            body = json.dumps(payload)
            rc = client.publish(topic, payload=body, qos=0, retain=False)[0]
            if rc != mqtt.MQTT_ERR_SUCCESS:
                print(f"Publish failed rc={rc}")
            else:
                print(f"Published to {topic}: DeviceID={payload['DeviceID']} fields={len(payload)-3}")
            if interval_ms > 0 and n+1 < count:
                time.sleep(interval_ms/1000.0)

        again = input("Send more? [Y/n]: ").strip().lower()
        if again and again.startswith('n'):
            break

    client.disconnect()
    print("Bye.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        print("\nInterrupted.")
        try:
            sys.exit(130)
        except SystemExit:
            os._exit(130)
