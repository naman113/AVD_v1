import ssl
import os
from pathlib import Path
import json
import time
import threading
from typing import Callable, Dict, Any, Optional, Tuple
import paho.mqtt.client as mqtt
import logging
from concurrent.futures import ThreadPoolExecutor


def _topic_filter_matches(filter_str: str, topic: str) -> bool:
    """MQTT topic filter match supporting + and # per spec.
    Simple implementation: '#' matches multi-level only at end; '+' matches one level.
    """
    if filter_str == topic:
        return True
    f_parts = filter_str.split('/')
    t_parts = topic.split('/')
    i = 0
    while i < len(f_parts):
        f = f_parts[i]
        if f == '#':
            # must be last element in filter
            return i == len(f_parts) - 1
        if i >= len(t_parts):
            return False
        if f != '+':
            if f != t_parts[i]:
                return False
        i += 1
    return i == len(t_parts)

class SingleClient:
    def __init__(self, conn: Dict[str, Any]):
        self.conn = conn
        cid = f"{conn.get('client_id_prefix','Unified')}_{int(time.time())}"
        self.client = mqtt.Client(client_id=cid)
        if conn.get('username'):
            self.client.username_pw_set(conn['username'], conn.get('password'))
        if conn.get('ca_cert'):
            ca_path = str(Path(str(conn['ca_cert'])).expanduser())
            if not Path(ca_path).exists():
                raise FileNotFoundError(f"CA certificate not found at: {ca_path}")
            self.client.tls_set(
                ca_certs=ca_path,
                tls_version=ssl.PROTOCOL_TLSv1_2
            )
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message
        # (topic, optional device_id, handler, qos)
        self._handlers = []
        self._lock = threading.RLock()
        self._started = False
        # thread pool for concurrent processing
        self._max_workers = int(self.conn.get('workers', 4))
        self._executor = ThreadPoolExecutor(max_workers=self._max_workers, thread_name_prefix=f"mqtt-{cid}")
        self._log = logging.getLogger(__name__)

    def start(self):
        if not self._started:
            keepalive = int(self.conn.get('keepalive', 60))
            self.client.connect(self.conn['broker'], int(self.conn.get('port', 8883)), keepalive)
            self.client.loop_start()
            self._started = True

    def stop(self):
        if self._started:
            self.client.loop_stop()
            self.client.disconnect()
            self._started = False
        # shut down the executor after stopping network loop
        try:
            self._executor.shutdown(wait=True)
        except Exception:
            pass

    def add_sub(self, topic: str, device_id: Optional[str], handler: Callable[[str, Dict[str, Any]], None], qos: int = 1):
        with self._lock:
            dev = None if device_id in (None, '*') else str(device_id)
            q = int(qos)
            self._handlers.append((topic, dev, handler, q))
            # subscribe with qos
            self.client.subscribe((topic, q))

    def clear(self):
        with self._lock:
            for t, _, _, _ in self._handlers:
                try:
                    self.client.unsubscribe(t)
                except Exception:
                    pass
            self._handlers.clear()

    def _on_connect(self, client, userdata, flags, rc):
        # resubscribe existing topics upon reconnect
        try:
            with self._lock:
                for t, _, _, q in self._handlers:
                    try:
                        self.client.subscribe((t, q))
                    except Exception:
                        pass
        except Exception:
            pass

    def _on_message(self, client, userdata, msg):
        topic = msg.topic
        payload_text = None
        try:
            payload_text = msg.payload.decode('utf-8')
            data = json.loads(payload_text)
            # Log received message
            device_id = data.get('DeviceID', 'unknown') if isinstance(data, dict) else 'unknown'
            self._log.info(f"[MQTT] Received message: topic={topic} device={device_id} payload_size={len(payload_text)}")
        except Exception:
            # Fallback to YAML or raw string
            try:
                import yaml  # type: ignore
                data = yaml.safe_load(payload_text) if payload_text else None
                device_id = data.get('DeviceID', 'unknown') if isinstance(data, dict) else 'unknown'
                self._log.info(f"[MQTT] Received message (YAML): topic={topic} device={device_id}")
            except Exception:
                data = payload_text
                self._log.info(f"[MQTT] Received message (RAW): topic={topic} payload={str(data)[:100]}")
        
        matches: list[Tuple[Callable[[str, Dict[str, Any]], None], Optional[str]]] = []
        with self._lock:
            for t, dev, handler, _ in self._handlers:
                if _topic_filter_matches(t, topic):
                    incoming = str(data.get('DeviceID')) if isinstance(data, dict) else None
                    if dev is None or incoming == dev:
                        matches.append((handler, dev))
        # If any specific device handlers matched, drop wildcard matches to avoid double processing
        if any(dev is not None for (_h, dev) in matches):
            matches = [(h, d) for (h, d) in matches if d is not None]
        
        self._log.info(f"[MQTT] Found {len(matches)} handler(s) for topic={topic}")
        for handler, device_pattern in matches:
            try:
                self._log.info(f"[MQTT] Submitting to handler: topic={topic} device_pattern={device_pattern}")
                self._executor.submit(handler, topic, data)
            except Exception as e:
                self._log.exception("Failed to submit handler for topic=%s: %s", topic, e)

    def get_subscriptions(self) -> list[Tuple[str, Optional[str], int]]:
        with self._lock:
            return [(t, dev, q) for (t, dev, _h, q) in self._handlers]

class MQTTHub:
    def __init__(self, defaults: Dict[str, Any]):
        self.defaults = defaults.copy()
        self._clients: Dict[Tuple[Any, ...], SingleClient] = {}
        self._lock = threading.RLock()

    def _key(self, conn: Dict[str, Any]) -> Tuple[Any, ...]:
        return (conn.get('broker'), int(conn.get('port', 8883)), conn.get('username'), conn.get('password'), conn.get('ca_cert'))

    def _merge(self, overrides: Dict[str, Any]) -> Dict[str, Any]:
        out = self.defaults.copy()
        out.update({k: v for k, v in overrides.items() if v is not None})
        return out

    def add_sub(self, topic: str, device_id: Optional[str], handler: Callable[[str, Dict[str, Any]], None], conn_overrides: Optional[Dict[str, Any]] = None, qos: int = 1):
        conn = self._merge(conn_overrides or {})
        key = self._key(conn)
        with self._lock:
            client = self._clients.get(key)
            if client is None:
                client = SingleClient(conn)
                self._clients[key] = client
            client.add_sub(topic, device_id, handler, qos=int(qos))
            client.start()

    def get_subscriptions(self) -> list[Tuple[str, Optional[str], int]]:
        subs: list[Tuple[str, Optional[str], int]] = []
        with self._lock:
            for c in self._clients.values():
                subs.extend(c.get_subscriptions())
        return subs

    def clear_all(self):
        with self._lock:
            for c in self._clients.values():
                c.clear()

    def stop_all(self):
        with self._lock:
            for c in self._clients.values():
                c.stop()
            self._clients.clear()
