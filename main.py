import time
import logging
import json
from typing import Dict, Any
from unified_ingestor.core.config_loader import ConfigLoader
from unified_ingestor.core.db import DB
from unified_ingestor.core.mqtt_hub import MQTTHub
from unified_ingestor.core.router import Router
from unified_ingestor.core.config_store import ConfigStore
from collections import defaultdict

# When a new DeviceID is seen, we could persist to config; this version logs to stdout for clarity.

def build_subs(hub: MQTTHub, router: Router, cfg: Dict[str, Any], counters):
    hub.clear_all()
    routes = cfg.get('routes', [])
    mqtt_servers = cfg.get('mqtt_servers', {})

    store = ConfigStore('unified_ingestor/unified_config.yml')

    def mk_handler(rule: Dict[str, Any] | None, allow_persist: bool):
        def _h(topic: str, data: Dict[str, Any]):
            info = router.route(topic, data, rule)
            # auto-discover: persist mapping for specific DeviceID if not present
            try:
                dev = None
                if isinstance(data, dict):
                    dev = data.get('DeviceID')
                    if isinstance(dev, list):
                        dev = dev[0]
                if dev is not None:
                    # increment counters
                    try:
                        counters[topic][str(dev)] += 1
                    except Exception:
                        pass
                    if False:  # DISABLED: Auto-persistence was corrupting config file
                        store.persist_device_mapping(cfg, topic, str(dev), info.get('pattern', 'auto'), info.get('table', ''))
            except Exception:
                pass
        return _h

    for r in routes:
        topic = r['topic']
        
        # Require explicit mqtt_server reference - no fallback to defaults
        mqtt_server_name = r.get('mqtt_server')
        if not mqtt_server_name:
            logging.warning(f"[CFG] Route '{topic}' missing mqtt_server reference - skipping")
            continue
            
        if mqtt_server_name not in mqtt_servers:
            logging.error(f"[CFG] Route '{topic}' references unknown mqtt_server '{mqtt_server_name}' - skipping")
            continue
            
        # Get server configuration
        server_config = mqtt_servers[mqtt_server_name]
        
        # Build connection with route-level overrides taking priority
        conn = {
            'broker': r.get('broker', server_config.get('broker')),
            'port': r.get('port', server_config.get('port', 8883)),
            'username': r.get('username', server_config.get('username')),
            'password': r.get('password', server_config.get('password')),
            'ca_cert': r.get('ca_cert', server_config.get('ca_cert')),
            'client_id_prefix': server_config.get('client_id_prefix', 'UnifiedIngestor'),
            'keepalive': server_config.get('keepalive', 60),
        }
        
        # Validate required connection parameters
        if not conn.get('broker'):
            logging.error(f"[CFG] Route '{topic}' missing broker configuration - skipping")
            continue
            
        try:
            rules = r.get('device_ids', [])
            allow_persist = bool(r.get('auto_discover', False))
            if not rules:
                hub.add_sub(topic, '*', mk_handler(None, allow_persist), conn)
                logging.info(f"[CFG] Added subscription: topic={topic} server={mqtt_server_name} broker={conn['broker']}")
                continue
            for rule in rules:
                device_pat = rule.get('pattern', '*')
                hub.add_sub(topic, None if device_pat == '*' else device_pat, mk_handler(rule, allow_persist), conn)
            logging.info(f"[CFG] Added subscription: topic={topic} server={mqtt_server_name} broker={conn['broker']} rules={len(rules)}")
        except Exception as e:
            logging.error(f"[CFG] Failed to setup subscription for topic={topic} server={mqtt_server_name}: {e}")
            # Continue with other routes even if this one fails


def main():
    logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
    cfg_loader = ConfigLoader('unified_ingestor/unified_config.yml', reload_seconds=15)
    cfg = cfg_loader.get()
    db = DB(cfg['database']['uri'])
    router = Router(db, cfg.get('patterns', []))
    # No mqtt_defaults - each route must specify mqtt_server
    hub = MQTTHub({})
    counters = defaultdict(lambda: defaultdict(int))

    build_subs(hub, router, cfg, counters)

    def on_change(new_cfg):
        logging.info('[CFG] Reload detected; rebuilding subscriptions')
        build_subs(hub, router, new_cfg, counters)
    cfg_loader.on_change(on_change)

    try:
        last_log = 0
        while True:
            time.sleep(1)
            now = time.time()
            if now - last_log >= 10:
                subs = hub.get_subscriptions()
                # summarize subscriptions
                topics = sorted(set(t for (t, _dev, _q) in subs))
                logging.info('[STATS] Subscribed topics: %s', topics)
                # shallow copy counters to avoid holding references too long
                snapshot = {t: dict(devs) for t, devs in counters.items()}
                logging.info('[STATS] Device entry counts: %s', snapshot)
                last_log = now
    except KeyboardInterrupt:
        pass
    finally:
        hub.stop_all()


if __name__ == '__main__':
    main()
