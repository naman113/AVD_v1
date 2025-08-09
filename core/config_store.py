import yaml
from typing import Dict, Any
from pathlib import Path

class ConfigStore:
    def __init__(self, path: str):
        # Use explicit filename to avoid collisions with other configs
        self.path = Path(path)

    def persist_device_mapping(self, cfg: Dict[str, Any], topic: str, device_id: str, pattern_name: str, table: str):
        # ALWAYS reload from disk to preserve ALL sections (mqtt_servers, database, patterns, routes)
        try:
            complete_config = yaml.safe_load(self.path.read_text()) or {}
        except FileNotFoundError:
            # If file doesn't exist, use the passed cfg as base
            complete_config = cfg.copy() if isinstance(cfg, dict) else {}
        
        # Ensure we have a valid dict structure
        if not isinstance(complete_config, dict):
            complete_config = cfg.copy() if isinstance(cfg, dict) else {}

        # Preserve critical sections
        for section in ['mqtt_servers', 'database', 'patterns']:
            if section in cfg and section not in complete_config:
                complete_config[section] = cfg[section]

        # find route by topic in the complete config
        routes = complete_config.setdefault('routes', [])
        route = next((r for r in routes if r.get('topic') == topic), None)
        if route is None:
            route = {'topic': topic, 'device_ids': []}
            routes.append(route)
        device_ids = route.setdefault('device_ids', [])
        
        # Check if device already exists to avoid duplicates
        exists = next((d for d in device_ids if str(d.get('pattern')) == str(device_id)), None)
        if not exists:
            device_ids.append({'pattern': str(device_id), 'pattern_name': pattern_name, 'table_override': table})
            # Write back the COMPLETE config preserving all sections
            text = yaml.safe_dump(complete_config, sort_keys=False, default_flow_style=False)
            self.path.write_text(text)
