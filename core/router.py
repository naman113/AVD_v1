from typing import Dict, Any, Optional
import hashlib
import re
import logging
from .db import DB
from .patterns import PatternMatcher
from .device_mapper import DeviceMapper
from .table_manager import TableManager

class Router:
    def __init__(self, db: DB, patterns: list[Dict[str, Any]], device_mapper: Optional[DeviceMapper] = None):
        self.db = db
        self.matcher = PatternMatcher(patterns)
        self.device_mapper = device_mapper
        self.table_manager = TableManager(db.engine)
        # in-memory table cache
        self._tables = {}
        # name -> pattern map for quick lookup
        self._pattern_by_name = {p.get('name'): p for p in patterns if p.get('name')}

    def _extract_device_id(self, payload: Any, row_data: Dict[str, Any] = None) -> Optional[str]:
        """Extract device ID from payload or row data, handling different message structures."""
        # Method 1: Direct DeviceID in payload (most common)
        if isinstance(payload, dict) and 'DeviceID' in payload:
            return str(payload['DeviceID'])
        
        # Method 2: DeviceID in nested 'd' structure
        if isinstance(payload, dict) and 'd' in payload and isinstance(payload['d'], dict):
            if 'DeviceID' in payload['d']:
                device_val = payload['d']['DeviceID']
                # Handle list values (take first element)
                if isinstance(device_val, list) and device_val:
                    return str(device_val[0])
                return str(device_val)
        
        # Method 3: Look in the row data that will be inserted (after auto-derivation)
        if row_data and 'DeviceID' in row_data:
            return str(row_data['DeviceID'])
        
        # Method 4: Try case-insensitive search in payload
        if isinstance(payload, dict):
            for key, value in payload.items():
                if key.lower() in ['deviceid', 'device_id', 'device']:
                    return str(value)
        
        # Method 5: Look in nested 'd' structure with case-insensitive search
        if isinstance(payload, dict) and 'd' in payload and isinstance(payload['d'], dict):
            for key, value in payload['d'].items():
                if key.lower() in ['deviceid', 'device_id', 'device']:
                    # Handle list values (take first element)
                    if isinstance(value, list) and value:
                        return str(value[0])
                    return str(value)
        
        return None

    def _format_table(self, table_tpl: Optional[str], topic: str) -> Optional[str]:
        if not table_tpl:
            return None
        try:
            safe_topic = re.sub(r"[^a-z0-9_]+", "_", topic.lower())
            safe_topic = re.sub(r"_+", "_", safe_topic).strip('_')
            return table_tpl.format(topic=safe_topic)
        except Exception:
            # fallback simple replace
            safe_topic = re.sub(r"[^a-z0-9_]+", "_", topic.lower())
            safe_topic = re.sub(r"_+", "_", safe_topic).strip('_')
            return table_tpl.replace('{topic}', safe_topic)

    def _safe_topic(self, topic: str) -> str:
        safe_topic = re.sub(r"[^a-z0-9_]+", "_", topic.lower())
        return re.sub(r"_+", "_", safe_topic).strip('_')

    def _ensure(self, table: str, columns: Dict[str, Any]):
        key = table
        existing = self._tables.get(key)
        if existing is None:
            self.db.ensure_table(table, columns)
            self._tables[key] = columns
            return
        # note: online column add not implemented to keep it simple; assume stable schema

    def _shape_table_name(self, base: str, columns: Dict[str, Any]) -> str:
        """Deterministically derive a table name variant from the payload column shape.
        Uses a short hash of sorted (name,type) pairs to segregate incompatible shapes.
        """
        # Build stable signature
        items = tuple((k, str(columns[k])) for k in sorted(columns.keys()))
        sig = hashlib.sha1(repr(items).encode('utf-8')).hexdigest()[:8]
        # Avoid duplicate suffix if already present
        if base.endswith(sig) or base.endswith(f"__s_{sig}"):
            return base
        return f"{base}__s_{sig}"

    def route(self, topic: str, payload: Any, rule: Optional[Dict[str, Any]] = None):
        # Log incoming message to router
        device_id = self._extract_device_id(payload) or 'unknown'
        logging.info(f"[ROUTER] Processing message: topic={topic} device={device_id}")
        
        # Match pattern
        # Determine base match
        matched_name, matched_pattern = self.matcher.match(payload)
        pattern_name = matched_name
        pattern = matched_pattern
        # Allow rule to override pattern selection, including forcing 'auto'
        if rule and rule.get('pattern_name'):
            override = rule['pattern_name']
            pattern_name = override
            if override == 'auto':
                pattern = None
            else:
                pattern = self._pattern_by_name.get(override, matched_pattern)

        # Derive columns once for auto behavior
        auto_columns = PatternMatcher.derive_columns_auto(topic, payload)
        safe = self._safe_topic(topic)

        # AUTO mode (no pattern)
        if not pattern:
            data_cols_count = len([k for k in auto_columns.keys() if k != 'topic'])
            
            # Use table manager to determine table name
            table_config = {
                'table_override': rule.get('table_override') if rule else None,
                'auto_create': True,
                'version_on_conflict': True
            }
            
            table_name = self.table_manager.get_or_create_table_name(
                table_config=table_config,
                topic=topic,
                device_pattern=rule.get('pattern', '*') if rule else '*',
                message_structure=payload
            )
            
            self._ensure(table_name, auto_columns)
            self.db.ensure_columns(table_name, auto_columns)
            row = PatternMatcher.to_row_auto(topic, payload)
            self.db.insert(self.db.meta.tables[table_name], row)
            
            # Register device in mapper if available
            device_id = self._extract_device_id(payload, row)
            if self.device_mapper and device_id:
                self.device_mapper.register_device(
                    topic=topic,
                    device_id=device_id,
                    table_name=table_name,
                    pattern_name='auto'
                )
            
            # Log successful ingestion
            log_device_id = device_id or 'unknown'
            logging.info(f"[ROUTER] Inserted row: topic={topic} device={log_device_id} table={table_name} pattern=auto columns={len(auto_columns)}")
            
            return {'table': table_name, 'pattern': 'auto', 'columns': auto_columns}

        # Known pattern
        if pattern.get('columns') == 'auto':
            columns = auto_columns
            data_cols_count = len([k for k in columns.keys() if k != 'topic'])
            
            # Use table manager to determine table name
            table_config = {
                'table_override': rule.get('table_override') if rule else None,
                'auto_create': True,
                'version_on_conflict': True
            }
            
            resolved = self.table_manager.get_or_create_table_name(
                table_config=table_config,
                topic=topic,
                device_pattern=rule.get('pattern', '*') if rule else '*',
                message_structure=payload
            )
            
            self._ensure(resolved, columns)
            self.db.ensure_columns(resolved, columns)
            row = PatternMatcher.to_row_auto(topic, payload)
            self.db.insert(self.db.meta.tables[resolved], row)
            
            # Register device in mapper if available
            device_id = self._extract_device_id(payload, row)
            if self.device_mapper and device_id:
                self.device_mapper.register_device(
                    topic=topic,
                    device_id=device_id,
                    table_name=resolved,
                    pattern_name=pattern_name or 'auto'
                )
            
            # Log successful ingestion
            log_device_id = device_id or 'unknown'
            logging.info(f"[ROUTER] Inserted row: topic={topic} device={log_device_id} table={resolved} pattern={pattern_name or 'auto'} columns={len(columns)}")
            
            return {'table': resolved, 'pattern': pattern_name or 'auto', 'columns': columns}
        else:
            columns = pattern.get('columns', {})
            data_cols_count = len([k for k in columns.keys() if k != 'topic']) if isinstance(columns, dict) else 0
            
            # Use table manager for explicit patterns too
            table_config = {
                'table_override': rule.get('table_override') if rule else None,
                'auto_create': True,
                'version_on_conflict': True
            }
            
            resolved = self.table_manager.get_or_create_table_name(
                table_config=table_config,
                topic=topic,
                device_pattern=rule.get('pattern', '*') if rule else '*',
                message_structure=payload
            )
            
            self._ensure(resolved, columns)
            row = PatternMatcher.to_row_auto(topic, payload)
            self.db.insert(self.db.meta.tables[resolved], row)
            
            # Register device in mapper if available
            device_id = self._extract_device_id(payload, row)
            if self.device_mapper and device_id:
                self.device_mapper.register_device(
                    topic=topic,
                    device_id=device_id,
                    table_name=resolved,
                    pattern_name=pattern_name or 'explicit'
                )
            
            return {'table': resolved, 'pattern': pattern_name or 'explicit', 'columns': columns}
