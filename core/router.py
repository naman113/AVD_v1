from typing import Dict, Any, Optional
import hashlib
import re
import logging
from .db import DB
from .patterns import PatternMatcher

class Router:
    def __init__(self, db: DB, patterns: list[Dict[str, Any]]):
        self.db = db
        self.matcher = PatternMatcher(patterns)
        # in-memory table cache
        self._tables = {}
        # name -> pattern map for quick lookup
        self._pattern_by_name = {p.get('name'): p for p in patterns if p.get('name')}

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
        device_id = payload.get('DeviceID', 'unknown') if isinstance(payload, dict) else 'unknown'
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
            preferred = self._format_table((rule.get('table_override') if rule else None), topic) \
                or f"{safe}_{data_cols_count}"
            table_name = self.db.resolve_compatible_table(preferred, auto_columns)
            self._ensure(table_name, auto_columns)
            self.db.ensure_columns(table_name, auto_columns)
            row = PatternMatcher.to_row_auto(topic, payload)
            self.db.insert(self.db.meta.tables[table_name], row)
            
            # Log successful ingestion
            device_id = payload.get('DeviceID', 'unknown') if isinstance(payload, dict) else 'unknown'
            logging.info(f"[ROUTER] Inserted row: topic={topic} device={device_id} table={table_name} pattern=auto columns={len(auto_columns)}")
            
            return {'table': table_name, 'pattern': 'auto', 'columns': auto_columns}

        # Known pattern
        if pattern.get('columns') == 'auto':
            columns = auto_columns
            data_cols_count = len([k for k in columns.keys() if k != 'topic'])
            preferred = self._format_table((rule.get('table_override') if rule else None), topic) \
                or f"{safe}_{data_cols_count}"
            resolved = self.db.resolve_compatible_table(preferred, columns)
            self._ensure(resolved, columns)
            self.db.ensure_columns(resolved, columns)
            row = PatternMatcher.to_row_auto(topic, payload)
            self.db.insert(self.db.meta.tables[resolved], row)
            
            # Log successful ingestion
            device_id = payload.get('DeviceID', 'unknown') if isinstance(payload, dict) else 'unknown'
            logging.info(f"[ROUTER] Inserted row: topic={topic} device={device_id} table={resolved} pattern={pattern_name or 'auto'} columns={len(columns)}")
            
            return {'table': resolved, 'pattern': pattern_name or 'auto', 'columns': columns}
        else:
            columns = pattern.get('columns', {})
            data_cols_count = len([k for k in columns.keys() if k != 'topic']) if isinstance(columns, dict) else 0
            preferred = self._format_table((rule.get('table_override') if rule else None), topic) \
                or f"{safe}_{data_cols_count}"
            resolved = self.db.resolve_compatible_table(preferred, columns)
            self._ensure(resolved, columns)
            row = PatternMatcher.to_row_auto(topic, payload)
            self.db.insert(self.db.meta.tables[resolved], row)
            return {'table': resolved, 'pattern': pattern_name or 'explicit', 'columns': columns}
