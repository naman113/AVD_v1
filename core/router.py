from typing import Dict, Any, Optional
import hashlib
import re
import logging
from .db import DB
from .patterns import PatternMatcher
from .device_mapper import DeviceMapper
from .table_manager import TableManager
from .data_transformer import DataTransformer
from .interval_difference_calculator import IntervalDifferenceCalculator

class Router:
    def __init__(self, db: DB, patterns: list[Dict[str, Any]], device_mapper: Optional[DeviceMapper] = None, routes: list[Dict[str, Any]] = None):
        self.db = db
        self.matcher = PatternMatcher(patterns)
        self.device_mapper = device_mapper
        self.table_manager = TableManager(db.engine)
        self.data_transformer = DataTransformer()
        # in-memory table cache
        self._tables = {}
        # name -> pattern map for quick lookup
        self._pattern_by_name = {p.get('name'): p for p in patterns if p.get('name')}
        # in-memory cache for last readings per device/topic for difference calculation
        self._last_readings = {}  # key: f"{topic}:{device_id}" -> Dict[str, Any]
        # NEW: Interval difference calculator
        self.interval_calculator = IntervalDifferenceCalculator()
        # Store routes for interval configuration lookup
        self._routes = routes or []
        self._route_by_topic = {r.get('topic'): r for r in self._routes if r.get('topic')}

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

    def _calculate_differences(self, topic: str, device_id: str, current_row: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Calculate differences between current and last reading for the same device/topic.
        Returns None if this is the first reading (to initialize cache).
        Returns difference row if this is a subsequent reading."""
        
        cache_key = f"{topic}:{device_id}"
        
        # Debug: Log the current row structure and types
        debug_info = {}
        for key, value in current_row.items():
            debug_info[key] = f"{type(value).__name__}:{value}"
        logging.info(f"[ROUTER] DEBUG Row structure for {cache_key}: {debug_info}")
        
        # If this is the first reading for this device/topic, store it and return None
        if cache_key not in self._last_readings:
            # Store the current reading as the baseline (excluding metadata fields)
            baseline = {}
            for key, value in current_row.items():
                if key not in ['topic', 'DeviceID', 'Date', 'Time', 'ts', 'ingested_at']:
                    # Try to convert string numbers to numeric values
                    numeric_value = self._try_convert_to_numeric(value)
                    if numeric_value is not None:
                        baseline[key] = numeric_value
            
            self._last_readings[cache_key] = {
                'data': baseline,
                'metadata': {
                    'DeviceID': current_row.get('DeviceID'),
                    'Date': current_row.get('Date'),
                    'Time': current_row.get('Time'),
                    'ts': current_row.get('ts')
                }
            }
            
            logging.info(f"[ROUTER] Initialized baseline for topic={topic} device={device_id} with {len(baseline)} numeric fields: {list(baseline.keys())}")
            return None
        
        # Calculate differences for numeric fields
        last_reading = self._last_readings[cache_key]
        last_data = last_reading['data']
        diff_row = {
            'topic': topic,
            'DeviceID': current_row.get('DeviceID'),
            'Date': current_row.get('Date'),
            'Time': current_row.get('Time'),
            'ts': current_row.get('ts')
        }
        
        # Calculate differences for each numeric field
        differences_found = False
        for key, current_value in current_row.items():
            if key not in ['topic', 'DeviceID', 'Date', 'Time', 'ts', 'ingested_at']:
                # Try to convert to numeric
                numeric_current = self._try_convert_to_numeric(current_value)
                if numeric_current is not None and key in last_data:
                    difference = numeric_current - last_data[key]
                    diff_row[key] = difference
                    differences_found = True
                    
                    # Update cache with current value
                    last_data[key] = numeric_current
                elif numeric_current is not None:
                    # New field that wasn't in the baseline
                    diff_row[key] = numeric_current  # First occurrence, use raw value
                    last_data[key] = numeric_current
                    differences_found = True
        
        # Update metadata in cache
        last_reading['metadata'] = {
            'DeviceID': current_row.get('DeviceID'),
            'Date': current_row.get('Date'),
            'Time': current_row.get('Time'),
            'ts': current_row.get('ts')
        }
        
        if differences_found:
            logging.info(f"[ROUTER] Calculated differences for topic={topic} device={device_id}: {[(k,v) for k,v in diff_row.items() if k not in ['topic', 'DeviceID', 'Date', 'Time', 'ts']]}")
            return diff_row
        else:
            logging.warning(f"[ROUTER] No numeric fields found for difference calculation: topic={topic} device={device_id}")
            return None

    def _try_convert_to_numeric(self, value: Any) -> Optional[float]:
        """Try to convert a value to numeric (float). Returns None if not possible."""
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                # Remove any whitespace and try to convert
                cleaned = value.strip()
                if cleaned:
                    return float(cleaned)
            except (ValueError, TypeError):
                pass
        return None

    def _process_interval_differences(self, topic: str, device_id: str, current_row: Dict[str, Any], 
                                    rule: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """Process interval-based differences if configured for this route."""
        
        # Check if interval differences are enabled for this route
        interval_config = self._get_interval_config(rule, topic)
        if not interval_config.get('enabled', False):
            return None
        
        frequency_minutes = interval_config.get('frequency_minutes', 5)
        
        # Use the interval calculator to process the reading
        return self.interval_calculator.process_reading(
            topic=topic,
            device_id=device_id,
            current_row=current_row,
            frequency_minutes=frequency_minutes
        )
    
    def _get_interval_config(self, rule: Optional[Dict[str, Any]], topic: str = None) -> Dict[str, Any]:
        """Get interval difference configuration for the current route."""
        
        # Default configuration
        default_config = {
            'enabled': False,
            'frequency_minutes': 5,
            'table_suffix': '_interval_diff'
        }
        
        # Check route-level configuration first
        if topic and topic in self._route_by_topic:
            route = self._route_by_topic[topic]
            if 'interval_difference' in route:
                route_config = route['interval_difference']
                # Merge route config with defaults
                config = default_config.copy()
                config.update(route_config)
                
                # Override with device rule config if present
                if rule and 'interval_difference' in rule:
                    config.update(rule['interval_difference'])
                
                return config
        
        # Check if rule has interval_difference configuration
        if rule and 'interval_difference' in rule:
            rule_config = rule['interval_difference']
            # Merge rule config with defaults
            config = default_config.copy()
            config.update(rule_config)
            return config
        
        return default_config

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
        
        # Match pattern first
        matched_name, matched_pattern = self.matcher.match(payload)
        pattern_name = matched_name
        pattern = matched_pattern
        
        # Allow rule to override pattern selection, including forcing 'auto'
        if rule and rule.get('pattern_name'):
            override = rule['pattern_name']
            pattern_name = override
            if override == 'auto':
                # If forced to auto, let pattern matching decide but use auto mode
                pattern = None
            else:
                pattern = self._pattern_by_name.get(override, matched_pattern)

        # Derive columns once for auto behavior
        auto_columns = PatternMatcher.derive_columns_auto(topic, payload)
        safe = self._safe_topic(topic)

        # If we have a matched pattern, use its table template
        if pattern and pattern.get('table'):
            table_template = pattern['table']
            resolved_table = self._format_table(table_template, topic)
            
            if pattern.get('columns') == 'auto':
                columns = auto_columns
            else:
                columns = pattern.get('columns', {})
            
            # Apply transformations if defined in the pattern
            transformed_payload = payload
            if pattern.get('transformations'):
                transformed_payload = self.data_transformer.apply_transformations(
                    data=payload,
                    topic=topic,
                    transformations=pattern['transformations']
                )
                # Recalculate columns after transformation
                if pattern.get('columns') == 'auto':
                    columns = PatternMatcher.derive_columns_auto(topic, transformed_payload)
            
            self._ensure(resolved_table, columns)
            self.db.ensure_columns(resolved_table, columns)
            row = PatternMatcher.to_row_auto(topic, transformed_payload)
            
            # Extract device ID for difference calculation
            device_id = self._extract_device_id(transformed_payload, row)
            if device_id:
                # EXISTING: Calculate consecutive differences
                diff_row = self._calculate_differences(topic, device_id, row)
                if diff_row is not None:
                    # Create difference table name with "_diff" suffix
                    diff_table = f"{resolved_table}_diff"
                    self._ensure(diff_table, columns)
                    self.db.ensure_columns(diff_table, columns)
                    self.db.insert(self.db.meta.tables[diff_table], diff_row)
                    
                    # Register device in mapper if available
                    if self.device_mapper:
                        self.device_mapper.register_device(
                            topic=topic,
                            device_id=device_id,
                            table_name=diff_table,
                            pattern_name=pattern_name or 'matched'
                        )
                    
                    # Log successful difference insertion
                    logging.info(f"[ROUTER] Inserted difference row: topic={topic} device={device_id} table={diff_table} pattern={pattern_name} columns={len(columns)}")
                
                # NEW: Process interval differences if configured
                interval_diff_row = self._process_interval_differences(topic, device_id, row, rule)
                if interval_diff_row is not None:
                    # Get interval configuration for table naming
                    interval_config = self._get_interval_config(rule, topic)
                    table_suffix = interval_config.get('table_suffix', '_interval_diff')
                    interval_table = f"{resolved_table}{table_suffix}"
                    
                    # Add interval_boundary and new P0 tracking columns if not present
                    interval_columns = columns.copy()
                    if 'interval_boundary' not in interval_columns:
                        interval_columns['interval_boundary'] = 'string'
                    if 'start_P0_value' not in interval_columns:
                        interval_columns['start_P0_value'] = 'float'
                    if 'start_P0_time' not in interval_columns:
                        interval_columns['start_P0_time'] = 'string'
                    if 'end_P0_value' not in interval_columns:
                        interval_columns['end_P0_value'] = 'float'
                    if 'end_P0_time' not in interval_columns:
                        interval_columns['end_P0_time'] = 'string'
                    
                    self._ensure(interval_table, interval_columns)
                    self.db.ensure_columns(interval_table, interval_columns)
                    self.db.insert(self.db.meta.tables[interval_table], interval_diff_row)
                    
                    # Register device in mapper if available
                    if self.device_mapper:
                        self.device_mapper.register_device(
                            topic=topic,
                            device_id=device_id,
                            table_name=interval_table,
                            pattern_name=f"{pattern_name or 'matched'}_interval"
                        )
                    
                    # Log successful interval difference insertion
                    logging.info(f"[ROUTER] Inserted interval difference row: topic={topic} device={device_id} table={interval_table} pattern={pattern_name}")
                
                if diff_row is not None or interval_diff_row is not None:
                    return {'table': resolved_table, 'pattern': pattern_name, 'columns': columns}
                else:
                    # First reading - just log that baseline was set
                    logging.info(f"[ROUTER] Set baseline reading: topic={topic} device={device_id} table={resolved_table} pattern={pattern_name}")
                    return {'table': resolved_table, 'pattern': pattern_name, 'columns': columns, 'baseline': True}
            else:
                # Fallback to original behavior if no device ID found - DISABLED to prevent redundant base table creation
                # self.db.insert(self.db.meta.tables[resolved_table], row)
                logging.info(f"[ROUTER] Skipped raw row insertion (no device ID): topic={topic} table={resolved_table} pattern={pattern_name} columns={len(columns)}")
                return {'table': resolved_table, 'pattern': pattern_name, 'columns': columns}

        # AUTO mode (no pattern or forced auto)
        else:
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
            
            # Extract device ID for difference calculation
            device_id = self._extract_device_id(payload, row)
            if device_id:
                # EXISTING: Calculate consecutive differences
                diff_row = self._calculate_differences(topic, device_id, row)
                if diff_row is not None:
                    # Create difference table name with "_diff" suffix
                    diff_table = f"{table_name}_diff"
                    self._ensure(diff_table, auto_columns)
                    self.db.ensure_columns(diff_table, auto_columns)
                    self.db.insert(self.db.meta.tables[diff_table], diff_row)
                    
                    # Register device in mapper if available
                    if self.device_mapper:
                        self.device_mapper.register_device(
                            topic=topic,
                            device_id=device_id,
                            table_name=diff_table,
                            pattern_name='auto'
                        )
                    
                    # Log successful difference insertion
                    logging.info(f"[ROUTER] Inserted difference row: topic={topic} device={device_id} table={diff_table} pattern=auto columns={len(auto_columns)}")
                
                # NEW: Process interval differences if configured
                interval_diff_row = self._process_interval_differences(topic, device_id, row, rule)
                if interval_diff_row is not None:
                    # Get interval configuration for table naming
                    interval_config = self._get_interval_config(rule, topic)
                    table_suffix = interval_config.get('table_suffix', '_interval_diff')
                    interval_table = f"{table_name}{table_suffix}"
                    
                    # Add interval_boundary and new P0 tracking columns if not present
                    interval_columns = auto_columns.copy()
                    if 'interval_boundary' not in interval_columns:
                        interval_columns['interval_boundary'] = 'string'
                    if 'start_P0_value' not in interval_columns:
                        interval_columns['start_P0_value'] = 'float'
                    if 'start_P0_time' not in interval_columns:
                        interval_columns['start_P0_time'] = 'string'
                    if 'end_P0_value' not in interval_columns:
                        interval_columns['end_P0_value'] = 'float'
                    if 'end_P0_time' not in interval_columns:
                        interval_columns['end_P0_time'] = 'string'
                    
                    self._ensure(interval_table, interval_columns)
                    self.db.ensure_columns(interval_table, interval_columns)
                    self.db.insert(self.db.meta.tables[interval_table], interval_diff_row)
                    
                    # Register device in mapper if available
                    if self.device_mapper:
                        self.device_mapper.register_device(
                            topic=topic,
                            device_id=device_id,
                            table_name=interval_table,
                            pattern_name='auto_interval'
                        )
                    
                    # Log successful interval difference insertion
                    logging.info(f"[ROUTER] Inserted interval difference row: topic={topic} device={device_id} table={interval_table} pattern=auto")
                
                if diff_row is not None or interval_diff_row is not None:
                    return {'table': table_name, 'pattern': 'auto', 'columns': auto_columns}
                else:
                    # First reading - just log that baseline was set
                    logging.info(f"[ROUTER] Set baseline reading: topic={topic} device={device_id} table={table_name} pattern=auto")
                    return {'table': table_name, 'pattern': 'auto', 'columns': auto_columns, 'baseline': True}
            else:
                # Fallback to original behavior if no device ID found - DISABLED to prevent redundant base table creation
                # self.db.insert(self.db.meta.tables[table_name], row)
                logging.info(f"[ROUTER] Skipped raw row insertion (no device ID): topic={topic} table={table_name} pattern=auto columns={len(auto_columns)}")
                return {'table': table_name, 'pattern': 'auto', 'columns': auto_columns}
