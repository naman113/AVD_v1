import asyncio
import hashlib
import json
import re
import logging
from typing import Dict, List, Optional, Tuple, Any
from sqlalchemy import inspect, text
from sqlalchemy.engine import Engine
from threading import RLock

logger = logging.getLogger(__name__)

class TableManager:
    """
    Manages automatic table creation and versioning based on configuration and message structure.
    
    Features:
    - Auto-create tables based on MQTT message structure
    - Handle table versioning when schema conflicts occur
    - Auto-discovery for unnamed tables
    - Compatible with existing table_override functionality
    """
    
    def __init__(self, engine: Engine):
        self.engine = engine
        self.table_cache = {}  # Cache for existing table schemas
        self._lock = RLock()
        
    def get_or_create_table_name(self, 
                                table_config: dict, 
                                topic: str, 
                                device_pattern: str, 
                                message_structure: dict) -> str:
        """
        Get or create table based on configuration and message structure.
        Returns the actual table name to use.
        
        Args:
            table_config: Configuration for table creation
            topic: MQTT topic
            device_pattern: Device pattern from config
            message_structure: The actual MQTT message structure
            
        Returns:
            str: The table name to use for data insertion
        """
        with self._lock:
            # Handle legacy table_override format
            if 'table_override' in table_config:
                table_name = table_config['table_override']
                if table_name is None:
                    # Auto-generate table name
                    table_name = self._generate_table_name(topic, device_pattern, message_structure)
                else:
                    # Use specified table name
                    pass
            else:
                # New table_config format
                if table_config.get('name'):
                    # Explicit table name provided
                    table_name = table_config['name']
                else:
                    # Auto-generate table name
                    table_name = self._generate_table_name(topic, device_pattern, message_structure)
            
            # Check if we should auto-create tables
            auto_create = table_config.get('auto_create', True)  # Default to True for backward compatibility
            version_on_conflict = table_config.get('version_on_conflict', True)
            
            if auto_create:
                return self._ensure_table_exists(table_name, message_structure, version_on_conflict)
            else:
                return table_name
    
    def _generate_table_name(self, topic: str, device_pattern: str, message_structure: dict) -> str:
        """Generate table name based on topic, pattern and message structure hash."""
        # Sanitize topic name
        safe_topic = re.sub(r"[^a-z0-9_]+", "_", topic.lower())
        safe_topic = re.sub(r"_+", "_", safe_topic).strip('_')
        
        # Count data columns (excluding metadata)
        data_cols = self._get_data_columns(message_structure)
        data_col_count = len(data_cols)
        
        # For parameter-based routing, use the parameter count in table name
        # Check for specific parameter patterns (4, 5, 9 params)
        param_patterns = {
            4: f"{safe_topic}_4",
            5: f"{safe_topic}_5", 
            9: f"{safe_topic}_9"
        }
        
        # If we have a standard parameter count, use the predefined table name
        if data_col_count in param_patterns:
            table_name = param_patterns[data_col_count]
            logger.info(f"[TABLE_MANAGER] Using parameter-based table name: {table_name} for {data_col_count} parameters")
            return table_name
        
        # Check if we already have a similar table structure for other counts
        existing_similar = self._find_similar_table(safe_topic, data_cols)
        if existing_similar:
            logger.info(f"[TABLE_MANAGER] Found similar existing table: {existing_similar}")
            return existing_similar
        
        # Generate new table name for non-standard parameter counts
        if device_pattern != '*':
            return f"{safe_topic}_{device_pattern}_{data_col_count}"
        else:
            return f"{safe_topic}_auto_{data_col_count}"
    
    def _get_data_columns(self, message_structure: dict) -> dict:
        """Extract data columns from message structure, excluding metadata."""
        columns = {}
        
        def analyze_value(value):
            """Determine SQL type from Python value."""
            if isinstance(value, bool):
                return 'boolean'
            elif isinstance(value, int):
                return 'int'
            elif isinstance(value, float):
                return 'float'
            elif isinstance(value, str):
                return 'string'
            elif isinstance(value, (dict, list)):
                return 'json'
            else:
                return 'string'
        
        def process_dict(obj: dict, prefix: str = ''):
            """Process dictionary recursively to extract columns."""
            for key, value in obj.items():
                full_key = f"{prefix}{key}" if prefix else key
                
                # Skip metadata columns that will be added automatically
                if full_key.lower() in ['topic', 'ingested_at', 'id']:
                    continue
                
                if isinstance(value, dict):
                    # For nested objects, store as json
                    columns[full_key] = 'json'
                elif isinstance(value, list) and value:
                    # For lists, analyze first element or store as json
                    if len(value) > 0:
                        first_item = value[0]
                        if isinstance(first_item, (dict, list)):
                            columns[full_key] = 'json'
                        else:
                            columns[full_key] = analyze_value(first_item)
                    else:
                        columns[full_key] = 'json'
                else:
                    columns[full_key] = analyze_value(value)
        
        # Handle different message structures
        if isinstance(message_structure, dict):
            if 'd' in message_structure and isinstance(message_structure['d'], dict):
                # Array enveloped pattern
                process_dict(message_structure['d'])
                if 'ts' in message_structure:
                    columns['ts'] = 'string'
            else:
                # Flat structure
                process_dict(message_structure)
        
        return columns
    
    def _find_similar_table(self, topic_prefix: str, required_columns: dict) -> Optional[str]:
        """Find existing table with similar structure to avoid creating duplicates."""
        try:
            inspector = inspect(self.engine)
            all_tables = inspector.get_table_names()
            
            # Filter tables with same topic prefix
            topic_tables = [t for t in all_tables if t.startswith(topic_prefix)]
            
            for table_name in topic_tables:
                existing_schema = self._get_table_schema(table_name)
                if self._schemas_similar(existing_schema, required_columns):
                    return table_name
                    
        except Exception as e:
            logger.warning(f"[TABLE_MANAGER] Error finding similar tables: {e}")
        
        return None
    
    def _ensure_table_exists(self, table_name: str, message_structure: dict, version_on_conflict: bool) -> str:
        """Ensure table exists with correct schema, handle versioning if needed."""
        try:
            if not self._table_exists(table_name):
                # Create new table
                self._create_table(table_name, message_structure)
                return table_name
            else:
                # Check if schema matches
                existing_schema = self._get_table_schema(table_name)
                required_columns = self._get_data_columns(message_structure)
                
                if self._schemas_compatible(existing_schema, required_columns):
                    # Schemas are compatible, can add missing columns if needed
                    self._add_missing_columns(table_name, required_columns, existing_schema)
                    return table_name
                elif version_on_conflict:
                    # Create versioned table
                    versioned_name = self._create_versioned_table(table_name, message_structure)
                    return versioned_name
                else:
                    # Use existing table as-is
                    logger.warning(f"[TABLE_MANAGER] Schema mismatch for table {table_name}, using existing schema")
                    return table_name
                    
        except Exception as e:
            logger.error(f"[TABLE_MANAGER] Error ensuring table exists: {e}")
            return table_name
    
    def _table_exists(self, table_name: str) -> bool:
        """Check if table exists in database."""
        try:
            inspector = inspect(self.engine)
            return inspector.has_table(table_name)
        except Exception:
            return False
    
    def _get_table_schema(self, table_name: str) -> Dict:
        """Get existing table schema."""
        if table_name in self.table_cache:
            return self.table_cache[table_name]
            
        try:
            inspector = inspect(self.engine)
            columns = inspector.get_columns(table_name)
            
            schema_dict = {}
            for col in columns:
                col_name = col['name']
                col_type = str(col['type']).lower()
                
                # Map database types to our internal types
                if 'integer' in col_type or 'int' in col_type:
                    schema_dict[col_name] = 'int'
                elif 'float' in col_type or 'double' in col_type or 'numeric' in col_type:
                    schema_dict[col_name] = 'float'
                elif 'boolean' in col_type or 'bool' in col_type:
                    schema_dict[col_name] = 'boolean'
                elif 'json' in col_type:
                    schema_dict[col_name] = 'json'
                else:
                    schema_dict[col_name] = 'string'
            
            self.table_cache[table_name] = schema_dict
            return schema_dict
            
        except Exception as e:
            logger.error(f"[TABLE_MANAGER] Error getting table schema for {table_name}: {e}")
            return {}
    
    def _schemas_similar(self, existing: Dict, required: Dict) -> bool:
        """Check if schemas are similar enough to reuse the same table."""
        # Consider schemas similar if they have 80% column overlap
        if not existing or not required:
            return False
            
        existing_cols = set(existing.keys()) - {'id', 'ingested_at', 'topic'}
        required_cols = set(required.keys())
        
        if not existing_cols or not required_cols:
            return False
        
        intersection = existing_cols.intersection(required_cols)
        union = existing_cols.union(required_cols)
        
        similarity = len(intersection) / len(union) if union else 0
        return similarity >= 0.8
    
    def _schemas_compatible(self, existing: Dict, required: Dict) -> bool:
        """Check if existing schema can accommodate required schema."""
        # Check for type conflicts in overlapping columns
        for col_name, required_type in required.items():
            if col_name in existing:
                existing_type = existing[col_name]
                if not self._types_compatible(existing_type, required_type):
                    return False
        return True
    
    def _types_compatible(self, existing_type: str, required_type: str) -> bool:
        """Check if two column types are compatible."""
        # Exact match
        if existing_type == required_type:
            return True
        
        # Compatible type conversions
        compatible_conversions = {
            ('int', 'float'): True,
            ('string', 'json'): True,
            ('json', 'string'): True,
        }
        
        return compatible_conversions.get((existing_type, required_type), False)
    
    def _add_missing_columns(self, table_name: str, required_columns: Dict, existing_schema: Dict):
        """Add missing columns to existing table."""
        missing_columns = {
            col: col_type for col, col_type in required_columns.items() 
            if col not in existing_schema
        }
        
        if not missing_columns:
            return
        
        type_mapping = {
            'string': 'TEXT',
            'int': 'INTEGER',
            'float': 'DOUBLE PRECISION',
            'json': 'JSONB',  # Use JSONB for PostgreSQL
            'boolean': 'BOOLEAN',
        }
        
        try:
            with self.engine.begin() as conn:
                for col_name, col_type in missing_columns.items():
                    sql_type = type_mapping.get(col_type, 'TEXT')
                    alter_sql = f'ALTER TABLE "{table_name}" ADD COLUMN "{col_name}" {sql_type}'
                    conn.execute(text(alter_sql))
                    logger.info(f"[TABLE_MANAGER] Added column '{col_name}' ({sql_type}) to table '{table_name}'")
            
            # Update cache
            self.table_cache.pop(table_name, None)
            
        except Exception as e:
            logger.error(f"[TABLE_MANAGER] Error adding columns to {table_name}: {e}")
    
    def _create_table(self, table_name: str, message_structure: dict):
        """Create new table based on message structure."""
        columns = self._get_data_columns(message_structure)
        
        # PostgreSQL-specific type mapping
        type_mapping = {
            'string': 'TEXT',
            'int': 'INTEGER', 
            'float': 'DOUBLE PRECISION',
            'json': 'JSONB',  # Use JSONB for better performance in PostgreSQL
            'boolean': 'BOOLEAN',
        }
        
        # Build column definitions for PostgreSQL
        column_defs = ['id SERIAL PRIMARY KEY']
        column_defs.append('topic TEXT')  # Always include topic
        
        for col_name, col_type in columns.items():
            sql_type = type_mapping.get(col_type, 'TEXT')
            column_defs.append(f'"{col_name}" {sql_type}')
        
        column_defs.append('ingested_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()')
        
        create_sql = f'''
            CREATE TABLE "{table_name}" (
                {', '.join(column_defs)}
            )
        '''
        
        try:
            with self.engine.begin() as conn:
                conn.execute(text(create_sql))
                
                # Create index on common columns for better query performance
                if 'DeviceID' in columns:
                    index_sql = f'CREATE INDEX idx_{table_name}_deviceid ON "{table_name}" ("DeviceID")'
                    conn.execute(text(index_sql))
                
                if 'ts' in columns:
                    index_sql = f'CREATE INDEX idx_{table_name}_ts ON "{table_name}" ("ts")'
                    conn.execute(text(index_sql))
                
                # Always create index on ingested_at
                index_sql = f'CREATE INDEX idx_{table_name}_ingested_at ON "{table_name}" (ingested_at)'
                conn.execute(text(index_sql))
            
            logger.info(f"[TABLE_MANAGER] Created table: {table_name} with {len(columns)} data columns")
            
            # Cache the schema
            schema_cache = {col: columns[col] for col in columns}
            schema_cache.update({'id': 'int', 'topic': 'string', 'ingested_at': 'string'})
            self.table_cache[table_name] = schema_cache
            
        except Exception as e:
            logger.error(f"[TABLE_MANAGER] Error creating table {table_name}: {e}")
            raise
    
    def _create_versioned_table(self, base_name: str, message_structure: dict) -> str:
        """Create a new versioned table when schema conflicts occur."""
        version = 1
        versioned_name = f"{base_name}_v{version}"
        
        # Find the next available version
        while self._table_exists(versioned_name):
            version += 1
            versioned_name = f"{base_name}_v{version}"
        
        self._create_table(versioned_name, message_structure)
        logger.info(f"[TABLE_MANAGER] Created versioned table: {versioned_name}")
        
        return versioned_name
    
    def clear_cache(self):
        """Clear the table schema cache."""
        with self._lock:
            self.table_cache.clear()
