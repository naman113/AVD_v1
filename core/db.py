from typing import Dict, Any, Set, Optional
import logging
import re
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, Float, DateTime, JSON
from sqlalchemy.engine import Engine
from sqlalchemy import inspect
from sqlalchemy.sql import func
from threading import RLock

SQL_TYPE_MAP = {
    'string': String(255),
    'int': Integer,
    'float': Float,
    'json': JSON,
}

class DB:
    # cache table->set(columns) across instances (lightweight). It's refreshed on use.
    _columns_cache = {}
    def __init__(self, uri: str):
        # If using PostgreSQL without an explicit driver, prefer psycopg (psycopg3)
        if uri.startswith("postgresql://") and "+" not in uri:
            uri = uri.replace("postgresql://", "postgresql+psycopg://", 1)
        self.engine: Engine = create_engine(uri, pool_pre_ping=True)
        self.meta = MetaData()
        self._tables: Dict[str, Table] = {}
        self._lock = RLock()

    def ensure_table(self, name: str, columns: Dict[str, Any]) -> Table:
        with self._lock:
            if name in self._tables:
                return self._tables[name]
            insp = inspect(self.engine)
            if insp.has_table(name):
                # Autoload existing table definition
                table = Table(name, self.meta, autoload_with=self.engine)
                self._tables[name] = table
                # cache columns
                try:
                    self._columns_cache[name] = {col['name'] for col in insp.get_columns(name)}
                except Exception:
                    self._columns_cache.pop(name, None)
                logging.info(f"[DB] Using existing table '{name}'")
                return table
            # Define and create new table
            cols = [Column('id', Integer, primary_key=True, autoincrement=True)]
            for cname, ctype in columns.items():
                if isinstance(ctype, str):
                    col = Column(cname, SQL_TYPE_MAP.get(ctype, String(255)))
                else:
                    col = Column(cname, ctype)
                cols.append(col)
            cols.append(Column('ingested_at', DateTime, default=func.now()))
            table = Table(name, self.meta, *cols)
            self.meta.create_all(self.engine)
            self._tables[name] = table
            # cache columns (id + provided + ingested_at)
            self._columns_cache[name] = {'id', *columns.keys(), 'ingested_at'}
            logging.info(f"[DB] Created table '{name}' with columns: {list(columns.keys())} + ingested_at")
            return table

    def insert(self, table: Table, row: Dict[str, Any]):
        with self.engine.begin() as c:
            c.execute(table.insert(), row)
        # compact log for inserts
        dev = row.get('DeviceID') or row.get('deviceid') or row.get('device_id')
        ts = row.get('Time') or row.get('ts') or row.get('time')
        topic = row.get('topic')
        keys = [k for k in row.keys() if k not in ('payload',)]
        logging.info(f"[DB] Inserted row into '{table.name}' topic={topic} device={dev} ts={ts} fields={keys}")

    def ensure_columns(self, table_name: str, columns: Dict[str, Any]):
        """Add any missing columns to an existing table."""
        insp = inspect(self.engine)
        existing_cols = {col['name'] for col in insp.get_columns(table_name)}
        adds = [(k, v) for k, v in columns.items() if k not in existing_cols]
        if not adds:
            # update cache anyway
            self._columns_cache[table_name] = existing_cols
            return
        ddl_map = {
            'string': 'TEXT',
            'int': 'INTEGER',
            'float': 'DOUBLE PRECISION',
            'json': 'JSON',
        }
        with self.engine.begin() as c:
            for cname, ctype in adds:
                if isinstance(ctype, str):
                    ddl_type = ddl_map.get(ctype, 'TEXT')
                else:
                    # crude mapping for SQLAlchemy types
                    tn = type(ctype).__name__.lower()
                    if 'integer' in tn:
                        ddl_type = 'INTEGER'
                    elif 'float' in tn or 'numeric' in tn:
                        ddl_type = 'DOUBLE PRECISION'
                    elif 'json' in tn:
                        ddl_type = 'JSON'
                    else:
                        ddl_type = 'TEXT'
                c.execute(f'ALTER TABLE "{table_name}" ADD COLUMN "{cname}" {ddl_type}')
                logging.info(f"[DB] Added column '{cname}' {ddl_type} to table '{table_name}'")
        # refresh metadata
        self.meta.clear()
        self.meta.reflect(bind=self.engine)
        # refresh cache
        try:
            self._columns_cache[table_name] = {col['name'] for col in insp.get_columns(table_name)}
        except Exception:
            self._columns_cache.pop(table_name, None)

    # --- Discovery and compatibility helpers ---
    def table_exists(self, name: str) -> bool:
        try:
            return inspect(self.engine).has_table(name)
        except Exception:
            return False

    def get_table_columns(self, name: str) -> Set[str]:
        if name in self._columns_cache:
            return self._columns_cache[name]
        try:
            cols = {col['name'] for col in inspect(self.engine).get_columns(name)}
            self._columns_cache[name] = cols
            return cols
        except Exception:
            return set()

    def find_tables_with_prefix(self, prefix: str) -> list[str]:
        try:
            names = inspect(self.engine).get_table_names()
        except Exception:
            names = []
        return [n for n in names if n.startswith(prefix)]

    def resolve_compatible_table(self, preferred: str, required_columns: Dict[str, Any]) -> str:
        """Return an existing compatible table when available, else the preferred name.
        Compatibility means the existing table already has some/all required columns; we'll add any missing.
        """
        # Only reuse the exact preferred table name; otherwise use the preferred (will be created on demand).
        if self.table_exists(preferred):
            return preferred
        return preferred
