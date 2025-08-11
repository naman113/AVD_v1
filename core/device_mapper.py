"""
Device Mapper - manages mapping between topics, device IDs, and tables.

Provides a centralized way to track which device IDs for which topics
are stored in which database tables, with optional device naming.
"""
import logging
from typing import Dict, Any, Optional, List, Tuple
from sqlalchemy import create_engine, MetaData, Table, Column, Integer, String, DateTime, UniqueConstraint, select, and_, text
from sqlalchemy.engine import Engine
from sqlalchemy.sql import func
from sqlalchemy import inspect
from threading import RLock


class DeviceMapper:
    """Manages device-to-table mappings with optional device naming."""
    
    def __init__(self, db_uri: str):
        # Use the same DB connection as the main app
        if db_uri.startswith("postgresql://") and "+" not in db_uri:
            db_uri = db_uri.replace("postgresql://", "postgresql+psycopg://", 1)
        
        self.engine: Engine = create_engine(db_uri, pool_pre_ping=True)
        self.meta = MetaData()
        self._lock = RLock()
        self._ensure_mapper_table()
        
    def _ensure_mapper_table(self):
        """Create the device mapper table if it doesn't exist."""
        with self._lock:
            # Check if table already exists
            insp = inspect(self.engine)
            if insp.has_table('device_mapper'):
                # Load existing table
                self.mapper_table = Table('device_mapper', self.meta, autoload_with=self.engine)
                logging.info("[MAPPER] Using existing device_mapper table")
                return
            
            # Create new table
            self.mapper_table = Table(
                'device_mapper',
                self.meta,
                Column('id', Integer, primary_key=True, autoincrement=True),
                Column('topic', String(255), nullable=False),
                Column('device_id', String(255), nullable=False),
                Column('table_name', String(255), nullable=False),
                Column('device_name', String(255), nullable=True),  # Optional friendly name
                Column('pattern_name', String(255), nullable=True),  # Pattern used for routing
                Column('first_seen', DateTime, default=func.now()),
                Column('last_seen', DateTime, default=func.now(), onupdate=func.now()),
                Column('message_count', Integer, default=1),
                # Ensure unique combination of topic + device_id
                UniqueConstraint('topic', 'device_id', name='unique_topic_device')
            )
            
            self.meta.create_all(self.engine)
            logging.info("[MAPPER] Created device_mapper table")
    
    def register_device(self, topic: str, device_id: str, table_name: str, 
                       pattern_name: Optional[str] = None, device_name: Optional[str] = None) -> bool:
        """
        Register or update a device mapping.
        
        Args:
            topic: MQTT topic name
            device_id: Device identifier
            table_name: Database table where device data is stored
            pattern_name: Pattern used for routing (optional)
            device_name: Friendly name for the device (optional)
            
        Returns:
            True if new device was registered, False if existing device was updated
        """
        with self._lock:
            try:
                # Check if device already exists
                with self.engine.begin() as conn:
                    existing = conn.execute(
                        self.mapper_table.select().where(
                            (self.mapper_table.c.topic == topic) & 
                            (self.mapper_table.c.device_id == device_id)
                        )
                    ).fetchone()
                    
                    if existing:
                        # Update existing record
                        conn.execute(
                            self.mapper_table.update().where(
                                (self.mapper_table.c.topic == topic) & 
                                (self.mapper_table.c.device_id == device_id)
                            ).values(
                                table_name=table_name,
                                pattern_name=pattern_name,
                                device_name=device_name or existing.device_name,  # Keep existing name if not provided
                                last_seen=func.now(),
                                message_count=existing.message_count + 1
                            )
                        )
                        logging.debug(f"[MAPPER] Updated device: topic={topic} device={device_id} table={table_name}")
                        return False
                    else:
                        # Insert new record
                        conn.execute(
                            self.mapper_table.insert().values(
                                topic=topic,
                                device_id=device_id,
                                table_name=table_name,
                                pattern_name=pattern_name,
                                device_name=device_name,
                                message_count=1
                            )
                        )
                        logging.info(f"[MAPPER] Registered new device: topic={topic} device={device_id} table={table_name}")
                        return True
                        
            except Exception as e:
                logging.error(f"[MAPPER] Failed to register device {topic}/{device_id}: {e}")
                return False
    
    def find_device(self, topic: str, device_id: str) -> Optional[Dict[str, Any]]:
        """Find a specific device by topic and device_id."""
        with self._lock:
            try:
                with self.engine.begin() as conn:
                    result = conn.execute(
                        self.mapper_table.select().where(
                            (self.mapper_table.c.topic == topic) & 
                            (self.mapper_table.c.device_id == device_id)
                        )
                    ).fetchone()
                    
                    if result:
                        return {
                            'topic': result.topic,
                            'device_id': result.device_id,
                            'table_name': result.table_name,
                            'device_name': result.device_name,
                            'pattern_name': result.pattern_name,
                            'first_seen': result.first_seen,
                            'last_seen': result.last_seen,
                            'message_count': result.message_count
                        }
                    return None
                    
            except Exception as e:
                logging.error(f"[MAPPER] Failed to find device {topic}/{device_id}: {e}")
                return None
    
    def find_devices_by_topic(self, topic: str) -> List[Dict[str, Any]]:
        """Find all devices for a given topic."""
        with self._lock:
            try:
                with self.engine.begin() as conn:
                    results = conn.execute(
                        self.mapper_table.select().where(
                            self.mapper_table.c.topic == topic
                        ).order_by(self.mapper_table.c.device_id)
                    ).fetchall()
                    
                    return [
                        {
                            'topic': row.topic,
                            'device_id': row.device_id,
                            'table_name': row.table_name,
                            'device_name': row.device_name,
                            'pattern_name': row.pattern_name,
                            'first_seen': row.first_seen,
                            'last_seen': row.last_seen,
                            'message_count': row.message_count
                        }
                        for row in results
                    ]
                    
            except Exception as e:
                logging.error(f"[MAPPER] Failed to find devices for topic {topic}: {e}")
                return []
    
    def find_devices_by_table(self, table_name: str) -> List[Dict[str, Any]]:
        """Find all devices stored in a given table."""
        with self._lock:
            try:
                with self.engine.begin() as conn:
                    results = conn.execute(
                        self.mapper_table.select().where(
                            self.mapper_table.c.table_name == table_name
                        ).order_by(self.mapper_table.c.topic, self.mapper_table.c.device_id)
                    ).fetchall()
                    
                    return [
                        {
                            'topic': row.topic,
                            'device_id': row.device_id,
                            'table_name': row.table_name,
                            'device_name': row.device_name,
                            'pattern_name': row.pattern_name,
                            'first_seen': row.first_seen,
                            'last_seen': row.last_seen,
                            'message_count': row.message_count
                        }
                        for row in results
                    ]
                    
            except Exception as e:
                logging.error(f"[MAPPER] Failed to find devices for table {table_name}: {e}")
                return []
    
    def set_device_name(self, topic: str, device_id: str, device_name: str) -> bool:
        """Set or update the friendly name for a device."""
        with self._lock:
            try:
                with self.engine.begin() as conn:
                    result = conn.execute(
                        self.mapper_table.update().where(
                            (self.mapper_table.c.topic == topic) & 
                            (self.mapper_table.c.device_id == device_id)
                        ).values(device_name=device_name)
                    )
                    
                    if result.rowcount > 0:
                        logging.info(f"[MAPPER] Set device name: {topic}/{device_id} -> '{device_name}'")
                        return True
                    else:
                        logging.warning(f"[MAPPER] Device not found for naming: {topic}/{device_id}")
                        return False
                        
            except Exception as e:
                logging.error(f"[MAPPER] Failed to set device name for {topic}/{device_id}: {e}")
                return False
    
    def get_all_devices(self) -> List[Dict[str, Any]]:
        """Get all registered devices."""
        with self._lock:
            try:
                with self.engine.begin() as conn:
                    results = conn.execute(
                        self.mapper_table.select().order_by(
                            self.mapper_table.c.topic, 
                            self.mapper_table.c.device_id
                        )
                    ).fetchall()
                    
                    return [
                        {
                            'topic': row.topic,
                            'device_id': row.device_id,
                            'table_name': row.table_name,
                            'device_name': row.device_name,
                            'pattern_name': row.pattern_name,
                            'first_seen': row.first_seen,
                            'last_seen': row.last_seen,
                            'message_count': row.message_count
                        }
                        for row in results
                    ]
                    
            except Exception as e:
                logging.error(f"[MAPPER] Failed to get all devices: {e}")
                return []
    
    def get_stats(self) -> Dict[str, Any]:
        """Get device mapper statistics."""
        with self._lock:
            try:
                with self.engine.begin() as conn:
                    # Total devices
                    total_devices = conn.execute(
                        func.count().select().select_from(self.mapper_table)
                    ).scalar()
                    
                    # Devices per topic
                    topic_counts = conn.execute(
                        select(
                            self.mapper_table.c.topic,
                            func.count().label('count')
                        ).group_by(self.mapper_table.c.topic).order_by(text('count DESC'))
                    ).fetchall()
                    
                    # Devices per table
                    table_counts = conn.execute(
                        select(
                            self.mapper_table.c.table_name,
                            func.count().label('count')
                        ).group_by(self.mapper_table.c.table_name).order_by(text('count DESC'))
                    ).fetchall()
                    
                    # Named vs unnamed devices
                    named_count = conn.execute(
                        select(func.count()).select_from(self.mapper_table).where(
                            and_(
                                self.mapper_table.c.device_name.is_not(None),
                                self.mapper_table.c.device_name != ''
                            )
                        )
                    ).scalar()
                    
                    return {
                        'total_devices': total_devices,
                        'named_devices': named_count,
                        'unnamed_devices': total_devices - named_count,
                        'devices_per_topic': {row.topic: row.count for row in topic_counts},
                        'devices_per_table': {row.table_name: row.count for row in table_counts}
                    }
                    
            except Exception as e:
                logging.error(f"[MAPPER] Failed to get stats: {e}")
                return {'error': str(e)}
