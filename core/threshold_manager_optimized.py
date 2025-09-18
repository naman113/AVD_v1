"""
Threshold Database Manager - Optimized Version
Handles fetching threshold configurations from the database table with efficient 30-minute caching.
"""
import logging
from typing import Dict, Any, Optional, List
from dataclasses import dataclass
import sqlite3
from threading import RLock
import time

# Import PostgreSQL drivers with fallback handling
try:
    import psycopg2
    from psycopg2.extras import RealDictCursor
    HAS_PSYCOPG2 = True
except ImportError:
    HAS_PSYCOPG2 = False
    psycopg2 = None
    RealDictCursor = None

@dataclass
class Threshold:
    company_id: int
    device_id: int
    parameter: str
    lower_threshold: float
    higher_threshold: float

class ThresholdManager:
    """
    Manages threshold configurations from database with efficient 30-minute caching.
    Loads all thresholds once and refreshes every 30 minutes to minimize database calls.
    """
    
    def __init__(self, db_config: Dict[str, Any]):
        """
        Initialize ThresholdManager with database configuration
        
        Args:
            db_config: Database configuration dictionary containing 'uri' or individual params
        """
        self.db_config = db_config
        self.thresholds_cache = {}  # Cache: {(company_id, device_id, parameter): {'lower': x, 'higher': y}}
        self.last_refresh_time = 0
        self.cache_duration = 1800  # 30 minutes in seconds
        self.lock = RLock()  # Thread-safe operations
        self.logger = logging.getLogger(__name__)
        
        # Test database connection and perform initial load
        self._test_connection()
        self._refresh_cache()
    
    def _get_connection(self):
        """Get database connection using URI or individual parameters"""
        try:
            # Check if URI is provided (preferred method)
            if 'uri' in self.db_config:
                uri = self.db_config['uri']
                if uri.startswith('postgresql://'):
                    if not HAS_PSYCOPG2:
                        raise ImportError("psycopg2 is not installed. Install it with: pip install psycopg2-binary")
                    return psycopg2.connect(uri)
                elif uri.startswith('sqlite://'):
                    # Remove sqlite:// prefix for sqlite3
                    db_path = uri.replace('sqlite://', '')
                    return sqlite3.connect(db_path)
                else:
                    raise ValueError(f"Unsupported database URI scheme: {uri}")
            
            # Fallback to individual parameters
            host = self.db_config.get('host', 'localhost')
            port = self.db_config.get('port', 5432)
            database = self.db_config.get('database')
            username = self.db_config.get('username')
            password = self.db_config.get('password')
            
            if not all([database, username, password]):
                raise ValueError("Database configuration missing required fields")
            
            if not HAS_PSYCOPG2:
                raise ImportError("psycopg2 is not installed. Install it with: pip install psycopg2-binary")
            
            return psycopg2.connect(
                host=host,
                port=port,
                database=database,
                user=username,
                password=password
            )
            
        except Exception as e:
            self.logger.error(f"Database connection failed: {e}")
            raise
    
    def _test_connection(self):
        """Test database connection"""
        try:
            with self._get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                self.logger.info("[THRESHOLD] Database connection test successful")
        except Exception as e:
            self.logger.error(f"[THRESHOLD] Database connection test failed: {e}")
            raise
    
    def _fetch_thresholds_from_db(self) -> List[Threshold]:
        """Fetch all thresholds from database"""
        try:
            with self._get_connection() as conn:
                # Create cursor based on database type
                if HAS_PSYCOPG2 and hasattr(conn, 'cursor') and 'postgresql://' in self.db_config.get('uri', ''):
                    cursor = conn.cursor(cursor_factory=RealDictCursor)
                else:
                    cursor = conn.cursor()
                
                query = """
                    SELECT company_id, device_id, parameter, 
                           lower_threshold, higher_threshold 
                    FROM threshold 
                    ORDER BY company_id, device_id, parameter
                """
                
                cursor.execute(query)
                rows = cursor.fetchall()
                
                thresholds = []
                for row in rows:
                    if isinstance(row, dict):  # PostgreSQL with RealDictCursor
                        thresholds.append(Threshold(
                            company_id=row['company_id'],
                            device_id=row['device_id'],
                            parameter=row['parameter'],
                            lower_threshold=float(row['lower_threshold']),
                            higher_threshold=float(row['higher_threshold'])
                        ))
                    else:  # SQLite tuple result or regular tuple
                        thresholds.append(Threshold(
                            company_id=row[0],
                            device_id=row[1],
                            parameter=row[2],
                            lower_threshold=float(row[3]),
                            higher_threshold=float(row[4])
                        ))
                
                self.logger.info(f"[THRESHOLD] Loaded {len(thresholds)} threshold configurations from database")
                return thresholds
                
        except Exception as e:
            self.logger.error(f"[THRESHOLD] Failed to fetch thresholds from database: {e}")
            return []
    
    def _refresh_cache(self):
        """Refresh the threshold cache from database"""
        with self.lock:
            try:
                self.logger.info("[THRESHOLD] Refreshing threshold cache from database...")
                
                # Fetch all thresholds from database
                thresholds = self._fetch_thresholds_from_db()
                
                # Clear existing cache and rebuild
                self.thresholds_cache.clear()
                
                # Build cache with composite key (company_id, device_id, parameter)
                for threshold in thresholds:
                    key = (threshold.company_id, threshold.device_id, threshold.parameter)
                    self.thresholds_cache[key] = {
                        'lower_threshold': threshold.lower_threshold,
                        'higher_threshold': threshold.higher_threshold
                    }
                
                # Update last refresh time
                self.last_refresh_time = time.time()
                
                self.logger.info(f"[THRESHOLD] Cache refreshed successfully with {len(self.thresholds_cache)} threshold entries")
                
            except Exception as e:
                self.logger.error(f"[THRESHOLD] Failed to refresh cache: {e}")
                # Don't clear cache on error, keep using stale data
    
    def _ensure_cache_fresh(self):
        """Ensure cache is fresh, refresh if needed"""
        current_time = time.time()
        if current_time - self.last_refresh_time > self.cache_duration:
            self.logger.info("[THRESHOLD] Cache expired (30 minutes), refreshing...")
            self._refresh_cache()
    
    def get_device_thresholds(self, company_id: int, device_id: int, parameter: str) -> Optional[Dict[str, float]]:
        """
        Get threshold configuration for a specific device parameter.
        Uses in-memory cache with 30-minute refresh cycle.
        
        Args:
            company_id: Company identifier
            device_id: Device identifier 
            parameter: Parameter name
            
        Returns:
            Dictionary with 'lower_threshold' and 'higher_threshold' or None if not found
        """
        # Ensure cache is fresh (checks every call, refreshes only if expired)
        self._ensure_cache_fresh()
        
        with self.lock:
            key = (company_id, device_id, parameter)
            threshold_data = self.thresholds_cache.get(key)
            
            if threshold_data:
                self.logger.debug(f"[THRESHOLD] Found threshold for {company_id}/{device_id}/{parameter}: {threshold_data}")
            else:
                self.logger.debug(f"[THRESHOLD] No threshold found for {company_id}/{device_id}/{parameter}")
                
            return threshold_data
    
    def get_all_device_thresholds(self, company_id: int, device_id: int) -> Dict[str, Dict[str, float]]:
        """
        Get all threshold configurations for a device.
        Uses in-memory cache with 30-minute refresh cycle.
        
        Args:
            company_id: Company identifier
            device_id: Device identifier
            
        Returns:
            Dictionary mapping parameter names to threshold data
        """
        # Ensure cache is fresh
        self._ensure_cache_fresh()
        
        with self.lock:
            result = {}
            for (c_id, d_id, param), threshold_data in self.thresholds_cache.items():
                if c_id == company_id and d_id == device_id:
                    result[param] = threshold_data
                    
            self.logger.debug(f"[THRESHOLD] Found {len(result)} parameters for device {company_id}/{device_id}")
            return result
    
    def force_refresh(self):
        """Force refresh the cache (useful for testing or manual refresh)"""
        self.logger.info("[THRESHOLD] Forcing cache refresh...")
        self._refresh_cache()
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """Get cache statistics for monitoring"""
        with self.lock:
            current_time = time.time()
            cache_age = current_time - self.last_refresh_time
            return {
                'total_entries': len(self.thresholds_cache),
                'cache_age_seconds': cache_age,
                'cache_age_minutes': cache_age / 60,
                'next_refresh_in_seconds': max(0, self.cache_duration - cache_age),
                'last_refresh_time': self.last_refresh_time,
                'cache_duration_minutes': self.cache_duration / 60
            }
    
    def close(self):
        """Clean up resources"""
        with self.lock:
            self.logger.info("[THRESHOLD] Closing ThresholdManager")
            # Clear cache to free memory
            self.thresholds_cache.clear()
