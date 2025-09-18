#!/usr/bin/env python3
"""
Setup test database and sample data for threshold testing
"""

import sqlite3
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_test_database():
    """Create SQLite database with test data for local development"""
    
    # Create/connect to SQLite database
    conn = sqlite3.connect('test_thresholds.db')
    cursor = conn.cursor()
    
    # Create thresholds table
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS thresholds (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            company_id INTEGER NOT NULL,
            device_id INTEGER NOT NULL,
            parameter VARCHAR(50) NOT NULL,
            lower_threshold DECIMAL(10,2),
            higher_threshold DECIMAL(10,2),
            created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            updated_at DATETIME DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(company_id, device_id, parameter)
        )
    ''')
    
    # Clear existing data
    cursor.execute('DELETE FROM thresholds')
    
    # Insert test data based on your configuration
    test_data = [
        # Company 1, Device 36 (Pengvin1 device 6)
        (1, 36, 'P0', 10.0, 40.0),
        (1, 36, 'P1', 25.0, 75.0),
        (1, 36, 'P2', 700.0, 1300.0),
        (1, 36, 'P3', 0.0, 100.0),
        
        # Company 1, Device 37 (Pengvin1 device 7)
        (1, 37, 'P0', 15.0, 35.0),
        (1, 37, 'P1', 30.0, 70.0),
        (1, 37, 'P2', 800.0, 1200.0),
        (1, 37, 'P3', 5.0, 95.0),
        
        # Company 1, Device 41 (Gree1 device 1)
        (1, 41, 'P0', 18.0, 28.0),
        (1, 41, 'P1', 40.0, 60.0),
        (1, 41, 'P2', 900.0, 1100.0),
        (1, 41, 'P3', 10.0, 90.0),
        (1, 41, 'P4', 0.0, 50.0),
        (1, 41, 'P5', 0.0, 100.0),
        
        # Company 1, Device 42 (Gree1 device 2)
        (1, 42, 'P0', 16.0, 32.0),
        (1, 42, 'P1', 35.0, 65.0),
        (1, 42, 'P2', 850.0, 1150.0),
        (1, 42, 'P3', 8.0, 92.0),
        (1, 42, 'P4', 0.0, 45.0),
        (1, 42, 'P5', 5.0, 95.0),
        
        # Company 1, Device 51 (Energy1 device 1)
        (1, 51, 'P0', 220.0, 240.0),
        (1, 51, 'P2', 0.0, 1000.0),
        (1, 51, 'P3', 0.0, 500.0),
    ]
    
    cursor.executemany('''
        INSERT OR REPLACE INTO thresholds 
        (company_id, device_id, parameter, lower_threshold, higher_threshold)
        VALUES (?, ?, ?, ?, ?)
    ''', test_data)
    
    conn.commit()
    
    # Verify data
    cursor.execute('SELECT COUNT(*) FROM thresholds')
    count = cursor.fetchone()[0]
    logger.info(f"Created test database with {count} threshold records")
    
    # Show sample data
    cursor.execute('''
        SELECT company_id, device_id, parameter, lower_threshold, higher_threshold 
        FROM thresholds 
        ORDER BY device_id, parameter
        LIMIT 10
    ''')
    
    logger.info("Sample threshold data:")
    for row in cursor.fetchall():
        logger.info(f"  Company {row[0]}, Device {row[1]}, {row[2]}: {row[3]} - {row[4]}")
    
    conn.close()
    logger.info("Test database setup complete: test_thresholds.db")

if __name__ == '__main__':
    create_test_database()
