#!/usr/bin/env python3
"""
Test script for table auto-creation functionality.

This script demonstrates the new table configuration features:
1. Auto table creation based on message structure
2. Table versioning on schema conflicts
3. Similar table detection
"""

import logging
import json
from core.table_manager import TableManager
from sqlalchemy import create_engine, text, inspect

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_table_manager():
    """Test the TableManager functionality with sample data."""
    
    # Use PostgreSQL connection from config (or a test database)
    # For testing purposes, we'll create a simple mock engine
    # In production, this would use the actual PostgreSQL connection
    try:
        # Try to connect to the actual database from config
        import yaml
        with open('unified_config.yml', 'r') as f:
            config = yaml.safe_load(f)
        
        db_uri = config['database']['uri']
        engine = create_engine(db_uri)
        
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        logger.info(f"Connected to PostgreSQL database: {db_uri.split('@')[1] if '@' in db_uri else db_uri}")
        
    except Exception as e:
        logger.warning(f"Could not connect to configured database: {e}")
        logger.info("Using mock engine for testing...")
        # Create a mock engine that doesn't actually execute SQL
        from sqlalchemy.pool import StaticPool
        engine = create_engine("postgresql://test:test@localhost/test", 
                             strategy='mock', executor=lambda sql, *_: None)
    
    table_manager = TableManager(engine)
    
    logger.info("Starting TableManager tests...")
    
    # Test 1: Auto-generate table name with flat message structure
    logger.info("\n--- Test 1: Auto-generate table name ---")
    message1 = {
        "DeviceID": "103",
        "Temperature": 25.5,
        "Humidity": 60,
        "Status": "online",
        "Timestamp": "2025-08-11T10:30:00Z"
    }
    
    table_config1 = {
        "auto_create": True,
        "version_on_conflict": True
    }
    
    table_name1 = table_manager.get_or_create_table_name(
        table_config=table_config1,
        topic="Gree1",
        device_pattern="103",
        message_structure=message1
    )
    logger.info(f"Generated table name: {table_name1}")
    
    # Test 2: Specify table name explicitly
    logger.info("\n--- Test 2: Explicit table name ---")
    table_config2 = {
        "name": "gree1_4",
        "auto_create": True,
        "version_on_conflict": True
    }
    
    table_name2 = table_manager.get_or_create_table_name(
        table_config=table_config2,
        topic="Gree1",
        device_pattern="103",
        message_structure=message1
    )
    logger.info(f"Explicit table name: {table_name2}")
    
    # Test 3: Schema conflict - should create versioned table
    logger.info("\n--- Test 3: Schema conflict (versioning) ---")
    message2 = {
        "DeviceID": "103",
        "Temperature": 25.5,
        "Pressure": 1013.25,  # Different column
        "Location": "Room1",   # Different column
        "Active": True         # Different column
    }
    
    table_name3 = table_manager.get_or_create_table_name(
        table_config=table_config2,  # Same explicit name
        topic="Gree1",
        device_pattern="103",
        message_structure=message2
    )
    logger.info(f"Versioned table name: {table_name3}")
    
    # Test 4: Array enveloped message structure
    logger.info("\n--- Test 4: Array enveloped structure ---")
    message3 = {
        "d": {
            "DeviceID": ["104"],
            "Temperature": [22.3],
            "Humidity": [55],
            "Power": [150.5]
        },
        "ts": "2025-08-11T10:35:00Z"
    }
    
    table_config3 = {
        "auto_create": True,
        "version_on_conflict": True
    }
    
    table_name4 = table_manager.get_or_create_table_name(
        table_config=table_config3,
        topic="Energy1",
        device_pattern="*",
        message_structure=message3
    )
    logger.info(f"Array enveloped table name: {table_name4}")
    
    # Test 5: Similar table detection
    logger.info("\n--- Test 5: Similar table detection ---")
    message4 = {
        "DeviceID": "105",
        "Temperature": 23.1,  # Same columns as message1
        "Humidity": 58,       # Same columns as message1
        "Status": "active",   # Same columns as message1
        "Timestamp": "2025-08-11T10:40:00Z"  # Same columns as message1
    }
    
    table_name5 = table_manager.get_or_create_table_name(
        table_config=table_config1,
        topic="Gree1",
        device_pattern="105",
        message_structure=message4
    )
    logger.info(f"Similar table detection result: {table_name5}")
    
    # Display final summary
    logger.info("\n--- Test Summary ---")
    logger.info(f"Test 1 (auto-generate): {table_name1}")
    logger.info(f"Test 2 (explicit): {table_name2}")
    logger.info(f"Test 3 (versioned): {table_name3}")
    logger.info(f"Test 4 (array enveloped): {table_name4}")
    logger.info(f"Test 5 (similar detection): {table_name5}")
    
    # Check which tables were actually created (only if using real database)
    logger.info("\n--- Created Tables ---")
    if 'mock' not in str(engine.url):
        try:
            inspector = inspect(engine)
            tables = inspector.get_table_names()
            logger.info(f"Tables in database: {tables}")
            
            for table in tables:
                if table.startswith(('gree1_', 'energy1_')):  # Only show our test tables
                    columns = inspector.get_columns(table)
                    col_names = [col['name'] for col in columns]
                    logger.info(f"Table '{table}' columns: {col_names}")
        except Exception as e:
            logger.warning(f"Could not inspect database tables: {e}")
    else:
        logger.info("Mock engine used - tables not actually created")

def test_config_migration():
    """Test the configuration migration functionality."""
    logger.info("\n\n=== Testing Configuration Migration ===")
    
    # Sample old configuration
    old_config = {
        "routes": [
            {
                "topic": "Gree1",
                "mqtt_server": "server1",
                "auto_discover": True,
                "device_ids": [
                    {
                        "pattern": "103",
                        "pattern_name": "auto",
                        "table_override": "gree1_4"
                    },
                    {
                        "pattern": "*",
                        "pattern_name": "auto", 
                        "table_override": None
                    }
                ]
            }
        ]
    }
    
    from tools.config_migrator import ConfigMigrator
    
    # Migrate configuration
    new_config = ConfigMigrator.migrate_config(old_config)
    
    logger.info("Old configuration:")
    logger.info(json.dumps(old_config, indent=2))
    
    logger.info("\nNew configuration:")
    logger.info(json.dumps(new_config, indent=2))
    
    # Verify migration
    route = new_config['routes'][0]
    device1 = route['device_ids'][0]
    device2 = route['device_ids'][1]
    
    assert 'table_config' in device1
    assert device1['table_config']['name'] == 'gree1_4'
    assert device1['table_config']['auto_create'] == True
    
    assert 'table_config' in device2
    assert 'name' not in device2['table_config']  # Auto-generate
    assert device2['table_config']['auto_create'] == True
    
    logger.info("✓ Configuration migration test passed!")

if __name__ == "__main__":
    try:
        test_table_manager()
        test_config_migration()
        logger.info("\n🎉 All tests completed successfully!")
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        raise
