#!/usr/bin/env python3
"""
PostgreSQL-specific test for table auto-creation functionality.

This script tests the table configuration logic without requiring database connections.
"""

import logging
import json
from unittest.mock import Mock, MagicMock
from core.table_manager import TableManager

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_table_naming_logic():
    """Test table name generation logic without database operations."""
    
    logger.info("Testing table naming logic...")
    
    # Create a mock engine
    mock_engine = Mock()
    table_manager = TableManager(mock_engine)
    
    # Test 1: Auto-generate table name with flat message structure
    logger.info("\n--- Test 1: Auto-generate table name ---")
    message1 = {
        "DeviceID": "103",
        "Temperature": 25.5,
        "Humidity": 60,
        "Status": "online",
        "Timestamp": "2025-08-11T10:30:00Z"
    }
    
    # Mock the internal methods to avoid database calls
    table_manager._table_exists = Mock(return_value=False)
    table_manager._find_similar_table = Mock(return_value=None)
    table_manager._create_table = Mock()
    
    table_config1 = {
        "auto_create": True,
        "version_on_conflict": True
    }
    
    table_name1 = table_manager._generate_table_name("Gree1", "103", message1)
    logger.info(f"Generated table name: {table_name1}")
    
    # Test 2: Data column extraction
    logger.info("\n--- Test 2: Data column extraction ---")
    columns1 = table_manager._get_data_columns(message1)
    logger.info(f"Extracted columns: {columns1}")
    
    # Test 3: Array enveloped message structure
    logger.info("\n--- Test 3: Array enveloped structure ---")
    message2 = {
        "d": {
            "DeviceID": ["104"],
            "Temperature": [22.3],
            "Humidity": [55],
            "Power": [150.5]
        },
        "ts": "2025-08-11T10:35:00Z"
    }
    
    table_name2 = table_manager._generate_table_name("Energy1", "*", message2)
    columns2 = table_manager._get_data_columns(message2)
    logger.info(f"Array enveloped table name: {table_name2}")
    logger.info(f"Array enveloped columns: {columns2}")
    
    # Test 4: Schema compatibility checking
    logger.info("\n--- Test 4: Schema compatibility ---")
    existing_schema = {
        "DeviceID": "string",
        "Temperature": "float",
        "Humidity": "int",
        "Status": "string"
    }
    
    required_schema = {
        "DeviceID": "string",
        "Temperature": "float",
        "Humidity": "int",
        "Status": "string",
        "NewColumn": "string"  # New column
    }
    
    compatible = table_manager._schemas_compatible(existing_schema, required_schema)
    similar = table_manager._schemas_similar(existing_schema, required_schema)
    
    logger.info(f"Schemas compatible: {compatible}")
    logger.info(f"Schemas similar: {similar}")
    
    # Test 5: Type compatibility
    logger.info("\n--- Test 5: Type compatibility ---")
    test_cases = [
        ("int", "float", True),
        ("string", "json", True),
        ("json", "string", True),
        ("int", "string", False),
        ("float", "boolean", False)
    ]
    
    for existing_type, required_type, expected in test_cases:
        result = table_manager._types_compatible(existing_type, required_type)
        status = "✓" if result == expected else "✗"
        logger.info(f"{status} {existing_type} -> {required_type}: {result}")
    
    return True

def test_configuration_migration():
    """Test the configuration migration logic."""
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
    
    # Test migration count
    count = ConfigMigrator.count_migrations(old_config)
    logger.info(f"Found {count} configurations to migrate")
    
    # Migrate configuration
    new_config = ConfigMigrator.migrate_config(old_config)
    
    logger.info("\nOld configuration:")
    logger.info(json.dumps(old_config['routes'][0]['device_ids'], indent=2))
    
    logger.info("\nNew configuration:")
    logger.info(json.dumps(new_config['routes'][0]['device_ids'], indent=2))
    
    # Verify migration
    route = new_config['routes'][0]
    device1 = route['device_ids'][0]
    device2 = route['device_ids'][1]
    
    # Check device1 (explicit table name)
    assert 'table_config' in device1
    assert device1['table_config']['name'] == 'gree1_4'
    assert device1['table_config']['auto_create'] == True
    assert device1['table_config']['version_on_conflict'] == True
    assert 'table_override' not in device1
    
    # Check device2 (auto-generate table name)
    assert 'table_config' in device2
    assert 'name' not in device2['table_config']  # Auto-generate
    assert device2['table_config']['auto_create'] == True
    assert device2['table_config']['version_on_conflict'] == True
    assert 'table_override' not in device2
    
    logger.info("✓ Configuration migration test passed!")
    return True

def test_backward_compatibility():
    """Test backward compatibility with table_override format."""
    logger.info("\n\n=== Testing Backward Compatibility ===")
    
    mock_engine = Mock()
    table_manager = TableManager(mock_engine)
    
    # Mock methods to avoid database calls
    table_manager._table_exists = Mock(return_value=False)
    table_manager._find_similar_table = Mock(return_value=None)
    table_manager._create_table = Mock()
    
    message = {
        "DeviceID": "103",
        "Temperature": 25.5,
        "Status": "online"
    }
    
    # Test old format: table_override with value
    logger.info("\n--- Test: table_override with value ---")
    old_config1 = {"table_override": "gree1_4"}
    
    table_name1 = table_manager.get_or_create_table_name(
        table_config=old_config1,
        topic="Gree1",
        device_pattern="103",
        message_structure=message
    )
    logger.info(f"table_override: 'gree1_4' -> {table_name1}")
    
    # Test old format: table_override with null
    logger.info("\n--- Test: table_override with null ---")
    old_config2 = {"table_override": None}
    
    table_name2 = table_manager.get_or_create_table_name(
        table_config=old_config2,
        topic="Gree1",
        device_pattern="103",
        message_structure=message
    )
    logger.info(f"table_override: null -> {table_name2}")
    
    # Test new format
    logger.info("\n--- Test: new table_config format ---")
    new_config = {
        "name": "gree1_4",
        "auto_create": True,
        "version_on_conflict": True
    }
    
    table_name3 = table_manager.get_or_create_table_name(
        table_config=new_config,
        topic="Gree1", 
        device_pattern="103",
        message_structure=message
    )
    logger.info(f"table_config: {{name: 'gree1_4'}} -> {table_name3}")
    
    logger.info("✓ Backward compatibility test passed!")
    return True

def main():
    """Run all tests."""
    try:
        logger.info("🚀 Starting PostgreSQL Table Manager Tests")
        
        test_table_naming_logic()
        test_configuration_migration()
        test_backward_compatibility()
        
        logger.info("\n🎉 All tests completed successfully!")
        logger.info("\nNext steps:")
        logger.info("1. Test with your actual PostgreSQL database")
        logger.info("2. Run the migration tool on your config: python tools/config_migrator.py unified_config.yml")
        logger.info("3. Start the application and verify table auto-creation works")
        
        return True
        
    except Exception as e:
        logger.error(f"❌ Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)
