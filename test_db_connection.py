#!/usr/bin/env python3
"""
Test database connection and ThresholdManager functionality
"""

import sys
import logging
from core.config_loader import ConfigLoader
from core.threshold_manager import ThresholdManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_database_connection():
    """Test database connection and basic functionality"""
    try:
        # Load configuration
        config_loader = ConfigLoader('config.yml')
        cfg = config_loader.get()
        db_config = cfg.get('database', {})
        
        logger.info("Testing database connection...")
        logger.info(f"Database URI: {db_config.get('uri', 'No URI provided')}")
        
        # Initialize ThresholdManager
        threshold_manager = ThresholdManager(db_config)
        logger.info("✓ ThresholdManager initialized successfully")
        
        # Test getting thresholds for a specific device
        company_id = 1
        device_id = 36
        parameter = 'P0'
        
        logger.info(f"Testing threshold lookup for company {company_id}, device {device_id}, parameter {parameter}")
        
        thresholds = threshold_manager.get_device_thresholds(company_id, device_id, parameter)
        if thresholds:
            logger.info(f"✓ Found thresholds: {thresholds}")
        else:
            logger.warning("⚠ No thresholds found - this is expected if the database is empty")
        
        # Test getting all thresholds for a device
        logger.info(f"Testing all thresholds for device {device_id}")
        all_thresholds = threshold_manager.get_all_device_thresholds(company_id, device_id)
        logger.info(f"✓ Found {len(all_thresholds)} threshold entries for device {device_id}")
        
        # Close connection
        threshold_manager.close()
        logger.info("✓ Database connection test completed successfully")
        
        return True
        
    except Exception as e:
        logger.error(f"✗ Database connection test failed: {e}")
        return False

if __name__ == '__main__':
    success = test_database_connection()
    sys.exit(0 if success else 1)
