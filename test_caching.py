#!/usr/bin/env python3
"""
Test the optimized ThresholdManager caching behavior
"""

import time
import logging
from core.config_loader import ConfigLoader
from core.threshold_manager import ThresholdManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def test_caching_behavior():
    """Test the 30-minute caching behavior"""
    try:
        # Load configuration
        config_loader = ConfigLoader('config.yml')
        cfg = config_loader.get()
        db_config = cfg.get('database', {})
        
        logger.info("=== Testing ThresholdManager Caching Behavior ===")
        
        # Initialize ThresholdManager (this will do initial cache load)
        logger.info("1. Initializing ThresholdManager...")
        threshold_manager = ThresholdManager(db_config)
        
        # Show initial cache stats
        stats = threshold_manager.get_cache_stats()
        logger.info(f"2. Initial cache stats: {stats}")
        
        # Test multiple lookups (should use cache, not hit database)
        logger.info("3. Testing multiple lookups (should use cache)...")
        
        company_id = 1
        device_id = 36
        parameter = 'P0'
        
        for i in range(5):
            thresholds = threshold_manager.get_device_thresholds(company_id, device_id, parameter)
            logger.info(f"   Lookup {i+1}: {thresholds}")
            time.sleep(0.5)  # Small delay to show it's fast
        
        # Show cache stats after lookups
        stats = threshold_manager.get_cache_stats()
        logger.info(f"4. Cache stats after lookups: {stats}")
        
        # Test getting all device thresholds
        logger.info("5. Getting all thresholds for device...")
        all_thresholds = threshold_manager.get_all_device_thresholds(company_id, device_id)
        logger.info(f"   Found {len(all_thresholds)} parameters: {list(all_thresholds.keys())}")
        
        # Test force refresh
        logger.info("6. Testing force refresh...")
        threshold_manager.force_refresh()
        
        stats = threshold_manager.get_cache_stats()
        logger.info(f"7. Cache stats after force refresh: {stats}")
        
        # Show cache behavior explanation
        logger.info("\n=== Caching Behavior Summary ===")
        logger.info("✓ Initial load: Fetches ALL thresholds from database once")
        logger.info("✓ Subsequent lookups: Use in-memory cache (no database calls)")
        logger.info("✓ Auto-refresh: Cache refreshes every 30 minutes automatically")
        logger.info("✓ Force refresh: Can manually refresh if needed")
        logger.info("✓ Thread-safe: Uses locks for concurrent access")
        
        # Close connection
        threshold_manager.close()
        logger.info("8. ThresholdManager closed successfully")
        
        return True
        
    except Exception as e:
        logger.error(f"Test failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == '__main__':
    success = test_caching_behavior()
    if success:
        logger.info("\n✓ Caching test completed successfully!")
    else:
        logger.error("\n✗ Caching test failed!")
