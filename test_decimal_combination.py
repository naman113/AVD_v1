#!/usr/bin/env python3
"""
Test script to verify decimal combination transformation
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.data_transformer import DataTransformer
from core.patterns import PatternMatcher
import yaml

def test_decimal_combination():
    """Test that P0 and P1 are correctly combined into a single decimal value"""
    
    transformer = DataTransformer()
    
    # Test case 1: Basic decimal combination
    test_data_1 = {
        "DeviceID": "energy_device_1",
        "Date": "2024-01-01",
        "Time": "12:00:00",
        "P0": 12345,
        "P1": 81723
    }
    
    transformation_config = {
        "name": "combine_decimal_parts",
        "condition": {
            "topic": "Energy1"
        },
        "action": {
            "type": "combine_decimal",
            "integer_field": "P0",
            "fractional_field": "P1", 
            "target_field": "P0",
            "remove_fractional": True
        }
    }
    
    # Apply transformation
    result_1 = transformer.apply_transformations(
        data=test_data_1,
        topic="Energy1",
        transformations=[transformation_config]
    )
    
    print("Test 1: Basic Decimal Combination")
    print("=" * 40)
    print(f"Original: P0={test_data_1['P0']}, P1={test_data_1['P1']}")
    print(f"Result: P0={result_1['P0']}")
    print(f"P1 removed: {'P1' not in result_1}")
    print(f"Expected: 12345.81723")
    print(f"Matches: {result_1['P0'] == 12345.81723}")
    print()
    
    # Test case 2: Different fractional length
    test_data_2 = {
        "DeviceID": "energy_device_2",
        "Date": "2024-01-01",
        "Time": "12:00:00",
        "P0": 987,
        "P1": 654
    }
    
    result_2 = transformer.apply_transformations(
        data=test_data_2,
        topic="Energy1",
        transformations=[transformation_config]
    )
    
    print("Test 2: Shorter Fractional Part")
    print("=" * 40)
    print(f"Original: P0={test_data_2['P0']}, P1={test_data_2['P1']}")
    print(f"Result: P0={result_2['P0']}")
    print(f"Expected: 987.654")
    print(f"Matches: {result_2['P0'] == 987.654}")
    print()
    
    # Test case 3: Zero fractional part
    test_data_3 = {
        "DeviceID": "energy_device_3",
        "Date": "2024-01-01",
        "Time": "12:00:00",
        "P0": 100,
        "P1": 0
    }
    
    result_3 = transformer.apply_transformations(
        data=test_data_3,
        topic="Energy1",
        transformations=[transformation_config]
    )
    
    print("Test 3: Zero Fractional Part")
    print("=" * 40)
    print(f"Original: P0={test_data_3['P0']}, P1={test_data_3['P1']}")
    print(f"Result: P0={result_3['P0']}")
    print(f"Expected: 100.0")
    print(f"Matches: {result_3['P0'] == 100.0}")
    print()
    
    # Test case 4: No transformation for different topic
    test_data_4 = {
        "DeviceID": "gree_device_1",
        "Date": "2024-01-01",
        "Time": "12:00:00",
        "P0": 123,
        "P1": 456
    }
    
    result_4 = transformer.apply_transformations(
        data=test_data_4,
        topic="Gree1",  # Different topic
        transformations=[transformation_config]
    )
    
    print("Test 4: No Transformation for Different Topic")
    print("=" * 40)
    print(f"Original: P0={test_data_4['P0']}, P1={test_data_4['P1']}")
    print(f"Result: P0={result_4['P0']}, P1={result_4.get('P1', 'MISSING')}")
    print(f"No change: {test_data_4 == result_4}")
    print()
    
    # Verify all tests
    assert result_1['P0'] == 12345.81723, f"Test 1 failed: {result_1['P0']} != 12345.81723"
    assert 'P1' not in result_1, "Test 1 failed: P1 should be removed"
    assert result_2['P0'] == 987.654, f"Test 2 failed: {result_2['P0']} != 987.654"
    assert result_3['P0'] == 100.0, f"Test 3 failed: {result_3['P0']} != 100.0"
    assert result_4 == test_data_4, "Test 4 failed: Data should be unchanged for different topic"
    
    print("✅ All decimal combination tests passed!")
    
def test_full_integration():
    """Test the full integration with pattern matching"""
    
    # Load the patterns from the config
    with open('unified_config.yml', 'r') as f:
        config = yaml.safe_load(f)
    
    patterns = config['patterns']
    matcher = PatternMatcher(patterns)
    transformer = DataTransformer()
    
    # Test Energy1 message with 5 parameters
    energy_message = {
        "DeviceID": "energy_device_123",
        "Date": "2024-01-01",
        "Time": "12:00:00",
        "P0": 12345,
        "P1": 81723
    }
    
    print("Full Integration Test")
    print("=" * 40)
    print(f"Original message: {energy_message}")
    
    # Find matching pattern
    pattern_name, pattern = matcher.match(energy_message)
    print(f"Matched pattern: {pattern_name}")
    print(f"Table: {pattern['table']}")
    
    # Apply transformations if present
    if pattern and pattern.get('transformations'):
        transformed = transformer.apply_transformations(
            data=energy_message,
            topic="Energy1",
            transformations=pattern['transformations']
        )
        print(f"Transformed message: {transformed}")
        
        # Verify transformation
        assert transformed['P0'] == 12345.81723, f"Integration test failed: {transformed['P0']} != 12345.81723"
        assert 'P1' not in transformed, "Integration test failed: P1 should be removed"
        print("✅ Integration test passed!")
    else:
        print("❌ No transformations found in pattern")

if __name__ == "__main__":
    test_decimal_combination()
    print()
    test_full_integration()
