#!/usr/bin/env python3
"""
Test script to verify parameter-based routing functionality
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.patterns import PatternMatcher
import yaml

def test_parameter_routing():
    """Test that messages with different parameter counts get matched to the right patterns"""
    
    # Load the patterns from the config
    with open('unified_config.yml', 'r') as f:
        config = yaml.safe_load(f)
    
    patterns = config['patterns']
    matcher = PatternMatcher(patterns)
    
    # Test message with 4 parameters (should match flat_series_4_params)
    message_4_params = {
        "DeviceID": "test_device_1",
        "Date": "2024-01-01", 
        "Time": "12:00:00",
        "P0": 25.5
    }
    
    # Test message with 5 parameters (should match flat_series_5_params)
    message_5_params = {
        "DeviceID": "test_device_2",
        "Date": "2024-01-01",
        "Time": "12:00:00", 
        "P0": 25.5,
        "P1": 30.2
    }
    
    # Test message with 9 parameters (should match flat_series_9_params)
    message_9_params = {
        "DeviceID": "test_device_3",
        "Date": "2024-01-01",
        "Time": "12:00:00",
        "P0": 25.5,
        "P1": 30.2,
        "P2": 22.1,
        "P3": 28.7,
        "P4": 31.3,
        "P5": 26.9
    }
    
    # Test array enveloped message
    message_array = {
        "d": {
            "DeviceID": ["test_device_4"],
            "Temperature": [23.5],
            "Humidity": [60.2]
        },
        "ts": "2024-01-01T12:00:00Z"
    }
    
    print("Testing Parameter-Based Routing")
    print("=" * 40)
    
    # Test 4 parameter message
    pattern_name, pattern = matcher.match(message_4_params)
    print(f"4 parameters: {pattern_name} -> {pattern['table'] if pattern else 'No match'}")
    assert pattern_name == "flat_series_4_params", f"Expected flat_series_4_params, got {pattern_name}"
    assert pattern['table'] == "{topic}_4", f"Expected {{topic}}_4, got {pattern['table']}"
    
    # Test 5 parameter message  
    pattern_name, pattern = matcher.match(message_5_params)
    print(f"5 parameters: {pattern_name} -> {pattern['table'] if pattern else 'No match'}")
    assert pattern_name == "flat_series_5_params", f"Expected flat_series_5_params, got {pattern_name}"
    assert pattern['table'] == "{topic}_5", f"Expected {{topic}}_5, got {pattern['table']}"
    
    # Test 9 parameter message
    pattern_name, pattern = matcher.match(message_9_params)
    print(f"9 parameters: {pattern_name} -> {pattern['table'] if pattern else 'No match'}")
    assert pattern_name == "flat_series_9_params", f"Expected flat_series_9_params, got {pattern_name}"
    assert pattern['table'] == "{topic}_9", f"Expected {{topic}}_9, got {pattern['table']}"
    
    # Test array enveloped message
    pattern_name, pattern = matcher.match(message_array)
    print(f"Array enveloped: {pattern_name} -> {pattern['table'] if pattern else 'No match'}")
    assert pattern_name == "array_enveloped", f"Expected array_enveloped, got {pattern_name}"
    
    print("\n✅ All tests passed! Parameter-based routing is working correctly.")
    print("\nExpected behavior:")
    print("- Messages with 4 parameters (DeviceID, Date, Time, P0) → {topic}_4 table")
    print("- Messages with 5 parameters (DeviceID, Date, Time, P0, P1) → {topic}_5 table") 
    print("- Messages with 9 parameters (DeviceID, Date, Time, P0-P5) → {topic}_9 table")
    print("- Array enveloped messages → {topic}_array_enveloped table")

if __name__ == "__main__":
    test_parameter_routing()
