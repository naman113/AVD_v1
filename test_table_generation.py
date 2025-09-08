#!/usr/bin/env python3
"""
Test script to verify table name generation for different topics and parameter counts
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from core.patterns import PatternMatcher
from core.table_manager import TableManager
from sqlalchemy import create_engine
import yaml

def test_table_name_generation():
    """Test table name generation for different topics and parameter counts"""
    
    # Create a mock engine (not connected to real DB)
    engine = create_engine("sqlite:///:memory:")
    table_manager = TableManager(engine)
    
    # Load the patterns from the config
    with open('unified_config.yml', 'r') as f:
        config = yaml.safe_load(f)
    
    patterns = config['patterns']
    matcher = PatternMatcher(patterns)
    
    # Test cases for different topics and parameter counts
    test_cases = [
        {
            "topic": "Gree1",
            "message": {
                "DeviceID": "device_1",
                "Date": "2024-01-01", 
                "Time": "12:00:00",
                "P0": 25.5
            },
            "expected_pattern": "flat_series_4_params",
            "expected_table_suffix": "_4"
        },
        {
            "topic": "Gree1", 
            "message": {
                "DeviceID": "device_2",
                "Date": "2024-01-01",
                "Time": "12:00:00",
                "P0": 25.5,
                "P1": 30.2,
                "P2": 22.1,
                "P3": 28.7,
                "P4": 31.3,
                "P5": 26.9
            },
            "expected_pattern": "flat_series_9_params",
            "expected_table_suffix": "_9"
        },
        {
            "topic": "Energy1",
            "message": {
                "DeviceID": "energy_device_1",
                "Date": "2024-01-01",
                "Time": "12:00:00", 
                "P0": 25.5,
                "P1": 30.2
            },
            "expected_pattern": "flat_series_5_params",
            "expected_table_suffix": "_5"
        }
    ]
    
    print("Testing Table Name Generation")
    print("=" * 50)
    
    for i, test_case in enumerate(test_cases):
        topic = test_case["topic"]
        message = test_case["message"]
        expected_pattern = test_case["expected_pattern"]
        expected_suffix = test_case["expected_table_suffix"]
        
        # Test pattern matching
        pattern_name, pattern = matcher.match(message)
        assert pattern_name == expected_pattern, f"Expected {expected_pattern}, got {pattern_name}"
        
        # Test table name generation
        table_config = {
            'table_override': None,
            'auto_create': True,
            'version_on_conflict': True
        }
        
        generated_table = table_manager._generate_table_name(
            topic=topic,
            device_pattern='*',
            message_structure=message
        )
        
        expected_table = f"{topic.lower()}{expected_suffix}"
        
        print(f"Test {i+1}:")
        print(f"  Topic: {topic}")
        print(f"  Parameters: {len(message)} ({', '.join(message.keys())})")
        print(f"  Matched Pattern: {pattern_name}")
        print(f"  Generated Table: {generated_table}")
        print(f"  Expected Table: {expected_table}")
        
        assert generated_table == expected_table, f"Expected {expected_table}, got {generated_table}"
        print(f"  ✅ Table name generation correct!")
        print()
    
    print("🎉 All table name generation tests passed!")
    print("\nSummary:")
    print("- Gree1 topic with 4 parameters → gree1_4 table")
    print("- Gree1 topic with 9 parameters → gree1_9 table")  
    print("- Energy1 topic with 5 parameters → energy1_5 table")

if __name__ == "__main__":
    test_table_name_generation()
