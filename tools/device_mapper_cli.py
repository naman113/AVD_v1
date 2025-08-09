#!/usr/bin/env python3
"""
Device Mapper CLI - Command line interface for managing device mappings.

Usage:
    python device_mapper_cli.py list [--topic TOPIC] [--table TABLE]
    python device_mapper_cli.py find TOPIC DEVICE_ID
    python device_mapper_cli.py name TOPIC DEVICE_ID DEVICE_NAME
    python device_mapper_cli.py stats
    python device_mapper_cli.py export [--format json|csv]

Examples:
    python device_mapper_cli.py list --topic Gree1
    python device_mapper_cli.py find Gree1 91
    python device_mapper_cli.py name Gree1 91 "Kitchen AC Unit"
    python device_mapper_cli.py stats
"""
import argparse
import sys
import json
import csv
from pathlib import Path
import yaml
from typing import Dict, Any, List

# Add the parent directory to the path so we can import our modules
sys.path.insert(0, str(Path(__file__).parent.parent))

from core.device_mapper import DeviceMapper

def load_config() -> Dict[str, Any]:
    """Load the unified_config.yml file."""
    config_path = Path(__file__).parent.parent / "unified_config.yml"
    if not config_path.exists():
        print(f"Error: Config file not found at {config_path}")
        sys.exit(1)
    
    with open(config_path, 'r') as f:
        return yaml.safe_load(f)

def format_device(device: Dict[str, Any]) -> str:
    """Format a device record for display."""
    name_part = f" ({device['device_name']})" if device['device_name'] else ""
    return (
        f"{device['topic']}/{device['device_id']}{name_part} -> {device['table_name']} "
        f"[{device['pattern_name']}] ({device['message_count']} msgs)"
    )

def cmd_list(mapper: DeviceMapper, args):
    """List devices with optional filtering."""
    if args.topic:
        devices = mapper.find_devices_by_topic(args.topic)
        print(f"Devices for topic '{args.topic}':")
    elif args.table:
        devices = mapper.find_devices_by_table(args.table)
        print(f"Devices in table '{args.table}':")
    else:
        devices = mapper.get_all_devices()
        print("All devices:")
    
    if not devices:
        print("No devices found.")
        return
    
    # Group by topic for better display
    by_topic = {}
    for device in devices:
        topic = device['topic']
        if topic not in by_topic:
            by_topic[topic] = []
        by_topic[topic].append(device)
    
    for topic in sorted(by_topic.keys()):
        print(f"\n  Topic: {topic}")
        for device in sorted(by_topic[topic], key=lambda d: d['device_id']):
            name_part = f" ({device['device_name']})" if device['device_name'] else ""
            print(f"    {device['device_id']}{name_part} -> {device['table_name']} "
                  f"[{device['pattern_name']}] ({device['message_count']} msgs)")

def cmd_find(mapper: DeviceMapper, args):
    """Find a specific device."""
    device = mapper.find_device(args.topic, args.device_id)
    if not device:
        print(f"Device {args.topic}/{args.device_id} not found.")
        return
    
    print(f"Device: {args.topic}/{args.device_id}")
    print(f"  Table: {device['table_name']}")
    print(f"  Pattern: {device['pattern_name']}")
    print(f"  Name: {device['device_name'] or 'Not set'}")
    print(f"  First seen: {device['first_seen']}")
    print(f"  Last seen: {device['last_seen']}")
    print(f"  Message count: {device['message_count']}")

def cmd_name(mapper: DeviceMapper, args):
    """Set or update device name."""
    if mapper.set_device_name(args.topic, args.device_id, args.device_name):
        print(f"Set name for {args.topic}/{args.device_id}: '{args.device_name}'")
    else:
        print(f"Failed to set name for {args.topic}/{args.device_id}. Device may not exist.")

def cmd_stats(mapper: DeviceMapper, args):
    """Show device mapper statistics."""
    stats = mapper.get_stats()
    if 'error' in stats:
        print(f"Error getting stats: {stats['error']}")
        return
    
    print("Device Mapper Statistics:")
    print(f"  Total devices: {stats['total_devices']}")
    print(f"  Named devices: {stats['named_devices']}")
    print(f"  Unnamed devices: {stats['unnamed_devices']}")
    
    print("\n  Devices per topic:")
    for topic, count in stats['devices_per_topic'].items():
        print(f"    {topic}: {count}")
    
    print("\n  Devices per table:")
    for table, count in stats['devices_per_table'].items():
        print(f"    {table}: {count}")

def cmd_export(mapper: DeviceMapper, args):
    """Export device data."""
    devices = mapper.get_all_devices()
    
    if args.format == 'json':
        # Convert datetime objects to strings for JSON serialization
        for device in devices:
            if device['first_seen']:
                device['first_seen'] = device['first_seen'].isoformat()
            if device['last_seen']:
                device['last_seen'] = device['last_seen'].isoformat()
        
        print(json.dumps(devices, indent=2))
    
    elif args.format == 'csv':
        if not devices:
            print("No devices to export.")
            return
        
        fieldnames = ['topic', 'device_id', 'table_name', 'device_name', 'pattern_name', 
                     'first_seen', 'last_seen', 'message_count']
        
        writer = csv.DictWriter(sys.stdout, fieldnames=fieldnames)
        writer.writeheader()
        
        for device in devices:
            # Convert datetime objects to strings
            row = device.copy()
            if row['first_seen']:
                row['first_seen'] = row['first_seen'].isoformat()
            if row['last_seen']:
                row['last_seen'] = row['last_seen'].isoformat()
            writer.writerow(row)

def main():
    parser = argparse.ArgumentParser(description='Device Mapper CLI')
    subparsers = parser.add_subparsers(dest='command', help='Available commands')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List devices')
    list_parser.add_argument('--topic', help='Filter by topic')
    list_parser.add_argument('--table', help='Filter by table')
    
    # Find command
    find_parser = subparsers.add_parser('find', help='Find a specific device')
    find_parser.add_argument('topic', help='Topic name')
    find_parser.add_argument('device_id', help='Device ID')
    
    # Name command
    name_parser = subparsers.add_parser('name', help='Set device name')
    name_parser.add_argument('topic', help='Topic name')
    name_parser.add_argument('device_id', help='Device ID')
    name_parser.add_argument('device_name', help='Friendly name for the device')
    
    # Stats command
    stats_parser = subparsers.add_parser('stats', help='Show statistics')
    
    # Export command
    export_parser = subparsers.add_parser('export', help='Export device data')
    export_parser.add_argument('--format', choices=['json', 'csv'], default='json', 
                              help='Export format (default: json)')
    
    args = parser.parse_args()
    
    if not args.command:
        parser.print_help()
        return
    
    # Load config and initialize mapper
    try:
        config = load_config()
        mapper = DeviceMapper(config['database']['uri'])
    except Exception as e:
        print(f"Error initializing device mapper: {e}")
        sys.exit(1)
    
    # Execute command
    try:
        if args.command == 'list':
            cmd_list(mapper, args)
        elif args.command == 'find':
            cmd_find(mapper, args)
        elif args.command == 'name':
            cmd_name(mapper, args)
        elif args.command == 'stats':
            cmd_stats(mapper, args)
        elif args.command == 'export':
            cmd_export(mapper, args)
    except Exception as e:
        print(f"Error executing command: {e}")
        sys.exit(1)

if __name__ == '__main__':
    main()
