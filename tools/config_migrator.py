#!/usr/bin/env python3
"""
Configuration Migration Utility

This script helps migrate from the old table_override format to the new table_config format.
It provides both automatic migration and manual conversion options.
"""

import yaml
import argparse
import logging
from pathlib import Path
from typing import Dict, Any

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class ConfigMigrator:
    """Migrates configuration from table_override to table_config format."""
    
    @staticmethod
    def migrate_device_config(device_config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Migrate a single device configuration from table_override to table_config.
        
        Args:
            device_config: Device configuration dictionary
            
        Returns:
            Migrated device configuration
        """
        migrated = device_config.copy()
        
        if 'table_override' in device_config:
            table_override = device_config['table_override']
            
            # Create new table_config structure
            table_config = {
                'auto_create': True,
                'version_on_conflict': True
            }
            
            # Handle table name
            if table_override is not None:
                table_config['name'] = table_override
            # If table_override is None, it means auto-generate table name
            
            # Replace table_override with table_config
            migrated['table_config'] = table_config
            del migrated['table_override']
            
            logger.debug(f"Migrated device pattern '{device_config.get('pattern')}': "
                        f"table_override={table_override} -> table_config={table_config}")
        
        return migrated
    
    @staticmethod
    def migrate_config(config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Migrate entire configuration from table_override to table_config format.
        
        Args:
            config: Full configuration dictionary
            
        Returns:
            Migrated configuration
        """
        migrated_config = config.copy()
        
        if 'routes' in config:
            migrated_routes = []
            
            for route in config['routes']:
                migrated_route = route.copy()
                
                if 'device_ids' in route:
                    migrated_device_ids = []
                    
                    for device_config in route['device_ids']:
                        migrated_device = ConfigMigrator.migrate_device_config(device_config)
                        migrated_device_ids.append(migrated_device)
                    
                    migrated_route['device_ids'] = migrated_device_ids
                
                migrated_routes.append(migrated_route)
            
            migrated_config['routes'] = migrated_routes
        
        return migrated_config
    
    @staticmethod
    def migrate_file(input_file: Path, output_file: Path = None, backup: bool = True):
        """
        Migrate configuration file from table_override to table_config format.
        
        Args:
            input_file: Path to input configuration file
            output_file: Path to output file (defaults to input_file)
            backup: Whether to create a backup of the original file
        """
        if not input_file.exists():
            raise FileNotFoundError(f"Configuration file not found: {input_file}")
        
        if output_file is None:
            output_file = input_file
        
        # Create backup if requested
        if backup and output_file == input_file:
            backup_file = input_file.with_suffix(input_file.suffix + '.backup')
            backup_file.write_text(input_file.read_text())
            logger.info(f"Created backup: {backup_file}")
        
        # Load configuration
        with open(input_file, 'r') as f:
            config = yaml.safe_load(f)
        
        # Migrate configuration
        migrated_config = ConfigMigrator.migrate_config(config)
        
        # Count migrations
        migration_count = ConfigMigrator.count_migrations(config)
        
        # Save migrated configuration
        with open(output_file, 'w') as f:
            yaml.dump(migrated_config, f, default_flow_style=False, sort_keys=False, indent=2)
        
        logger.info(f"Migrated {migration_count} device configurations")
        logger.info(f"Configuration migrated successfully: {output_file}")
    
    @staticmethod
    def count_migrations(config: Dict[str, Any]) -> int:
        """Count how many device configurations need migration."""
        count = 0
        if 'routes' in config:
            for route in config['routes']:
                if 'device_ids' in route:
                    for device_config in route['device_ids']:
                        if 'table_override' in device_config:
                            count += 1
        return count
    
    @staticmethod
    def is_migration_needed(config_file: Path) -> bool:
        """Check if configuration file needs migration."""
        with open(config_file, 'r') as f:
            config = yaml.safe_load(f)
        return ConfigMigrator.count_migrations(config) > 0

def main():
    parser = argparse.ArgumentParser(description='Migrate unified_config.yml from table_override to table_config format')
    parser.add_argument('config_file', type=Path, help='Path to configuration file')
    parser.add_argument('-o', '--output', type=Path, help='Output file (defaults to input file)')
    parser.add_argument('--no-backup', action='store_true', help='Skip creating backup file')
    parser.add_argument('--check-only', action='store_true', help='Only check if migration is needed')
    parser.add_argument('-v', '--verbose', action='store_true', help='Enable verbose logging')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    try:
        if args.check_only:
            if ConfigMigrator.is_migration_needed(args.config_file):
                count = ConfigMigrator.count_migrations(yaml.safe_load(open(args.config_file)))
                print(f"Migration needed: {count} device configurations to migrate")
                return 1
            else:
                print("No migration needed")
                return 0
        
        ConfigMigrator.migrate_file(
            input_file=args.config_file,
            output_file=args.output,
            backup=not args.no_backup
        )
        
        return 0
        
    except Exception as e:
        logger.error(f"Migration failed: {e}")
        return 1

if __name__ == '__main__':
    exit(main())
