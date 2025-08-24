#!/usr/bin/env python3
"""
System monitoring script for the unified MQTT ingestor
Monitors CPU, RAM, disk usage, network stats, and application-specific metrics
"""

import psutil
import time
import os
import signal
import subprocess
import logging
from datetime import datetime
from typing import Dict, Optional
import json
import argparse

class SystemMonitor:
    def __init__(self, refresh_interval: int = 5, process_name: str = "python"):
        self.refresh_interval = refresh_interval
        self.process_name = process_name
        self.target_process: Optional[psutil.Process] = None
        self.start_time = time.time()
        
        # Setup logging
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler('system_monitor.log'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def find_target_process(self) -> Optional[psutil.Process]:
        """Find the main.py process or any Python process containing 'main.py'"""
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                # Look for Python processes running main.py
                if proc.info['name'] and 'python' in proc.info['name'].lower():
                    cmdline = proc.info['cmdline']
                    if cmdline and any('main.py' in arg for arg in cmdline):
                        return psutil.Process(proc.info['pid'])
            except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                continue
        return None

    def get_system_metrics(self) -> Dict:
        """Get overall system metrics"""
        # CPU metrics
        cpu_percent = psutil.cpu_percent(interval=1)
        cpu_count = psutil.cpu_count()
        cpu_freq = psutil.cpu_freq()
        
        # Memory metrics
        memory = psutil.virtual_memory()
        swap = psutil.swap_memory()
        
        # Disk metrics
        disk = psutil.disk_usage('/')
        
        # Network metrics
        network = psutil.net_io_counters()
        
        return {
            'timestamp': datetime.now().isoformat(),
            'cpu': {
                'percent': cpu_percent,
                'count': cpu_count,
                'frequency_mhz': cpu_freq.current if cpu_freq else None
            },
            'memory': {
                'total_gb': round(memory.total / (1024**3), 2),
                'available_gb': round(memory.available / (1024**3), 2),
                'used_gb': round(memory.used / (1024**3), 2),
                'percent': memory.percent,
                'free_gb': round(memory.free / (1024**3), 2)
            },
            'swap': {
                'total_gb': round(swap.total / (1024**3), 2),
                'used_gb': round(swap.used / (1024**3), 2),
                'percent': swap.percent
            },
            'disk': {
                'total_gb': round(disk.total / (1024**3), 2),
                'used_gb': round(disk.used / (1024**3), 2),
                'free_gb': round(disk.free / (1024**3), 2),
                'percent': round((disk.used / disk.total) * 100, 2)
            },
            'network': {
                'bytes_sent': network.bytes_sent,
                'bytes_recv': network.bytes_recv,
                'packets_sent': network.packets_sent,
                'packets_recv': network.packets_recv
            }
        }

    def get_process_metrics(self, process: psutil.Process) -> Dict:
        """Get metrics for the target process"""
        try:
            # Process info
            with process.oneshot():
                cpu_percent = process.cpu_percent()
                memory_info = process.memory_info()
                memory_percent = process.memory_percent()
                num_threads = process.num_threads()
                num_fds = process.num_fds() if hasattr(process, 'num_fds') else None
                connections = len(process.connections()) if hasattr(process, 'connections') else 0
                
                return {
                    'pid': process.pid,
                    'name': process.name(),
                    'status': process.status(),
                    'cpu_percent': cpu_percent,
                    'memory': {
                        'rss_mb': round(memory_info.rss / (1024**2), 2),
                        'vms_mb': round(memory_info.vms / (1024**2), 2),
                        'percent': round(memory_percent, 2)
                    },
                    'threads': num_threads,
                    'file_descriptors': num_fds,
                    'network_connections': connections,
                    'create_time': datetime.fromtimestamp(process.create_time()).isoformat()
                }
        except (psutil.NoSuchProcess, psutil.AccessDenied):
            return None

    def get_mqtt_specific_metrics(self) -> Dict:
        """Get MQTT ingestor specific metrics from log analysis"""
        metrics = {
            'log_entries': 0,
            'error_count': 0,
            'warning_count': 0,
            'topics_seen': set(),
            'devices_seen': set(),
            'last_activity': None
        }
        
        # Try to read recent log entries
        try:
            # Look for application logs
            log_files = ['unified_ingestor.log', 'app.log', 'main.log']
            for log_file in log_files:
                if os.path.exists(log_file):
                    with open(log_file, 'r') as f:
                        lines = f.readlines()
                        # Analyze last 100 lines
                        for line in lines[-100:]:
                            metrics['log_entries'] += 1
                            if 'ERROR' in line:
                                metrics['error_count'] += 1
                            elif 'WARNING' in line:
                                metrics['warning_count'] += 1
                            
                            # Extract topic and device info from log lines
                            if 'topic=' in line:
                                try:
                                    topic_part = line.split('topic=')[1].split()[0]
                                    metrics['topics_seen'].add(topic_part)
                                except:
                                    pass
                            
                            if 'device=' in line:
                                try:
                                    device_part = line.split('device=')[1].split()[0]
                                    metrics['devices_seen'].add(device_part)
                                except:
                                    pass
                    
                    metrics['last_activity'] = datetime.now().isoformat()
                    break
        except Exception as e:
            self.logger.warning(f"Could not read log files: {e}")
        
        # Convert sets to lists for JSON serialization
        metrics['topics_seen'] = list(metrics['topics_seen'])
        metrics['devices_seen'] = list(metrics['devices_seen'])
        
        return metrics

    def display_metrics(self, system_metrics: Dict, process_metrics: Dict = None, mqtt_metrics: Dict = None):
        """Display metrics in a formatted way"""
        
        # Clear screen
        os.system('clear' if os.name == 'posix' else 'cls')
        
        print("🔧 UNIFIED MQTT INGESTOR - SYSTEM MONITOR")
        print("=" * 60)
        print(f"📅 {system_metrics['timestamp']}")
        print(f"⏱️  Monitor running for: {self.format_uptime(time.time() - self.start_time)}")
        print()
        
        # System metrics
        print("🖥️  SYSTEM METRICS")
        print("-" * 40)
        cpu = system_metrics['cpu']
        memory = system_metrics['memory']
        disk = system_metrics['disk']
        
        print(f"CPU Usage:     {cpu['percent']:6.1f}% ({cpu['count']} cores)")
        if cpu['frequency_mhz']:
            print(f"CPU Frequency: {cpu['frequency_mhz']:6.0f} MHz")
        
        print(f"Memory Used:   {memory['used_gb']:6.2f} GB / {memory['total_gb']:.2f} GB ({memory['percent']:.1f}%)")
        print(f"Memory Free:   {memory['available_gb']:6.2f} GB")
        
        print(f"Disk Usage:    {disk['used_gb']:6.2f} GB / {disk['total_gb']:.2f} GB ({disk['percent']:.1f}%)")
        print(f"Disk Free:     {disk['free_gb']:6.2f} GB")
        
        # Network
        network = system_metrics['network']
        print(f"Network RX:    {self.format_bytes(network['bytes_recv'])}")
        print(f"Network TX:    {self.format_bytes(network['bytes_sent'])}")
        print()
        
        # Process metrics
        if process_metrics:
            print("📱 APPLICATION METRICS")
            print("-" * 40)
            print(f"Process ID:    {process_metrics['pid']}")
            print(f"Status:        {process_metrics['status']}")
            print(f"CPU Usage:     {process_metrics['cpu_percent']:6.1f}%")
            print(f"Memory (RSS):  {process_metrics['memory']['rss_mb']:6.1f} MB ({process_metrics['memory']['percent']:.1f}%)")
            print(f"Memory (VMS):  {process_metrics['memory']['vms_mb']:6.1f} MB")
            print(f"Threads:       {process_metrics['threads']}")
            if process_metrics['file_descriptors']:
                print(f"File Descriptors: {process_metrics['file_descriptors']}")
            print(f"Network Connections: {process_metrics['network_connections']}")
            print(f"Started:       {process_metrics['create_time']}")
            print()
        else:
            print("📱 APPLICATION METRICS")
            print("-" * 40)
            print("❌ Target process not found (main.py not running)")
            print()
        
        # MQTT specific metrics
        if mqtt_metrics:
            print("📡 MQTT INGESTOR METRICS")
            print("-" * 40)
            print(f"Log Entries:   {mqtt_metrics['log_entries']}")
            print(f"Errors:        {mqtt_metrics['error_count']}")
            print(f"Warnings:      {mqtt_metrics['warning_count']}")
            print(f"Topics Seen:   {len(mqtt_metrics['topics_seen'])} ({', '.join(mqtt_metrics['topics_seen'][:5])}{'...' if len(mqtt_metrics['topics_seen']) > 5 else ''})")
            print(f"Devices Seen:  {len(mqtt_metrics['devices_seen'])} ({', '.join(mqtt_metrics['devices_seen'][:5])}{'...' if len(mqtt_metrics['devices_seen']) > 5 else ''})")
            if mqtt_metrics['last_activity']:
                print(f"Last Activity: {mqtt_metrics['last_activity']}")
            print()
        
        print("📊 PERFORMANCE INDICATORS")
        print("-" * 40)
        
        # Performance warnings
        warnings = []
        if cpu['percent'] > 80:
            warnings.append("⚠️  High CPU usage")
        if memory['percent'] > 85:
            warnings.append("⚠️  High memory usage")
        if disk['percent'] > 90:
            warnings.append("⚠️  Low disk space")
        if process_metrics and process_metrics['memory']['rss_mb'] > 1000:
            warnings.append("⚠️  Application using >1GB RAM")
        if mqtt_metrics and mqtt_metrics['error_count'] > 5:
            warnings.append("⚠️  Multiple errors in logs")
        
        if warnings:
            for warning in warnings:
                print(warning)
        else:
            print("✅ All systems normal")
        
        print()
        print(f"🔄 Refreshing every {self.refresh_interval} seconds... (Press Ctrl+C to stop)")

    def format_bytes(self, bytes_value: int) -> str:
        """Format bytes in human readable format"""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if bytes_value < 1024:
                return f"{bytes_value:.1f} {unit}"
            bytes_value /= 1024
        return f"{bytes_value:.1f} PB"

    def format_uptime(self, seconds: float) -> str:
        """Format uptime in human readable format"""
        hours, remainder = divmod(int(seconds), 3600)
        minutes, seconds = divmod(remainder, 60)
        return f"{hours:02d}:{minutes:02d}:{seconds:02d}"

    def save_metrics_to_file(self, system_metrics: Dict, process_metrics: Dict = None, mqtt_metrics: Dict = None):
        """Save metrics to JSON file for historical analysis"""
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        all_metrics = {
            'timestamp': timestamp,
            'system': system_metrics,
            'process': process_metrics,
            'mqtt': mqtt_metrics
        }
        
        # Save to JSON file
        metrics_file = f"metrics_{timestamp}.json"
        try:
            with open(metrics_file, 'w') as f:
                json.dump(all_metrics, f, indent=2)
        except Exception as e:
            self.logger.error(f"Could not save metrics to file: {e}")

    def run(self, save_to_file: bool = False):
        """Main monitoring loop"""
        print("🚀 Starting system monitor...")
        print(f"Looking for process containing 'main.py'...")
        
        try:
            while True:
                # Get system metrics
                system_metrics = self.get_system_metrics()
                
                # Try to find and monitor the target process
                if not self.target_process or not self.target_process.is_running():
                    self.target_process = self.find_target_process()
                
                process_metrics = None
                if self.target_process:
                    process_metrics = self.get_process_metrics(self.target_process)
                
                # Get MQTT specific metrics
                mqtt_metrics = self.get_mqtt_specific_metrics()
                
                # Display metrics
                self.display_metrics(system_metrics, process_metrics, mqtt_metrics)
                
                # Save to file if requested
                if save_to_file:
                    self.save_metrics_to_file(system_metrics, process_metrics, mqtt_metrics)
                
                # Wait for next refresh
                time.sleep(self.refresh_interval)
                
        except KeyboardInterrupt:
            print("\n\n👋 Monitor stopped by user")
        except Exception as e:
            self.logger.error(f"Monitor error: {e}")
            print(f"\n❌ Monitor error: {e}")

def main():
    parser = argparse.ArgumentParser(description='System monitor for MQTT ingestor')
    parser.add_argument('--interval', '-i', type=int, default=5, 
                       help='Refresh interval in seconds (default: 5)')
    parser.add_argument('--save', '-s', action='store_true',
                       help='Save metrics to JSON files')
    parser.add_argument('--process', '-p', type=str, default='python',
                       help='Process name to monitor (default: python)')
    
    args = parser.parse_args()
    
    monitor = SystemMonitor(
        refresh_interval=args.interval,
        process_name=args.process
    )
    
    monitor.run(save_to_file=args.save)

if __name__ == "__main__":
    main()
