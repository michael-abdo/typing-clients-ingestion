#!/usr/bin/env python3
"""
24-Hour Database-Primary System Monitor

Comprehensive monitoring system for database-primary operations.
Tracks performance, reliability, fallback usage, and system health
over extended periods.

Usage:
- For 24h monitoring: python3 monitor_database_primary_system.py --duration 86400
- For demo (5 minutes): python3 monitor_database_primary_system.py --duration 300
- For quick test: python3 monitor_database_primary_system.py --duration 60
"""

import os
import sys
import time
import json
import argparse
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List
import statistics

# Set up environment
os.environ['DB_PASSWORD'] = 'migration_pass_2025'
sys.path.append('.')

from utils.database_primary_manager import DatabasePrimaryManager
from utils.database_logging import db_logger

class DatabasePrimaryMonitor:
    """24-hour monitoring system for database-primary operations."""
    
    def __init__(self, duration_seconds: int = 86400):
        """Initialize monitor."""
        self.duration_seconds = duration_seconds
        self.db_manager = DatabasePrimaryManager()
        
        # Monitoring configuration
        self.check_interval = 60  # Check every minute
        self.performance_test_interval = 300  # Performance test every 5 minutes
        self.report_interval = 3600  # Hourly reports
        
        # Monitoring data
        self.start_time = datetime.now()
        self.end_time = self.start_time + timedelta(seconds=duration_seconds)
        
        self.monitoring_data = {
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat(),
            'duration_hours': duration_seconds / 3600,
            'checks': [],
            'performance_tests': [],
            'hourly_summaries': [],
            'alerts': [],
            'overall_stats': {}
        }
        
        # Create monitoring directory
        self.monitoring_dir = Path("monitoring_reports")
        self.monitoring_dir.mkdir(exist_ok=True)
        
        print(f"🔍 Database-Primary System Monitor")
        print(f"Duration: {duration_seconds / 3600:.1f} hours ({duration_seconds}s)")
        print(f"Start: {self.start_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"End: {self.end_time.strftime('%Y-%m-%d %H:%M:%S')}")
        print(f"Check interval: {self.check_interval}s")
        print(f"Performance test interval: {self.performance_test_interval}s")
        print(f"Report interval: {self.report_interval}s")
    
    def perform_health_check(self) -> Dict[str, Any]:
        """Perform comprehensive system health check."""
        
        check_time = datetime.now()
        check_data = {
            'timestamp': check_time.isoformat(),
            'checks': {}
        }
        
        try:
            # Test 1: Database connectivity
            start_time = time.time()
            record_count = self.db_manager.count_records()
            db_time = time.time() - start_time
            
            check_data['checks']['database_connectivity'] = {
                'status': 'ok',
                'response_time': db_time,
                'record_count': record_count
            }
            
        except Exception as e:
            check_data['checks']['database_connectivity'] = {
                'status': 'error',
                'error': str(e)
            }
        
        try:
            # Test 2: Single record read
            start_time = time.time()
            test_record = self.db_manager.get_record_by_id(1)
            read_time = time.time() - start_time
            
            check_data['checks']['record_read'] = {
                'status': 'ok' if test_record else 'warning',
                'response_time': read_time,
                'record_found': test_record is not None
            }
            
        except Exception as e:
            check_data['checks']['record_read'] = {
                'status': 'error',
                'error': str(e)
            }
        
        try:
            # Test 3: CSV fallback capability
            start_time = time.time()
            csv_df = self.db_manager.read_dataframe(force_csv=True)
            csv_time = time.time() - start_time
            
            check_data['checks']['csv_fallback'] = {
                'status': 'ok',
                'response_time': csv_time,
                'csv_record_count': len(csv_df)
            }
            
        except Exception as e:
            check_data['checks']['csv_fallback'] = {
                'status': 'error',
                'error': str(e)
            }
        
        # Get usage statistics
        try:
            stats = self.db_manager.get_usage_statistics()
            check_data['usage_stats'] = stats
        except Exception as e:
            check_data['usage_stats'] = {'error': str(e)}
        
        # Overall health assessment
        all_checks = [check['status'] for check in check_data['checks'].values()]
        if 'error' in all_checks:
            check_data['overall_health'] = 'critical'
        elif 'warning' in all_checks:
            check_data['overall_health'] = 'warning'
        else:
            check_data['overall_health'] = 'healthy'
        
        return check_data
    
    def perform_performance_test(self) -> Dict[str, Any]:
        """Perform detailed performance testing."""
        
        test_time = datetime.now()
        perf_data = {
            'timestamp': test_time.isoformat(),
            'tests': {}
        }
        
        try:
            # Test 1: Single record performance (10 reads)
            test_ids = [1, 2, 3, 4, 5]
            times = []
            
            for test_id in test_ids:
                start_time = time.time()
                record = self.db_manager.get_record_by_id(test_id)
                end_time = time.time()
                times.append(end_time - start_time)
            
            perf_data['tests']['single_record_reads'] = {
                'avg_time': statistics.mean(times),
                'min_time': min(times),
                'max_time': max(times),
                'tests_run': len(times)
            }
            
        except Exception as e:
            perf_data['tests']['single_record_reads'] = {'error': str(e)}
        
        try:
            # Test 2: Bulk read performance
            start_time = time.time()
            bulk_df = self.db_manager.read_dataframe()
            bulk_time = time.time() - start_time
            
            perf_data['tests']['bulk_read'] = {
                'total_time': bulk_time,
                'records_read': len(bulk_df),
                'throughput': len(bulk_df) / bulk_time if bulk_time > 0 else 0
            }
            
        except Exception as e:
            perf_data['tests']['bulk_read'] = {'error': str(e)}
        
        try:
            # Test 3: Count performance
            start_time = time.time()
            count = self.db_manager.count_records()
            count_time = time.time() - start_time
            
            perf_data['tests']['count_operation'] = {
                'time': count_time,
                'count': count
            }
            
        except Exception as e:
            perf_data['tests']['count_operation'] = {'error': str(e)}
        
        return perf_data
    
    def generate_hourly_summary(self, hour_data: List[Dict]) -> Dict[str, Any]:
        """Generate hourly summary from collected data."""
        
        if not hour_data:
            return {'error': 'No data available for summary'}
        
        summary = {
            'hour': datetime.now().strftime('%Y-%m-%d %H:00'),
            'checks_performed': len(hour_data),
            'health_status': {},
            'performance_summary': {},
            'alerts_raised': 0
        }
        
        # Health status summary
        health_counts = {'healthy': 0, 'warning': 0, 'critical': 0}
        for check in hour_data:
            if 'overall_health' in check:
                health_counts[check['overall_health']] += 1
        
        summary['health_status'] = health_counts
        summary['health_percentage'] = {
            status: (count / len(hour_data) * 100) 
            for status, count in health_counts.items()
        }
        
        # Performance summary
        db_times = []
        csv_times = []
        fallback_rates = []
        
        for check in hour_data:
            if 'usage_stats' in check and isinstance(check['usage_stats'], dict):
                if 'fallback_rate' in check['usage_stats']:
                    fallback_rates.append(check['usage_stats']['fallback_rate'])
            
            if 'checks' in check:
                if 'database_connectivity' in check['checks'] and 'response_time' in check['checks']['database_connectivity']:
                    db_times.append(check['checks']['database_connectivity']['response_time'])
                
                if 'csv_fallback' in check['checks'] and 'response_time' in check['checks']['csv_fallback']:
                    csv_times.append(check['checks']['csv_fallback']['response_time'])
        
        if db_times:
            summary['performance_summary']['avg_db_response_time'] = statistics.mean(db_times)
            summary['performance_summary']['max_db_response_time'] = max(db_times)
        
        if csv_times:
            summary['performance_summary']['avg_csv_response_time'] = statistics.mean(csv_times)
        
        if fallback_rates:
            summary['performance_summary']['avg_fallback_rate'] = statistics.mean(fallback_rates)
            summary['performance_summary']['max_fallback_rate'] = max(fallback_rates)
        
        return summary
    
    def check_alerts(self, check_data: Dict[str, Any]) -> List[str]:
        """Check for alert conditions."""
        
        alerts = []
        
        # Critical alerts
        if check_data.get('overall_health') == 'critical':
            alerts.append(f"🚨 CRITICAL: System health check failed at {check_data['timestamp']}")
        
        # Performance alerts
        if 'usage_stats' in check_data and isinstance(check_data['usage_stats'], dict):
            fallback_rate = check_data['usage_stats'].get('fallback_rate', 0)
            if fallback_rate > 10:
                alerts.append(f"⚠️ WARNING: High fallback rate ({fallback_rate:.1f}%) at {check_data['timestamp']}")
        
        # Response time alerts
        if 'checks' in check_data:
            db_check = check_data['checks'].get('database_connectivity', {})
            if 'response_time' in db_check and db_check['response_time'] > 1.0:
                alerts.append(f"⚠️ WARNING: Slow database response ({db_check['response_time']:.2f}s) at {check_data['timestamp']}")
        
        return alerts
    
    def save_monitoring_data(self):
        """Save monitoring data to file."""
        
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        filename = self.monitoring_dir / f"monitoring_report_{timestamp}.json"
        
        with open(filename, 'w') as f:
            json.dump(self.monitoring_data, f, indent=2)
        
        print(f"📁 Monitoring data saved to: {filename}")
        return filename
    
    def run_monitoring(self):
        """Run the monitoring system."""
        
        print(f"\n🚀 Starting monitoring...")
        print(f"Press Ctrl+C to stop early")
        
        hour_data = []
        last_performance_test = 0
        last_hourly_report = 0
        check_count = 0
        
        try:
            while datetime.now() < self.end_time:
                current_time = time.time()
                
                # Perform health check
                print(f"\n⏰ {datetime.now().strftime('%H:%M:%S')} - Health Check #{check_count + 1}")
                check_data = self.perform_health_check()
                self.monitoring_data['checks'].append(check_data)
                hour_data.append(check_data)
                check_count += 1
                
                # Check for alerts
                alerts = self.check_alerts(check_data)
                if alerts:
                    for alert in alerts:
                        print(f"   {alert}")
                        self.monitoring_data['alerts'].append({
                            'timestamp': check_data['timestamp'],
                            'alert': alert
                        })
                
                # Report health status
                health = check_data['overall_health']
                health_icon = "✅" if health == 'healthy' else "⚠️" if health == 'warning' else "🚨"
                print(f"   {health_icon} Overall Health: {health.upper()}")
                
                if 'usage_stats' in check_data and isinstance(check_data['usage_stats'], dict):
                    stats = check_data['usage_stats']
                    print(f"   📊 DB Success: {stats.get('database_success_rate', 0):.1f}%, Fallback: {stats.get('fallback_rate', 0):.1f}%")
                
                # Performance test (every 5 minutes)
                if current_time - last_performance_test >= self.performance_test_interval:
                    print(f"   ⚡ Running performance test...")
                    perf_data = self.perform_performance_test()
                    self.monitoring_data['performance_tests'].append(perf_data)
                    last_performance_test = current_time
                    
                    if 'single_record_reads' in perf_data['tests']:
                        avg_time = perf_data['tests']['single_record_reads'].get('avg_time', 0)
                        print(f"      Single read avg: {avg_time:.3f}s")
                
                # Hourly summary (every hour)
                if current_time - last_hourly_report >= self.report_interval:
                    print(f"   📊 Generating hourly summary...")
                    hourly_summary = self.generate_hourly_summary(hour_data)
                    self.monitoring_data['hourly_summaries'].append(hourly_summary)
                    hour_data = []  # Reset for next hour
                    last_hourly_report = current_time
                    
                    print(f"      Health: {hourly_summary.get('health_percentage', {}).get('healthy', 0):.0f}% healthy")
                
                # Wait for next check
                time.sleep(self.check_interval)
                
        except KeyboardInterrupt:
            print(f"\n⏹️ Monitoring stopped by user")
        
        # Generate final summary
        self.generate_final_summary()
        
        # Save data
        self.save_monitoring_data()
    
    def generate_final_summary(self):
        """Generate final monitoring summary."""
        
        actual_duration = (datetime.now() - self.start_time).total_seconds()
        
        print(f"\n📊 Final Monitoring Summary")
        print("=" * 30)
        print(f"Monitoring duration: {actual_duration / 3600:.1f} hours")
        print(f"Health checks performed: {len(self.monitoring_data['checks'])}")
        print(f"Performance tests: {len(self.monitoring_data['performance_tests'])}")
        print(f"Alerts raised: {len(self.monitoring_data['alerts'])}")
        
        # Health statistics
        if self.monitoring_data['checks']:
            health_counts = {'healthy': 0, 'warning': 0, 'critical': 0}
            for check in self.monitoring_data['checks']:
                if 'overall_health' in check:
                    health_counts[check['overall_health']] += 1
            
            total_checks = len(self.monitoring_data['checks'])
            print(f"\nHealth Status Distribution:")
            for status, count in health_counts.items():
                percentage = (count / total_checks * 100) if total_checks > 0 else 0
                print(f"  {status.capitalize()}: {count} ({percentage:.1f}%)")
        
        # Performance statistics
        if self.monitoring_data['performance_tests']:
            single_read_times = []
            for test in self.monitoring_data['performance_tests']:
                if 'single_record_reads' in test['tests'] and 'avg_time' in test['tests']['single_record_reads']:
                    single_read_times.append(test['tests']['single_record_reads']['avg_time'])
            
            if single_read_times:
                print(f"\nPerformance Statistics:")
                print(f"  Avg single read time: {statistics.mean(single_read_times):.3f}s")
                print(f"  Best single read time: {min(single_read_times):.3f}s")
                print(f"  Worst single read time: {max(single_read_times):.3f}s")
        
        # Alerts summary
        if self.monitoring_data['alerts']:
            print(f"\nAlerts Summary:")
            for alert in self.monitoring_data['alerts'][-5:]:  # Show last 5 alerts
                print(f"  {alert['alert']}")
        
        # Overall assessment
        health_percentage = 0
        if self.monitoring_data['checks']:
            healthy_count = sum(1 for check in self.monitoring_data['checks'] 
                              if check.get('overall_health') == 'healthy')
            health_percentage = (healthy_count / len(self.monitoring_data['checks']) * 100)
        
        print(f"\n💡 Overall Assessment:")
        if health_percentage >= 95:
            print(f"✅ Excellent: {health_percentage:.1f}% uptime - System ready for production")
        elif health_percentage >= 90:
            print(f"✅ Good: {health_percentage:.1f}% uptime - Minor issues detected")
        elif health_percentage >= 80:
            print(f"⚠️ Warning: {health_percentage:.1f}% uptime - Review system configuration")
        else:
            print(f"🚨 Critical: {health_percentage:.1f}% uptime - System needs immediate attention")

def main():
    """Main monitoring function."""
    
    parser = argparse.ArgumentParser(description='Database-Primary System Monitor')
    parser.add_argument('--duration', type=int, default=300,
                       help='Monitoring duration in seconds (default: 300 = 5 minutes)')
    parser.add_argument('--interval', type=int, default=60,
                       help='Check interval in seconds (default: 60)')
    
    args = parser.parse_args()
    
    if args.duration == 86400:
        print("🕐 24-hour monitoring mode selected")
    elif args.duration >= 3600:
        print(f"🕐 {args.duration / 3600:.1f}-hour monitoring mode selected")
    else:
        print(f"🕐 {args.duration / 60:.0f}-minute demo mode selected")
    
    monitor = DatabasePrimaryMonitor(args.duration)
    monitor.check_interval = args.interval
    monitor.run_monitoring()

if __name__ == "__main__":
    main()