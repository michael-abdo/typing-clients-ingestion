#!/usr/bin/env python3
"""
Monitoring and Status Reporting - Production monitoring for download tracking system
Comprehensive monitoring, alerting, and reporting for the row-centric download system
"""

import os
import json
import time
from typing import Dict, Any, List, Optional
from datetime import datetime, timedelta
from dataclasses import dataclass, asdict
from pathlib import Path

try:
    import pandas as pd
    from logging_config import get_logger
    from csv_tracker import get_download_status_summary, get_failed_downloads
    from error_handling import ErrorHandler
except ImportError:
    from .logging_config import get_logger
    from .csv_tracker import get_download_status_summary, get_failed_downloads
    from .error_handling import ErrorHandler


@dataclass
class SystemMetrics:
    """System performance and health metrics"""
    timestamp: str
    total_rows: int
    pending_downloads: int
    completed_downloads: int
    failed_downloads: int
    success_rate: float
    avg_download_time: Optional[float]
    disk_usage_gb: float
    error_rate: float
    active_processes: int


@dataclass
class DownloadStats:
    """Download statistics by type"""
    youtube_pending: int = 0
    youtube_completed: int = 0
    youtube_failed: int = 0
    youtube_success_rate: float = 0.0
    drive_pending: int = 0
    drive_completed: int = 0
    drive_failed: int = 0
    drive_success_rate: float = 0.0
    total_files_downloaded: int = 0
    total_size_mb: float = 0.0


@dataclass
class AlertCondition:
    """Alert condition configuration"""
    name: str
    condition_type: str  # 'threshold', 'rate', 'count'
    metric: str
    threshold: float
    severity: str  # 'info', 'warning', 'error', 'critical'
    enabled: bool = True
    last_triggered: Optional[str] = None


class DownloadMonitor:
    """Production monitoring system for download tracking"""
    
    def __init__(self, csv_path: str = 'output.csv', metrics_dir: str = 'logs/metrics'):
        self.csv_path = csv_path
        self.metrics_dir = Path(metrics_dir)
        self.metrics_dir.mkdir(parents=True, exist_ok=True)
        self.logger = get_logger(__name__)
        self.error_handler = ErrorHandler(self.logger)
        
        # Default alert conditions
        self.alert_conditions = [
            AlertCondition("high_failure_rate", "threshold", "error_rate", 0.2, "warning"),
            AlertCondition("low_disk_space", "threshold", "disk_usage_gb", 1.0, "error"),
            AlertCondition("stuck_downloads", "count", "pending_downloads", 100, "warning"),
            AlertCondition("critical_errors", "count", "critical_errors", 1, "critical")
        ]
        
    def collect_metrics(self) -> SystemMetrics:
        """Collect comprehensive system metrics"""
        try:
            # Get download status
            status_summary = get_download_status_summary(self.csv_path)
            
            # Calculate overall stats
            total_pending = status_summary['youtube']['pending'] + status_summary['drive']['pending']
            total_completed = status_summary['youtube']['completed'] + status_summary['drive']['completed']
            total_failed = status_summary['youtube']['failed'] + status_summary['drive']['failed']
            total_downloads = total_completed + total_failed
            
            success_rate = total_completed / total_downloads if total_downloads > 0 else 0.0
            error_rate = total_failed / total_downloads if total_downloads > 0 else 0.0
            
            # Get disk usage
            disk_usage = self._get_disk_usage()
            
            # Get download time estimates
            avg_download_time = self._estimate_avg_download_time()
            
            # Count active processes
            active_processes = self._count_active_processes()
            
            metrics = SystemMetrics(
                timestamp=datetime.now().isoformat(),
                total_rows=status_summary['total_rows'],
                pending_downloads=total_pending,
                completed_downloads=total_completed,
                failed_downloads=total_failed,
                success_rate=success_rate,
                avg_download_time=avg_download_time,
                disk_usage_gb=disk_usage,
                error_rate=error_rate,
                active_processes=active_processes
            )
            
            # Save metrics
            self._save_metrics(metrics)
            
            return metrics
            
        except Exception as e:
            error_context = self.error_handler.handle_error(e, {'operation': 'collect_metrics'})
            raise
    
    def get_download_stats(self) -> DownloadStats:
        """Get detailed download statistics"""
        try:
            status_summary = get_download_status_summary(self.csv_path)
            
            # Calculate success rates
            youtube_total = status_summary['youtube']['completed'] + status_summary['youtube']['failed']
            youtube_success_rate = status_summary['youtube']['completed'] / youtube_total if youtube_total > 0 else 0.0
            
            drive_total = status_summary['drive']['completed'] + status_summary['drive']['failed']
            drive_success_rate = status_summary['drive']['completed'] / drive_total if drive_total > 0 else 0.0
            
            # Get file counts and sizes
            total_files, total_size = self._get_download_file_stats()
            
            return DownloadStats(
                youtube_pending=status_summary['youtube']['pending'],
                youtube_completed=status_summary['youtube']['completed'],
                youtube_failed=status_summary['youtube']['failed'],
                youtube_success_rate=youtube_success_rate,
                drive_pending=status_summary['drive']['pending'],
                drive_completed=status_summary['drive']['completed'],
                drive_failed=status_summary['drive']['failed'],
                drive_success_rate=drive_success_rate,
                total_files_downloaded=total_files,
                total_size_mb=total_size
            )
            
        except Exception as e:
            error_context = self.error_handler.handle_error(e, {'operation': 'get_download_stats'})
            return DownloadStats()  # Return empty stats on error
    
    def check_alerts(self, metrics: SystemMetrics) -> List[Dict[str, Any]]:
        """Check alert conditions and return triggered alerts"""
        triggered_alerts = []
        
        for condition in self.alert_conditions:
            if not condition.enabled:
                continue
                
            try:
                if self._evaluate_condition(condition, metrics):
                    alert = {
                        'name': condition.name,
                        'severity': condition.severity,
                        'message': self._format_alert_message(condition, metrics),
                        'timestamp': datetime.now().isoformat(),
                        'metric_value': getattr(metrics, condition.metric, None)
                    }
                    triggered_alerts.append(alert)
                    condition.last_triggered = alert['timestamp']
                    
                    # Log alert
                    self.logger.warning(f"ALERT: {alert['message']}")
                    
            except Exception as e:
                self.error_handler.handle_error(e, {'alert_condition': condition.name})
                
        return triggered_alerts
    
    def generate_report(self, include_details: bool = False) -> Dict[str, Any]:
        """Generate comprehensive status report"""
        try:
            metrics = self.collect_metrics()
            download_stats = self.get_download_stats()
            failed_downloads = get_failed_downloads(self.csv_path, 'both') if include_details else []
            
            # Get historical metrics
            historical_metrics = self._get_historical_metrics(hours=24)
            
            report = {
                'report_generated': datetime.now().isoformat(),
                'system_status': self._determine_system_status(metrics),
                'current_metrics': asdict(metrics),
                'download_stats': asdict(download_stats),
                'historical_summary': {
                    'metrics_count': len(historical_metrics),
                    'avg_success_rate_24h': self._calculate_avg_success_rate(historical_metrics),
                    'trend': self._calculate_trend(historical_metrics)
                },
                'alerts': self.check_alerts(metrics),
                'recommendations': self._generate_recommendations(metrics, download_stats)
            }
            
            if include_details:
                report['failed_downloads'] = [
                    {'row_id': row.row_id, 'name': row.name, 'type': row.type}
                    for row in failed_downloads[:20]  # Limit to first 20
                ]
                
            # Save report
            self._save_report(report)
            
            return report
            
        except Exception as e:
            error_context = self.error_handler.handle_error(e, {'operation': 'generate_report'})
            return {'error': str(e), 'timestamp': datetime.now().isoformat()}
    
    def _get_disk_usage(self) -> float:
        """Get disk usage in GB"""
        try:
            import shutil
            total, used, free = shutil.disk_usage('.')
            return free / (1024 ** 3)  # Convert to GB
        except:
            return 0.0
    
    def _estimate_avg_download_time(self) -> Optional[float]:
        """Estimate average download time from recent logs"""
        try:
            # Look for recent log files with timing info
            log_dir = Path('logs/runs')
            if not log_dir.exists():
                return None
                
            recent_runs = sorted(log_dir.glob('2025-*'), reverse=True)[:5]
            times = []
            
            for run_dir in recent_runs:
                summary_file = run_dir / 'summary.json'
                if summary_file.exists():
                    with open(summary_file) as f:
                        data = json.load(f)
                        if 'duration_seconds' in data:
                            times.append(data['duration_seconds'])
                            
            return sum(times) / len(times) if times else None
            
        except:
            return None
    
    def _count_active_processes(self) -> int:
        """Count active download processes"""
        try:
            import psutil
            count = 0
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    cmdline = ' '.join(proc.info['cmdline'] or [])
                    if any(keyword in cmdline for keyword in ['yt-dlp', 'download_youtube', 'download_drive']):
                        count += 1
                except:
                    continue
            return count
        except ImportError:
            return 0
    
    def _get_download_file_stats(self) -> tuple[int, float]:
        """Get total file count and size"""
        try:
            total_files = 0
            total_size = 0.0
            
            for download_dir in ['youtube_downloads', 'drive_downloads']:
                if os.path.exists(download_dir):
                    for file_path in Path(download_dir).rglob('*'):
                        if file_path.is_file() and not file_path.name.endswith('_metadata.json'):
                            total_files += 1
                            total_size += file_path.stat().st_size
                            
            return total_files, total_size / (1024 * 1024)  # Convert to MB
            
        except:
            return 0, 0.0
    
    def _evaluate_condition(self, condition: AlertCondition, metrics: SystemMetrics) -> bool:
        """Evaluate alert condition"""
        metric_value = getattr(metrics, condition.metric, 0)
        
        if condition.condition_type == 'threshold':
            if condition.metric == 'disk_usage_gb':
                return metric_value < condition.threshold  # Alert on low disk space
            else:
                return metric_value > condition.threshold  # Alert on high values
        elif condition.condition_type == 'count':
            return metric_value >= condition.threshold
        elif condition.condition_type == 'rate':
            return metric_value > condition.threshold
            
        return False
    
    def _format_alert_message(self, condition: AlertCondition, metrics: SystemMetrics) -> str:
        """Format alert message"""
        metric_value = getattr(metrics, condition.metric, 0)
        
        messages = {
            'high_failure_rate': f"High failure rate: {metric_value:.1%} (threshold: {condition.threshold:.1%})",
            'low_disk_space': f"Low disk space: {metric_value:.1f}GB free (threshold: {condition.threshold}GB)",
            'stuck_downloads': f"Many pending downloads: {metric_value} (threshold: {condition.threshold})",
            'critical_errors': f"Critical errors detected: {metric_value}"
        }
        
        return messages.get(condition.name, f"{condition.name}: {metric_value} > {condition.threshold}")
    
    def _save_metrics(self, metrics: SystemMetrics):
        """Save metrics to file"""
        try:
            date_str = datetime.now().strftime('%Y-%m-%d')
            metrics_file = self.metrics_dir / f"metrics_{date_str}.jsonl"
            
            with open(metrics_file, 'a') as f:
                f.write(json.dumps(asdict(metrics)) + '\n')
                
        except Exception as e:
            self.error_handler.handle_error(e, {'operation': 'save_metrics'})
    
    def _save_report(self, report: Dict[str, Any]):
        """Save report to file"""
        try:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            report_file = self.metrics_dir / f"report_{timestamp}.json"
            
            with open(report_file, 'w') as f:
                json.dump(report, f, indent=2)
                
        except Exception as e:
            self.error_handler.handle_error(e, {'operation': 'save_report'})
    
    def _get_historical_metrics(self, hours: int = 24) -> List[SystemMetrics]:
        """Get historical metrics"""
        try:
            cutoff_time = datetime.now() - timedelta(hours=hours)
            metrics = []
            
            # Read recent metrics files
            for metrics_file in self.metrics_dir.glob("metrics_*.jsonl"):
                try:
                    with open(metrics_file) as f:
                        for line in f:
                            data = json.loads(line.strip())
                            timestamp = datetime.fromisoformat(data['timestamp'])
                            if timestamp > cutoff_time:
                                metrics.append(SystemMetrics(**data))
                except:
                    continue
                    
            return sorted(metrics, key=lambda m: m.timestamp)
            
        except:
            return []
    
    def _calculate_avg_success_rate(self, metrics: List[SystemMetrics]) -> float:
        """Calculate average success rate"""
        if not metrics:
            return 0.0
        return sum(m.success_rate for m in metrics) / len(metrics)
    
    def _calculate_trend(self, metrics: List[SystemMetrics]) -> str:
        """Calculate trend direction"""
        if len(metrics) < 2:
            return "stable"
            
        recent = metrics[-5:]  # Last 5 metrics
        older = metrics[-10:-5] if len(metrics) >= 10 else metrics[:-5]
        
        if not older:
            return "stable"
            
        recent_avg = sum(m.success_rate for m in recent) / len(recent)
        older_avg = sum(m.success_rate for m in older) / len(older)
        
        diff = recent_avg - older_avg
        if diff > 0.05:
            return "improving"
        elif diff < -0.05:
            return "declining"
        else:
            return "stable"
    
    def _determine_system_status(self, metrics: SystemMetrics) -> str:
        """Determine overall system status"""
        if metrics.error_rate > 0.5:
            return "critical"
        elif metrics.error_rate > 0.2 or metrics.disk_usage_gb < 1.0:
            return "warning"
        elif metrics.success_rate > 0.8:
            return "healthy"
        else:
            return "degraded"
    
    def _generate_recommendations(self, metrics: SystemMetrics, stats: DownloadStats) -> List[str]:
        """Generate actionable recommendations"""
        recommendations = []
        
        if metrics.error_rate > 0.2:
            recommendations.append("High error rate detected. Check failed downloads and retry with --retry-failed")
            
        if metrics.disk_usage_gb < 5.0:
            recommendations.append("Low disk space. Consider cleaning old downloads or expanding storage")
            
        if stats.youtube_success_rate < 0.7:
            recommendations.append("YouTube download success rate is low. Check yt-dlp version and network connectivity")
            
        if stats.drive_success_rate < 0.7:
            recommendations.append("Google Drive download success rate is low. Check permissions and rate limits")
            
        if metrics.pending_downloads > 100:
            recommendations.append("Many pending downloads. Consider increasing max-youtube and max-drive limits")
            
        if not recommendations:
            recommendations.append("System operating normally. No immediate actions required")
            
        return recommendations


if __name__ == "__main__":
    """CLI interface for monitoring"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Download System Monitor")
    parser.add_argument('--status', action='store_true',
                       help='Show current system status')
    parser.add_argument('--report', action='store_true',
                       help='Generate full status report')
    parser.add_argument('--metrics', action='store_true',
                       help='Show current metrics')
    parser.add_argument('--alerts', action='store_true',
                       help='Check alert conditions')
    parser.add_argument('--detailed', action='store_true',
                       help='Include detailed information')
    parser.add_argument('--csv-path', default='output.csv',
                       help='Path to CSV file (default: output.csv)')
    
    args = parser.parse_args()
    
    monitor = DownloadMonitor(args.csv_path)
    
    if args.status:
        metrics = monitor.collect_metrics()
        stats = monitor.get_download_stats()
        status = monitor._determine_system_status(metrics)
        
        print(f"\n📊 System Status: {status.upper()}")
        print(f"Success Rate: {metrics.success_rate:.1%}")
        print(f"Pending Downloads: {metrics.pending_downloads}")
        print(f"Failed Downloads: {metrics.failed_downloads}")
        print(f"Disk Space: {metrics.disk_usage_gb:.1f}GB free")
        
    elif args.report:
        report = monitor.generate_report(include_details=args.detailed)
        
        print(f"\n📋 Status Report - {report['report_generated']}")
        print(f"System Status: {report['system_status'].upper()}")
        print(f"Success Rate: {report['current_metrics']['success_rate']:.1%}")
        
        if report['alerts']:
            print(f"\n⚠️  Active Alerts:")
            for alert in report['alerts']:
                print(f"  • {alert['severity'].upper()}: {alert['message']}")
        
        if report['recommendations']:
            print(f"\n💡 Recommendations:")
            for rec in report['recommendations']:
                print(f"  • {rec}")
                
    elif args.metrics:
        metrics = monitor.collect_metrics()
        stats = monitor.get_download_stats()
        
        print(f"\n📈 Current Metrics:")
        print(f"Total Rows: {metrics.total_rows}")
        print(f"Pending: {metrics.pending_downloads}")
        print(f"Completed: {metrics.completed_downloads}")
        print(f"Failed: {metrics.failed_downloads}")
        print(f"Success Rate: {metrics.success_rate:.1%}")
        print(f"Error Rate: {metrics.error_rate:.1%}")
        print(f"Disk Usage: {metrics.disk_usage_gb:.1f}GB free")
        
        if args.detailed:
            print(f"\n📁 Download Stats:")
            print(f"YouTube Success Rate: {stats.youtube_success_rate:.1%}")
            print(f"Drive Success Rate: {stats.drive_success_rate:.1%}")
            print(f"Total Files: {stats.total_files_downloaded}")
            print(f"Total Size: {stats.total_size_mb:.1f}MB")
        
    elif args.alerts:
        metrics = monitor.collect_metrics()
        alerts = monitor.check_alerts(metrics)
        
        if alerts:
            print(f"\n⚠️  {len(alerts)} Active Alerts:")
            for alert in alerts:
                print(f"  • {alert['severity'].upper()}: {alert['message']}")
        else:
            print("\n✅ No active alerts")
            
    else:
        parser.print_help()