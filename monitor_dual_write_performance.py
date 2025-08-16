#!/usr/bin/env python3
"""
Monitor Dual-Write Performance and Log Discrepancies

Comprehensive monitoring system for dual-write operations:
1. Performance metrics (timing, throughput, error rates)
2. Data consistency monitoring  
3. Discrepancy detection and logging
4. Stress testing under load
5. Performance benchmarking
"""

import os
import sys
import time
import json
import threading
import pandas as pd
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed
import statistics

# Set up environment
os.environ['DB_PASSWORD'] = 'migration_pass_2025'
sys.path.append('.')

from utils.database_operations import DatabaseConfig, DatabaseManager
from utils.data_validation import DataValidator
from utils.database_logging import db_logger

class DualWritePerformanceMonitor:
    """Comprehensive performance monitoring for dual-write operations."""
    
    def __init__(self):
        """Initialize performance monitor."""
        
        # Database configuration
        self.config = DatabaseConfig(
            db_type='postgresql',
            host='localhost',
            port=5432,
            database='typing_clients_uuid',
            username='migration_user',
            password='migration_pass_2025'
        )
        
        self.db_manager = DatabaseManager(self.config)
        self.validator = DataValidator()
        self.csv_file = Path("outputs/output.csv")
        
        # Performance metrics
        self.metrics = {
            'total_operations': 0,
            'successful_dual_writes': 0,
            'failed_operations': 0,
            'csv_only_fallbacks': 0,
            'db_only_writes': 0,
            'validation_failures': 0,
            'discrepancies_detected': 0,
            'operation_times': [],
            'db_write_times': [],
            'csv_write_times': [],
            'validation_times': [],
            'start_time': None,
            'end_time': None
        }
        
        # Discrepancy log
        self.discrepancies = []
        
        # Performance thresholds
        self.thresholds = {
            'max_operation_time': 5.0,  # seconds
            'max_validation_time': 1.0,  # seconds
            'max_error_rate': 0.05,     # 5%
            'max_fallback_rate': 0.10   # 10%
        }
    
    def log_performance_metric(self, operation: str, duration: float, 
                              status: str, details: Dict[str, Any] = None):
        """Log performance metric with timing."""
        
        metric_entry = {
            'timestamp': datetime.now().isoformat(),
            'operation': operation,
            'duration_seconds': duration,
            'status': status,
            'details': details or {}
        }
        
        # Log to database logger
        db_logger.info(f"📊 PERFORMANCE: {operation} - {status} in {duration:.3f}s")
        
        # Track in metrics
        self.metrics['operation_times'].append(duration)
        
        if status == 'success':
            self.metrics['successful_dual_writes'] += 1
        elif status == 'failed':
            self.metrics['failed_operations'] += 1
        elif status == 'csv_fallback':
            self.metrics['csv_only_fallbacks'] += 1
    
    def detect_discrepancy(self, record_id: Any, csv_data: Dict[str, Any], 
                          db_data: Dict[str, Any], error_details: str):
        """Log data discrepancy between CSV and database."""
        
        discrepancy = {
            'timestamp': datetime.now().isoformat(),
            'record_id': record_id,
            'error_type': 'data_mismatch',
            'error_details': error_details,
            'csv_data_sample': {k: str(v)[:100] for k, v in csv_data.items()},
            'db_data_sample': {k: str(v)[:100] for k, v in db_data.items()}
        }
        
        self.discrepancies.append(discrepancy)
        self.metrics['discrepancies_detected'] += 1
        
        # Log critical discrepancy
        db_logger.error(f"🚨 DISCREPANCY DETECTED: Record {record_id}")
        db_logger.error(f"   Error: {error_details}")
        db_logger.error(f"   CSV sample: {discrepancy['csv_data_sample']}")
        db_logger.error(f"   DB sample: {discrepancy['db_data_sample']}")
    
    def perform_monitored_dual_write(self, record: Dict[str, Any]) -> Tuple[bool, Dict[str, float]]:
        """Perform dual-write with comprehensive performance monitoring."""
        
        operation_start = time.time()
        timings = {}
        
        try:
            self.metrics['total_operations'] += 1
            record_id = record.get('row_id', 'unknown')
            
            # Phase 1: Database write with timing
            db_start = time.time()
            db_success = False
            
            try:
                with self.db_manager.get_connection() as conn:
                    cursor = conn.cursor()
                    
                    # Normalize and prepare record
                    normalized_record = self.validator.normalize_csv_row(record)
                    clean_record = {k: v for k, v in normalized_record.items() 
                                  if k not in ['created_at', 'updated_at']}
                    
                    # Convert JSON fields
                    for key in ['file_uuids', 's3_paths']:
                        if key in clean_record and isinstance(clean_record[key], dict):
                            clean_record[key] = json.dumps(clean_record[key])
                    
                    # Insert to database
                    columns = list(clean_record.keys())
                    placeholders = ['%s' for _ in columns]
                    values = list(clean_record.values())
                    
                    sql = f"""
                        INSERT INTO typing_clients_data ({', '.join(columns)}) 
                        VALUES ({', '.join(placeholders)})
                        ON CONFLICT (row_id) DO UPDATE SET name = EXCLUDED.name
                    """
                    
                    cursor.execute(sql, values)
                    conn.commit()
                    db_success = True
                    
            except Exception as db_error:
                db_logger.error(f"Database write failed for record {record_id}: {db_error}")
            
            db_time = time.time() - db_start
            timings['db_write_time'] = db_time
            self.metrics['db_write_times'].append(db_time)
            
            # Phase 2: CSV write with timing
            csv_start = time.time()
            csv_success = False
            
            try:
                # Normalize for CSV
                csv_record = self.validator.normalize_csv_row(record)
                for key in ['file_uuids', 's3_paths']:
                    if key in csv_record and isinstance(csv_record[key], dict):
                        csv_record[key] = json.dumps(csv_record[key])
                
                # Write to CSV
                if self.csv_file.exists():
                    df = pd.read_csv(self.csv_file)
                    if record['row_id'] in df['row_id'].values:
                        for key, value in csv_record.items():
                            df.loc[df['row_id'] == record['row_id'], key] = value
                        df.to_csv(self.csv_file, index=False)
                    else:
                        new_df = pd.concat([df, pd.DataFrame([csv_record])], ignore_index=True)
                        new_df.to_csv(self.csv_file, index=False)
                else:
                    df = pd.DataFrame([csv_record])
                    df.to_csv(self.csv_file, index=False)
                
                csv_success = True
                
            except Exception as csv_error:
                db_logger.error(f"CSV write failed for record {record_id}: {csv_error}")
            
            csv_time = time.time() - csv_start
            timings['csv_write_time'] = csv_time
            self.metrics['csv_write_times'].append(csv_time)
            
            # Phase 3: Validation with timing
            validation_start = time.time()
            validation_success = False
            
            if db_success and csv_success:
                try:
                    # Get database record
                    with self.db_manager.get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute("SELECT * FROM typing_clients_data WHERE row_id = %s", [record_id])
                        db_result = cursor.fetchone()
                    
                    # Get CSV record
                    df_current = pd.read_csv(self.csv_file)
                    csv_matches = df_current[df_current['row_id'] == record_id]
                    csv_result = csv_matches.iloc[0].to_dict()
                    
                    # Validate
                    self.validator.validate_dual_write(csv_result, db_result, record_id)
                    validation_success = True
                    
                except Exception as validation_error:
                    self.metrics['validation_failures'] += 1
                    self.detect_discrepancy(record_id, csv_result if 'csv_result' in locals() else {}, 
                                          dict(db_result) if 'db_result' in locals() else {}, 
                                          str(validation_error))
            
            validation_time = time.time() - validation_start
            timings['validation_time'] = validation_time
            self.metrics['validation_times'].append(validation_time)
            
            # Calculate total operation time
            operation_time = time.time() - operation_start
            timings['total_operation_time'] = operation_time
            
            # Determine overall status
            if db_success and csv_success and validation_success:
                status = 'success'
            elif csv_success and not db_success:
                status = 'csv_fallback'
                self.metrics['csv_only_fallbacks'] += 1
            elif db_success and not csv_success:
                status = 'db_only'
                self.metrics['db_only_writes'] += 1
            else:
                status = 'failed'
                self.metrics['failed_operations'] += 1
            
            # Log performance
            self.log_performance_metric('dual_write', operation_time, status, timings)
            
            # Check performance thresholds
            if operation_time > self.thresholds['max_operation_time']:
                db_logger.warning(f"⚠️ SLOW OPERATION: {operation_time:.3f}s > {self.thresholds['max_operation_time']}s")
            
            if validation_time > self.thresholds['max_validation_time']:
                db_logger.warning(f"⚠️ SLOW VALIDATION: {validation_time:.3f}s > {self.thresholds['max_validation_time']}s")
            
            return status == 'success' or status == 'csv_fallback', timings
            
        except Exception as e:
            operation_time = time.time() - operation_start
            self.log_performance_metric('dual_write', operation_time, 'failed', {'error': str(e)})
            return False, timings
    
    def stress_test_dual_write(self, num_records: int = 50, concurrent_workers: int = 5):
        """Stress test dual-write system with concurrent operations."""
        
        print(f"🏋️ Running stress test: {num_records} records, {concurrent_workers} workers")
        
        # Generate test records
        test_records = []
        base_id = 900000  # Use high IDs to avoid conflicts
        
        for i in range(num_records):
            record = {
                'row_id': base_id + i,
                'name': f'Stress Test User {i+1}',
                'email': f'stress{i+1}@example.com',
                'type': 'STRESS_TEST',
                'processed': i % 2 == 0,  # Alternate true/false
                'document_text': f'Stress test document content {i+1} - testing concurrent dual-write operations'
            }
            test_records.append(record)
        
        # Record start time
        self.metrics['start_time'] = datetime.now()
        stress_start = time.time()
        
        successful_operations = 0
        failed_operations = 0
        
        try:
            # Execute stress test with thread pool
            with ThreadPoolExecutor(max_workers=concurrent_workers) as executor:
                # Submit all operations
                future_to_record = {
                    executor.submit(self.perform_monitored_dual_write, record): record 
                    for record in test_records
                }
                
                # Collect results
                for future in as_completed(future_to_record):
                    record = future_to_record[future]
                    try:
                        success, timings = future.result()
                        if success:
                            successful_operations += 1
                        else:
                            failed_operations += 1
                    except Exception as e:
                        failed_operations += 1
                        db_logger.error(f"Stress test operation failed: {e}")
            
            # Record end time
            stress_duration = time.time() - stress_start
            self.metrics['end_time'] = datetime.now()
            
            print(f"\n📊 STRESS TEST RESULTS")
            print(f"   Total records: {num_records}")
            print(f"   Successful operations: {successful_operations}")
            print(f"   Failed operations: {failed_operations}")
            print(f"   Success rate: {successful_operations/num_records*100:.1f}%")
            print(f"   Total duration: {stress_duration:.1f}s")
            print(f"   Throughput: {num_records/stress_duration:.1f} records/sec")
            
            return successful_operations, failed_operations, stress_duration
            
        finally:
            # Cleanup test records
            self.cleanup_test_records(base_id, num_records)
    
    def cleanup_test_records(self, base_id: int, num_records: int):
        """Clean up test records from database and CSV."""
        
        try:
            test_ids = list(range(base_id, base_id + num_records))
            
            # Remove from database
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                placeholders = ','.join(['%s' for _ in test_ids])
                cursor.execute(f"DELETE FROM typing_clients_data WHERE row_id IN ({placeholders})", test_ids)
                conn.commit()
            
            # Remove from CSV
            if self.csv_file.exists():
                df = pd.read_csv(self.csv_file)
                df = df[~df['row_id'].isin(test_ids)]
                df.to_csv(self.csv_file, index=False)
            
            db_logger.info(f"🧹 Cleaned up {num_records} test records")
            
        except Exception as e:
            db_logger.error(f"Cleanup failed: {e}")
    
    def generate_performance_report(self) -> Dict[str, Any]:
        """Generate comprehensive performance report."""
        
        if not self.metrics['operation_times']:
            return {'error': 'No performance data collected'}
        
        total_operations = self.metrics['total_operations']
        operation_times = self.metrics['operation_times']
        db_times = self.metrics['db_write_times']
        csv_times = self.metrics['csv_write_times']
        validation_times = self.metrics['validation_times']
        
        report = {
            'summary': {
                'total_operations': total_operations,
                'successful_dual_writes': self.metrics['successful_dual_writes'],
                'failed_operations': self.metrics['failed_operations'],
                'csv_only_fallbacks': self.metrics['csv_only_fallbacks'],
                'validation_failures': self.metrics['validation_failures'],
                'discrepancies_detected': self.metrics['discrepancies_detected'],
                'success_rate': self.metrics['successful_dual_writes'] / total_operations * 100 if total_operations > 0 else 0,
                'fallback_rate': self.metrics['csv_only_fallbacks'] / total_operations * 100 if total_operations > 0 else 0
            },
            'timing_analysis': {
                'operation_times': {
                    'mean': statistics.mean(operation_times),
                    'median': statistics.median(operation_times),
                    'min': min(operation_times),
                    'max': max(operation_times),
                    'std_dev': statistics.stdev(operation_times) if len(operation_times) > 1 else 0
                },
                'db_write_times': {
                    'mean': statistics.mean(db_times) if db_times else 0,
                    'median': statistics.median(db_times) if db_times else 0
                },
                'csv_write_times': {
                    'mean': statistics.mean(csv_times) if csv_times else 0,
                    'median': statistics.median(csv_times) if csv_times else 0
                },
                'validation_times': {
                    'mean': statistics.mean(validation_times) if validation_times else 0,
                    'median': statistics.median(validation_times) if validation_times else 0
                }
            },
            'threshold_violations': {
                'slow_operations': len([t for t in operation_times if t > self.thresholds['max_operation_time']]),
                'slow_validations': len([t for t in validation_times if t > self.thresholds['max_validation_time']]),
                'high_error_rate': (self.metrics['failed_operations'] / total_operations) > self.thresholds['max_error_rate'],
                'high_fallback_rate': (self.metrics['csv_only_fallbacks'] / total_operations) > self.thresholds['max_fallback_rate']
            },
            'discrepancies': self.discrepancies,
            'recommendations': []
        }
        
        # Generate recommendations
        if report['threshold_violations']['high_error_rate']:
            report['recommendations'].append("High error rate detected - investigate database connectivity and error handling")
        
        if report['threshold_violations']['high_fallback_rate']:
            report['recommendations'].append("High fallback rate detected - check database performance and availability")
        
        if report['threshold_violations']['slow_operations'] > 0:
            report['recommendations'].append(f"{report['threshold_violations']['slow_operations']} slow operations detected - consider optimizing database queries or increasing resources")
        
        if self.metrics['discrepancies_detected'] > 0:
            report['recommendations'].append(f"{self.metrics['discrepancies_detected']} data discrepancies detected - investigate data validation logic")
        
        return report

def main():
    """Run comprehensive dual-write performance monitoring."""
    
    print("📊 Dual-Write Performance Monitoring and Discrepancy Detection")
    print("="*65)
    
    monitor = DualWritePerformanceMonitor()
    
    try:
        # Test 1: Basic performance test
        print("\n🧪 Test 1: Basic Performance Validation")
        test_record = {
            'row_id': 800001,
            'name': 'Performance Test User',
            'email': 'performance@example.com',
            'type': 'PERFORMANCE_TEST',
            'processed': True,
            'document_text': 'Performance testing document content'
        }
        
        success, timings = monitor.perform_monitored_dual_write(test_record)
        print(f"   Result: {'✅ SUCCESS' if success else '❌ FAILED'}")
        print(f"   Total time: {timings.get('total_operation_time', 0):.3f}s")
        print(f"   DB write: {timings.get('db_write_time', 0):.3f}s")
        print(f"   CSV write: {timings.get('csv_write_time', 0):.3f}s")
        print(f"   Validation: {timings.get('validation_time', 0):.3f}s")
        
        # Cleanup
        monitor.cleanup_test_records(800001, 1)
        
        # Test 2: Stress test
        print(f"\n🏋️ Test 2: Stress Test (50 records, 5 workers)")
        successful, failed, duration = monitor.stress_test_dual_write(50, 5)
        
        # Test 3: Generate performance report
        print(f"\n📋 Test 3: Performance Analysis Report")
        report = monitor.generate_performance_report()
        
        print(f"\n📊 PERFORMANCE SUMMARY")
        print("="*30)
        print(f"Total operations: {report['summary']['total_operations']}")
        print(f"Success rate: {report['summary']['success_rate']:.1f}%")
        print(f"Fallback rate: {report['summary']['fallback_rate']:.1f}%")
        print(f"Discrepancies: {report['summary']['discrepancies_detected']}")
        
        print(f"\n⏱️ TIMING ANALYSIS")
        print("="*20)
        timing = report['timing_analysis']['operation_times']
        print(f"Mean operation time: {timing['mean']:.3f}s")
        print(f"Median operation time: {timing['median']:.3f}s")
        print(f"Min/Max operation time: {timing['min']:.3f}s / {timing['max']:.3f}s")
        
        print(f"\n🚨 THRESHOLD VIOLATIONS")
        print("="*25)
        violations = report['threshold_violations']
        print(f"Slow operations: {violations['slow_operations']}")
        print(f"Slow validations: {violations['slow_validations']}")
        print(f"High error rate: {'⚠️ YES' if violations['high_error_rate'] else '✅ NO'}")
        print(f"High fallback rate: {'⚠️ YES' if violations['high_fallback_rate'] else '✅ NO'}")
        
        if report['recommendations']:
            print(f"\n💡 RECOMMENDATIONS")
            print("="*18)
            for i, rec in enumerate(report['recommendations'], 1):
                print(f"{i}. {rec}")
        
        # Overall assessment
        issues_found = (
            violations['high_error_rate'] or 
            violations['high_fallback_rate'] or 
            violations['slow_operations'] > 5 or
            report['summary']['discrepancies_detected'] > 0
        )
        
        if not issues_found:
            print(f"\n🎉 PERFORMANCE MONITORING PASSED")
            print(f"✅ Dual-write system performing within acceptable thresholds")
            print(f"✅ No significant discrepancies detected")
            print(f"✅ Ready for production workloads")
            return True
        else:
            print(f"\n⚠️ PERFORMANCE ISSUES DETECTED")
            print(f"❌ Review recommendations and optimize before production")
            return False
        
    except Exception as e:
        print(f"\n💥 Performance monitoring failed: {e}")
        db_logger.error(f"Performance monitoring error: {e}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)