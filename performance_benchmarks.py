#!/usr/bin/env python3
"""
Performance Benchmarks: Database vs CSV Operations

Comprehensive performance testing comparing database-primary operations
against traditional CSV operations across various scenarios.

Test Categories:
1. Single record reads
2. Multiple record reads  
3. Bulk operations
4. Filtered queries
5. Count operations
6. Stress testing
"""

import os
import sys
import time
import statistics
from datetime import datetime
from typing import List, Dict, Any, Tuple

# Set up environment
os.environ['DB_PASSWORD'] = 'migration_pass_2025'
sys.path.append('.')

from utils.database_primary_manager import DatabasePrimaryManager
from utils.csv_manager import CSVManager
from utils.database_logging import db_logger

class PerformanceBenchmark:
    """Comprehensive performance benchmark suite."""
    
    def __init__(self):
        """Initialize benchmark suite."""
        self.db_manager = DatabasePrimaryManager()
        self.csv_manager = CSVManager("outputs/output.csv")
        
        self.results = {
            'test_suites': [],
            'summary': {
                'total_tests': 0,
                'db_wins': 0,
                'csv_wins': 0,
                'ties': 0
            },
            'start_time': datetime.now()
        }
        
        # Test data
        self.test_record_ids = [1, 2, 3, 4, 5, 10, 25, 50, 100, 200]  # DB records
        self.bulk_sizes = [10, 50, 100]
        
    def time_operation(self, operation_func, iterations: int = 5) -> Tuple[float, Any]:
        """Time an operation over multiple iterations."""
        
        times = []
        result = None
        
        for _ in range(iterations):
            start_time = time.time()
            result = operation_func()
            end_time = time.time()
            times.append(end_time - start_time)
        
        avg_time = statistics.mean(times)
        return avg_time, result
    
    def test_single_record_reads(self) -> Dict[str, Any]:
        """Test single record read performance."""
        
        print("🔍 Test Suite 1: Single Record Reads")
        print("-" * 35)
        
        db_times = []
        csv_times = []
        
        for record_id in self.test_record_ids:
            # Test database read
            db_time, db_result = self.time_operation(
                lambda: self.db_manager.get_record_by_id(record_id)
            )
            db_times.append(db_time)
            
            # Test CSV read (only for records that exist in CSV)
            csv_time = None
            csv_result = None
            
            try:
                csv_df = self.csv_manager.read()
                if record_id in csv_df['row_id'].values:
                    csv_time, csv_result = self.time_operation(
                        lambda: csv_df[csv_df['row_id'] == record_id].iloc[0].to_dict()
                    )
                    csv_times.append(csv_time)
            except:
                pass  # CSV record might not exist
            
            print(f"   Record {record_id}: DB={db_time:.4f}s, CSV={csv_time:.4f}s" if csv_time else f"   Record {record_id}: DB={db_time:.4f}s, CSV=N/A")
        
        avg_db_time = statistics.mean(db_times)
        avg_csv_time = statistics.mean(csv_times) if csv_times else 0.001
        
        improvement = ((avg_csv_time - avg_db_time) / avg_csv_time * 100) if avg_csv_time > 0 else 0
        
        result = {
            'test_name': 'Single Record Reads',
            'db_avg_time': avg_db_time,
            'csv_avg_time': avg_csv_time,
            'performance_change': improvement,
            'winner': 'database' if avg_db_time < avg_csv_time else 'csv' if avg_csv_time > 0 else 'database',
            'records_tested': len(self.test_record_ids)
        }
        
        print(f"   📊 Average: DB={avg_db_time:.4f}s, CSV={avg_csv_time:.4f}s")
        print(f"   🏆 Winner: {result['winner'].upper()}, Performance change: {improvement:+.1f}%")
        
        return result
    
    def test_bulk_reads(self) -> Dict[str, Any]:
        """Test bulk read performance."""
        
        print(f"\n📚 Test Suite 2: Bulk Reads")
        print("-" * 25)
        
        db_times = []
        csv_times = []
        
        for bulk_size in self.bulk_sizes:
            test_ids = self.test_record_ids[:bulk_size]
            
            # Test database bulk read
            db_time, db_result = self.time_operation(
                lambda: [self.db_manager.get_record_by_id(rid) for rid in test_ids]
            )
            db_times.append(db_time)
            
            # Test CSV bulk read
            csv_time, csv_result = self.time_operation(
                lambda: self.csv_manager.read().head(bulk_size).to_dict('records')
            )
            csv_times.append(csv_time)
            
            throughput_db = bulk_size / db_time if db_time > 0 else 0
            throughput_csv = bulk_size / csv_time if csv_time > 0 else 0
            
            print(f"   {bulk_size} records: DB={db_time:.4f}s ({throughput_db:.1f} r/s), CSV={csv_time:.4f}s ({throughput_csv:.1f} r/s)")
        
        avg_db_time = statistics.mean(db_times)
        avg_csv_time = statistics.mean(csv_times)
        
        improvement = ((avg_csv_time - avg_db_time) / avg_csv_time * 100) if avg_csv_time > 0 else 0
        
        result = {
            'test_name': 'Bulk Reads',
            'db_avg_time': avg_db_time,
            'csv_avg_time': avg_csv_time,
            'performance_change': improvement,
            'winner': 'database' if avg_db_time < avg_csv_time else 'csv',
            'bulk_sizes_tested': self.bulk_sizes
        }
        
        print(f"   📊 Average: DB={avg_db_time:.4f}s, CSV={avg_csv_time:.4f}s")
        print(f"   🏆 Winner: {result['winner'].upper()}, Performance change: {improvement:+.1f}%")
        
        return result
    
    def test_count_operations(self) -> Dict[str, Any]:
        """Test count operation performance."""
        
        print(f"\n🔢 Test Suite 3: Count Operations")
        print("-" * 30)
        
        # Test database count
        db_time, db_count = self.time_operation(
            lambda: self.db_manager.count_records()
        )
        
        # Test CSV count
        csv_time, csv_count = self.time_operation(
            lambda: len(self.csv_manager.read())
        )
        
        improvement = ((csv_time - db_time) / csv_time * 100) if csv_time > 0 else 0
        
        result = {
            'test_name': 'Count Operations',
            'db_avg_time': db_time,
            'csv_avg_time': csv_time,
            'performance_change': improvement,
            'winner': 'database' if db_time < csv_time else 'csv',
            'db_count': db_count,
            'csv_count': csv_count
        }
        
        print(f"   Database count: {db_count} records in {db_time:.4f}s")
        print(f"   CSV count: {csv_count} records in {csv_time:.4f}s")
        print(f"   🏆 Winner: {result['winner'].upper()}, Performance change: {improvement:+.1f}%")
        
        return result
    
    def test_filtered_queries(self) -> Dict[str, Any]:
        """Test filtered query performance."""
        
        print(f"\n🔍 Test Suite 4: Filtered Queries")
        print("-" * 35)
        
        # Test database filtered query (processed=True)
        db_time, db_results = self.time_operation(
            lambda: self.db_manager.get_records_by_criteria(processed=True)
        )
        
        # Test CSV filtered query
        csv_time, csv_results = self.time_operation(
            lambda: self.csv_manager.read()[self.csv_manager.read()['processed'] == True].to_dict('records')
        )
        
        improvement = ((csv_time - db_time) / csv_time * 100) if csv_time > 0 else 0
        
        result = {
            'test_name': 'Filtered Queries',
            'db_avg_time': db_time,
            'csv_avg_time': csv_time,
            'performance_change': improvement,
            'winner': 'database' if db_time < csv_time else 'csv',
            'db_results_count': len(db_results),
            'csv_results_count': len(csv_results)
        }
        
        print(f"   Database filter: {len(db_results)} records in {db_time:.4f}s")
        print(f"   CSV filter: {len(csv_results)} records in {csv_time:.4f}s")
        print(f"   🏆 Winner: {result['winner'].upper()}, Performance change: {improvement:+.1f}%")
        
        return result
    
    def test_full_data_load(self) -> Dict[str, Any]:
        """Test full dataset loading performance."""
        
        print(f"\n📊 Test Suite 5: Full Data Load")
        print("-" * 30)
        
        # Test database full load
        db_time, db_df = self.time_operation(
            lambda: self.db_manager.read_dataframe()
        )
        
        # Test CSV full load
        csv_time, csv_df = self.time_operation(
            lambda: self.csv_manager.read()
        )
        
        improvement = ((csv_time - db_time) / csv_time * 100) if csv_time > 0 else 0
        
        result = {
            'test_name': 'Full Data Load',
            'db_avg_time': db_time,
            'csv_avg_time': csv_time,
            'performance_change': improvement,
            'winner': 'database' if db_time < csv_time else 'csv',
            'db_records': len(db_df),
            'csv_records': len(csv_df)
        }
        
        throughput_db = len(db_df) / db_time if db_time > 0 else 0
        throughput_csv = len(csv_df) / csv_time if csv_time > 0 else 0
        
        print(f"   Database load: {len(db_df)} records in {db_time:.4f}s ({throughput_db:.1f} r/s)")
        print(f"   CSV load: {len(csv_df)} records in {csv_time:.4f}s ({throughput_csv:.1f} r/s)")
        print(f"   🏆 Winner: {result['winner'].upper()}, Performance change: {improvement:+.1f}%")
        
        return result
    
    def test_stress_operations(self) -> Dict[str, Any]:
        """Test system under stress (repeated operations)."""
        
        print(f"\n⚡ Test Suite 6: Stress Testing")
        print("-" * 30)
        
        stress_iterations = 50
        test_id = 1  # Use a known record
        
        # Stress test database reads
        db_start = time.time()
        for _ in range(stress_iterations):
            self.db_manager.get_record_by_id(test_id)
        db_total_time = time.time() - db_start
        
        # Stress test CSV reads
        csv_df = self.csv_manager.read()  # Load once
        csv_start = time.time()
        for _ in range(stress_iterations):
            if test_id in csv_df['row_id'].values:
                csv_df[csv_df['row_id'] == test_id].iloc[0].to_dict()
        csv_total_time = time.time() - csv_start
        
        improvement = ((csv_total_time - db_total_time) / csv_total_time * 100) if csv_total_time > 0 else 0
        
        result = {
            'test_name': 'Stress Testing',
            'db_avg_time': db_total_time,
            'csv_avg_time': csv_total_time,
            'performance_change': improvement,
            'winner': 'database' if db_total_time < csv_total_time else 'csv',
            'iterations': stress_iterations,
            'db_ops_per_sec': stress_iterations / db_total_time if db_total_time > 0 else 0,
            'csv_ops_per_sec': stress_iterations / csv_total_time if csv_total_time > 0 else 0
        }
        
        print(f"   Database stress: {stress_iterations} ops in {db_total_time:.4f}s ({result['db_ops_per_sec']:.1f} ops/s)")
        print(f"   CSV stress: {stress_iterations} ops in {csv_total_time:.4f}s ({result['csv_ops_per_sec']:.1f} ops/s)")
        print(f"   🏆 Winner: {result['winner'].upper()}, Performance change: {improvement:+.1f}%")
        
        return result
    
    def run_all_benchmarks(self) -> Dict[str, Any]:
        """Run all performance benchmarks."""
        
        print("⚡ Performance Benchmarks: Database vs CSV")
        print("=" * 45)
        
        # Run all test suites
        test_suites = [
            self.test_single_record_reads(),
            self.test_bulk_reads(),
            self.test_count_operations(),
            self.test_filtered_queries(),
            self.test_full_data_load(),
            self.test_stress_operations()
        ]
        
        self.results['test_suites'] = test_suites
        
        # Calculate summary statistics
        for suite in test_suites:
            self.results['summary']['total_tests'] += 1
            if suite['winner'] == 'database':
                self.results['summary']['db_wins'] += 1
            elif suite['winner'] == 'csv':
                self.results['summary']['csv_wins'] += 1
            else:
                self.results['summary']['ties'] += 1
        
        # Generate final report
        self.generate_final_report()
        
        return self.results
    
    def generate_final_report(self):
        """Generate comprehensive performance report."""
        
        end_time = datetime.now()
        duration = (end_time - self.results['start_time']).total_seconds()
        
        print(f"\n📊 Performance Benchmark Results")
        print("=" * 35)
        
        summary = self.results['summary']
        print(f"Total test suites: {summary['total_tests']}")
        print(f"Database wins: {summary['db_wins']}")
        print(f"CSV wins: {summary['csv_wins']}")
        print(f"Ties: {summary['ties']}")
        print(f"Database win rate: {(summary['db_wins']/summary['total_tests']*100):.1f}%")
        print(f"Benchmark duration: {duration:.1f}s")
        
        print(f"\n📈 Performance Summary by Test:")
        for suite in self.results['test_suites']:
            winner_icon = "🥇" if suite['winner'] == 'database' else "🥈"
            print(f"   {winner_icon} {suite['test_name']}: {suite['winner'].upper()} ({suite['performance_change']:+.1f}%)")
        
        # Overall recommendation
        print(f"\n💡 Overall Assessment:")
        if summary['db_wins'] >= summary['csv_wins']:
            print(f"✅ Database-primary operations show superior performance")
            print(f"✅ Recommended to proceed with database-primary mode")
        else:
            print(f"⚠️ CSV operations show better performance in some areas")
            print(f"⚠️ Review database configuration and optimization")
        
        # Usage statistics
        db_stats = self.db_manager.get_usage_statistics()
        print(f"\n📊 Database Manager Statistics:")
        print(f"   Database operations: {db_stats['database_reads']}")
        print(f"   CSV fallbacks: {db_stats['csv_fallbacks']}")
        print(f"   Database success rate: {db_stats['database_success_rate']:.1f}%")
        print(f"   Fallback rate: {db_stats['fallback_rate']:.1f}%")

def main():
    """Execute performance benchmarks."""
    
    benchmark = PerformanceBenchmark()
    results = benchmark.run_all_benchmarks()
    
    # Return success based on overall performance
    db_wins = results['summary']['db_wins']
    total_tests = results['summary']['total_tests']
    success_rate = (db_wins / total_tests * 100) if total_tests > 0 else 0
    
    return success_rate >= 50  # At least 50% database wins

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)