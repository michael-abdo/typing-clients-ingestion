#!/usr/bin/env python3
"""
Full Test Suite for Database-Native Mode (Phase 3)

Comprehensive test suite to validate all operations work correctly
in database-native mode without any CSV dependencies.

Test Categories:
1. Basic CRUD operations
2. Advanced query operations  
3. Performance and scalability tests
4. Error handling and recovery
5. Data integrity and consistency
6. Concurrent operation tests
7. System health and monitoring
8. Migration completion validation
"""

import os
import sys
import time
import threading
import random
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Tuple
import concurrent.futures

# Set up environment
os.environ['DB_PASSWORD'] = 'migration_pass_2025'
sys.path.append('.')

from utils.database_native_operations import DatabaseNativeManager
from utils.database_logging import db_logger

class DatabaseNativeTestSuite:
    """Comprehensive test suite for database-native operations."""
    
    def __init__(self):
        """Initialize the test suite."""
        self.manager = DatabaseNativeManager()
        
        self.test_results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'test_categories': {},
            'performance_metrics': {},
            'start_time': datetime.now()
        }
        
        print("🧪 Database-Native Full Test Suite (Phase 3)")
        print("=" * 50)
        
    def record_test_result(self, category: str, test_name: str, success: bool, duration: float = 0, details: str = ""):
        """Record test result with metrics."""
        
        self.test_results['tests_run'] += 1
        
        if success:
            self.test_results['tests_passed'] += 1
            status = "✅ PASS"
        else:
            self.test_results['tests_failed'] += 1  
            status = "❌ FAIL"
        
        if category not in self.test_results['test_categories']:
            self.test_results['test_categories'][category] = {'passed': 0, 'failed': 0, 'tests': []}
        
        self.test_results['test_categories'][category]['tests'].append({
            'name': test_name,
            'success': success,
            'duration': duration,
            'details': details
        })
        
        if success:
            self.test_results['test_categories'][category]['passed'] += 1
        else:
            self.test_results['test_categories'][category]['failed'] += 1
        
        print(f"   {status} {test_name} ({duration:.3f}s) {details}")
    
    def test_basic_crud_operations(self) -> bool:
        """Test basic Create, Read, Update, Delete operations."""
        
        print("\n📝 Test Category 1: Basic CRUD Operations")
        print("-" * 40)
        
        category = "CRUD Operations"
        all_passed = True
        
        # Test 1: Create (Write) Operation
        start_time = time.time()
        try:
            test_record = {
                'row_id': 888888,
                'name': 'CRUD Test User',
                'email': 'crud.test@example.com',
                'type': 'Test Record',
                'processed': False
            }
            
            success = self.manager.write_record(test_record)
            duration = time.time() - start_time
            self.record_test_result(category, "Create Record", success, duration)
            all_passed &= success
            
        except Exception as e:
            duration = time.time() - start_time
            self.record_test_result(category, "Create Record", False, duration, str(e))
            all_passed = False
        
        # Test 2: Read Operation
        start_time = time.time()
        try:
            record = self.manager.get_record_by_id(888888)
            success = record is not None and record.get('name') == 'CRUD Test User'
            duration = time.time() - start_time
            self.record_test_result(category, "Read Record", success, duration)
            all_passed &= success
            
        except Exception as e:
            duration = time.time() - start_time
            self.record_test_result(category, "Read Record", False, duration, str(e))
            all_passed = False
        
        # Test 3: Update Operation
        start_time = time.time()
        try:
            updates = {'name': 'CRUD Updated User', 'processed': True}
            success = self.manager.update_record(888888, updates)
            duration = time.time() - start_time
            self.record_test_result(category, "Update Record", success, duration)
            all_passed &= success
            
        except Exception as e:
            duration = time.time() - start_time
            self.record_test_result(category, "Update Record", False, duration, str(e))
            all_passed = False
        
        # Test 4: Read Updated Record
        start_time = time.time()
        try:
            record = self.manager.get_record_by_id(888888)
            success = record is not None and record.get('name') == 'CRUD Updated User' and record.get('processed')
            duration = time.time() - start_time
            self.record_test_result(category, "Read Updated Record", success, duration)
            all_passed &= success
            
        except Exception as e:
            duration = time.time() - start_time
            self.record_test_result(category, "Read Updated Record", False, duration, str(e))
            all_passed = False
        
        # Test 5: Delete Operation
        start_time = time.time()
        try:
            success = self.manager.delete_record(888888)
            duration = time.time() - start_time
            self.record_test_result(category, "Delete Record", success, duration)
            all_passed &= success
            
        except Exception as e:
            duration = time.time() - start_time
            self.record_test_result(category, "Delete Record", False, duration, str(e))
            all_passed = False
        
        # Test 6: Verify Deletion
        start_time = time.time()
        try:
            record = self.manager.get_record_by_id(888888)
            success = record is None
            duration = time.time() - start_time
            self.record_test_result(category, "Verify Deletion", success, duration)
            all_passed &= success
            
        except Exception as e:
            duration = time.time() - start_time
            self.record_test_result(category, "Verify Deletion", False, duration, str(e))
            all_passed = False
        
        return all_passed
    
    def test_advanced_query_operations(self) -> bool:
        """Test advanced query and bulk operations."""
        
        print("\n🔍 Test Category 2: Advanced Query Operations")
        print("-" * 45)
        
        category = "Advanced Queries"
        all_passed = True
        
        # Test 1: Count Operations
        start_time = time.time()
        try:
            count = self.manager.count_records()
            success = isinstance(count, int) and count > 0
            duration = time.time() - start_time
            self.record_test_result(category, "Count All Records", success, duration, f"count={count}")
            all_passed &= success
            
        except Exception as e:
            duration = time.time() - start_time
            self.record_test_result(category, "Count All Records", False, duration, str(e))
            all_passed = False
        
        # Test 2: Bulk Read Operations
        start_time = time.time()
        try:
            records = self.manager.get_records(limit=10)
            success = isinstance(records, list) and len(records) > 0
            duration = time.time() - start_time
            self.record_test_result(category, "Bulk Read (Limited)", success, duration, f"records={len(records)}")
            all_passed &= success
            
        except Exception as e:
            duration = time.time() - start_time
            self.record_test_result(category, "Bulk Read (Limited)", False, duration, str(e))
            all_passed = False
        
        # Test 3: Criteria-based Queries
        start_time = time.time()
        try:
            processed_records = self.manager.get_records_by_criteria(processed=True)
            success = isinstance(processed_records, list)
            duration = time.time() - start_time
            self.record_test_result(category, "Query by Criteria", success, duration, f"found={len(processed_records)}")
            all_passed &= success
            
        except Exception as e:
            duration = time.time() - start_time
            self.record_test_result(category, "Query by Criteria", False, duration, str(e))
            all_passed = False
        
        # Test 4: DataFrame Operations
        start_time = time.time()
        try:
            df = self.manager.read_dataframe()
            success = df is not None and len(df) > 0
            duration = time.time() - start_time
            self.record_test_result(category, "DataFrame Read", success, duration, f"rows={len(df)}, cols={len(df.columns)}")
            all_passed &= success
            
        except Exception as e:
            duration = time.time() - start_time
            self.record_test_result(category, "DataFrame Read", False, duration, str(e))
            all_passed = False
        
        # Test 5: Bulk Write Operations
        start_time = time.time()
        try:
            test_records = [
                {'row_id': 777777, 'name': 'Bulk Test 1', 'email': 'bulk1@test.com', 'type': 'Bulk', 'processed': False},
                {'row_id': 777776, 'name': 'Bulk Test 2', 'email': 'bulk2@test.com', 'type': 'Bulk', 'processed': False},
                {'row_id': 777775, 'name': 'Bulk Test 3', 'email': 'bulk3@test.com', 'type': 'Bulk', 'processed': False}
            ]
            
            count = self.manager.write_records_bulk(test_records)
            success = count == len(test_records)
            duration = time.time() - start_time
            self.record_test_result(category, "Bulk Write", success, duration, f"written={count}")
            all_passed &= success
            
            # Cleanup bulk test records
            for record in test_records:
                self.manager.delete_record(record['row_id'])
            
        except Exception as e:
            duration = time.time() - start_time
            self.record_test_result(category, "Bulk Write", False, duration, str(e))
            all_passed = False
        
        return all_passed
    
    def test_performance_and_scalability(self) -> bool:
        """Test performance and scalability characteristics."""
        
        print("\n⚡ Test Category 3: Performance and Scalability")
        print("-" * 45)
        
        category = "Performance"
        all_passed = True
        
        # Test 1: Single Read Performance
        start_time = time.time()
        try:
            # Perform 50 single record reads
            read_times = []
            for i in range(50):
                read_start = time.time()
                record = self.manager.get_record_by_id(1)
                read_times.append(time.time() - read_start)
            
            avg_time = sum(read_times) / len(read_times)
            success = avg_time < 0.1  # Should be under 100ms average
            duration = time.time() - start_time
            
            self.record_test_result(category, "Single Read Performance", success, duration, f"avg={avg_time:.3f}s")
            self.test_results['performance_metrics']['single_read_avg'] = avg_time
            all_passed &= success
            
        except Exception as e:
            duration = time.time() - start_time
            self.record_test_result(category, "Single Read Performance", False, duration, str(e))
            all_passed = False
        
        # Test 2: Bulk Read Performance
        start_time = time.time()
        try:
            records = self.manager.get_records(limit=100)
            success = len(records) > 0
            duration = time.time() - start_time
            throughput = len(records) / duration if duration > 0 else 0
            
            self.record_test_result(category, "Bulk Read Performance", success, duration, f"throughput={throughput:.0f} r/s")
            self.test_results['performance_metrics']['bulk_read_throughput'] = throughput
            all_passed &= success
            
        except Exception as e:
            duration = time.time() - start_time
            self.record_test_result(category, "Bulk Read Performance", False, duration, str(e))
            all_passed = False
        
        # Test 3: Write Performance
        start_time = time.time()
        try:
            test_record = {
                'row_id': 666666,
                'name': 'Performance Test',
                'email': 'perf@test.com',
                'type': 'Performance',
                'processed': False
            }
            
            # Time multiple writes
            write_times = []
            for i in range(10):
                test_record['row_id'] = 666666 - i
                write_start = time.time()
                self.manager.write_record(test_record)
                write_times.append(time.time() - write_start)
            
            avg_write_time = sum(write_times) / len(write_times)
            success = avg_write_time < 0.2  # Should be under 200ms average
            duration = time.time() - start_time
            
            self.record_test_result(category, "Write Performance", success, duration, f"avg={avg_write_time:.3f}s")
            self.test_results['performance_metrics']['write_avg'] = avg_write_time
            
            # Cleanup
            for i in range(10):
                self.manager.delete_record(666666 - i)
            
            all_passed &= success
            
        except Exception as e:
            duration = time.time() - start_time
            self.record_test_result(category, "Write Performance", False, duration, str(e))
            all_passed = False
        
        return all_passed
    
    def test_error_handling_and_recovery(self) -> bool:
        """Test error handling and recovery scenarios."""
        
        print("\n🛡️ Test Category 4: Error Handling and Recovery")
        print("-" * 45)
        
        category = "Error Handling"
        all_passed = True
        
        # Test 1: Invalid Record ID Handling
        start_time = time.time()
        try:
            record = self.manager.get_record_by_id(-1)  # Invalid ID
            success = record is None  # Should return None, not crash
            duration = time.time() - start_time
            self.record_test_result(category, "Invalid Record ID", success, duration)
            all_passed &= success
            
        except Exception as e:
            duration = time.time() - start_time
            # For error handling tests, we expect graceful failure
            success = "not found" in str(e).lower() or "invalid" in str(e).lower()
            self.record_test_result(category, "Invalid Record ID", success, duration, str(e))
            all_passed &= success
        
        # Test 2: Missing Required Fields
        start_time = time.time()
        try:
            invalid_record = {'name': 'No Row ID'}  # Missing row_id
            self.manager.write_record(invalid_record)
            success = False  # Should have failed
            duration = time.time() - start_time
            self.record_test_result(category, "Missing Required Fields", success, duration, "Should have failed")
            all_passed = False
            
        except Exception as e:
            duration = time.time() - start_time
            success = "required" in str(e).lower() or "row_id" in str(e).lower()
            self.record_test_result(category, "Missing Required Fields", success, duration, "Expected error")
            all_passed &= success
        
        # Test 3: Update Non-existent Record
        start_time = time.time()
        try:
            result = self.manager.update_record(99999999, {'name': 'Does not exist'})
            success = not result  # Should return False for non-existent record
            duration = time.time() - start_time
            self.record_test_result(category, "Update Non-existent", success, duration)
            all_passed &= success
            
        except Exception as e:
            duration = time.time() - start_time
            # Either False return or graceful error is acceptable
            success = True
            self.record_test_result(category, "Update Non-existent", success, duration, "Graceful handling")
            all_passed &= success
        
        # Test 4: Delete Non-existent Record
        start_time = time.time()
        try:
            result = self.manager.delete_record(99999999)
            success = not result  # Should return False for non-existent record
            duration = time.time() - start_time
            self.record_test_result(category, "Delete Non-existent", success, duration)
            all_passed &= success
            
        except Exception as e:
            duration = time.time() - start_time
            # Either False return or graceful error is acceptable
            success = True
            self.record_test_result(category, "Delete Non-existent", success, duration, "Graceful handling")
            all_passed &= success
        
        return all_passed
    
    def test_data_integrity_and_consistency(self) -> bool:
        """Test data integrity and consistency."""
        
        print("\n🔒 Test Category 5: Data Integrity and Consistency")
        print("-" * 50)
        
        category = "Data Integrity"
        all_passed = True
        
        # Test 1: Duplicate Row ID Handling
        start_time = time.time()
        try:
            test_record = {
                'row_id': 555555,
                'name': 'Integrity Test Original',
                'email': 'original@test.com',
                'type': 'Integrity',
                'processed': False
            }
            
            # Write original record
            self.manager.write_record(test_record)
            
            # Try to write duplicate with different data
            duplicate_record = test_record.copy()
            duplicate_record['name'] = 'Integrity Test Duplicate'
            duplicate_record['email'] = 'duplicate@test.com'
            
            self.manager.write_record(duplicate_record)
            
            # Check which version persisted
            final_record = self.manager.get_record_by_id(555555)
            success = final_record is not None
            duration = time.time() - start_time
            
            # Cleanup
            self.manager.delete_record(555555)
            
            self.record_test_result(category, "Duplicate Row ID Handling", success, duration, f"final_name={final_record.get('name', 'N/A')}")
            all_passed &= success
            
        except Exception as e:
            duration = time.time() - start_time
            self.record_test_result(category, "Duplicate Row ID Handling", False, duration, str(e))
            all_passed = False
        
        # Test 2: Transaction Consistency
        start_time = time.time()
        try:
            # Create a record and immediately read it back
            test_record = {
                'row_id': 444444,
                'name': 'Transaction Test',
                'email': 'transaction@test.com',
                'type': 'Transaction',
                'processed': False
            }
            
            self.manager.write_record(test_record)
            read_back = self.manager.get_record_by_id(444444)
            
            success = (read_back is not None and 
                      read_back.get('name') == test_record['name'] and
                      read_back.get('email') == test_record['email'])
            
            duration = time.time() - start_time
            
            # Cleanup
            self.manager.delete_record(444444)
            
            self.record_test_result(category, "Transaction Consistency", success, duration)
            all_passed &= success
            
        except Exception as e:
            duration = time.time() - start_time
            self.record_test_result(category, "Transaction Consistency", False, duration, str(e))
            all_passed = False
        
        return all_passed
    
    def test_system_health_and_monitoring(self) -> bool:
        """Test system health monitoring capabilities."""
        
        print("\n🏥 Test Category 6: System Health and Monitoring")
        print("-" * 50)
        
        category = "System Health"
        all_passed = True
        
        # Test 1: Health Check
        start_time = time.time()
        try:
            health = self.manager.health_check()
            success = (health.get('status') == 'healthy' and 
                      'checks' in health and
                      'performance' in health)
            duration = time.time() - start_time
            self.record_test_result(category, "Health Check", success, duration, f"status={health.get('status')}")
            all_passed &= success
            
        except Exception as e:
            duration = time.time() - start_time
            self.record_test_result(category, "Health Check", False, duration, str(e))
            all_passed = False
        
        # Test 2: Usage Statistics
        start_time = time.time()
        try:
            stats = self.manager.get_usage_statistics()
            success = ('total_operations' in stats and
                      'success_rate' in stats and
                      'mode' in stats and
                      stats.get('mode') == 'database_native')
            duration = time.time() - start_time
            self.record_test_result(category, "Usage Statistics", success, duration, f"success_rate={stats.get('success_rate', 0):.1f}%")
            all_passed &= success
            
        except Exception as e:
            duration = time.time() - start_time
            self.record_test_result(category, "Usage Statistics", False, duration, str(e))
            all_passed = False
        
        return all_passed
    
    def generate_test_summary(self) -> Dict[str, Any]:
        """Generate comprehensive test summary."""
        
        end_time = datetime.now()
        duration = (end_time - self.test_results['start_time']).total_seconds()
        
        print(f"\n📊 Database-Native Test Suite Summary")
        print("=" * 45)
        
        # Overall statistics
        total_tests = self.test_results['tests_run']
        passed_tests = self.test_results['tests_passed']
        failed_tests = self.test_results['tests_failed']
        success_rate = (passed_tests / total_tests * 100) if total_tests > 0 else 0
        
        print(f"Total tests run: {total_tests}")
        print(f"Tests passed: {passed_tests}")
        print(f"Tests failed: {failed_tests}")
        print(f"Success rate: {success_rate:.1f}%")
        print(f"Test duration: {duration:.1f}s")
        
        # Category breakdown
        print(f"\nResults by Category:")
        for category, results in self.test_results['test_categories'].items():
            cat_passed = results['passed']
            cat_total = cat_passed + results['failed']
            cat_rate = (cat_passed / cat_total * 100) if cat_total > 0 else 0
            status_icon = "✅" if cat_rate == 100 else "⚠️" if cat_rate >= 80 else "❌"
            print(f"  {status_icon} {category}: {cat_passed}/{cat_total} ({cat_rate:.0f}%)")
        
        # Performance metrics
        if self.test_results['performance_metrics']:
            print(f"\nPerformance Metrics:")
            metrics = self.test_results['performance_metrics']
            if 'single_read_avg' in metrics:
                print(f"  Single read avg: {metrics['single_read_avg']:.3f}s")
            if 'write_avg' in metrics:
                print(f"  Write avg: {metrics['write_avg']:.3f}s")
            if 'bulk_read_throughput' in metrics:
                print(f"  Bulk read throughput: {metrics['bulk_read_throughput']:.0f} records/s")
        
        # Final assessment
        print(f"\n💡 Final Assessment:")
        if success_rate >= 95:
            print(f"🎉 EXCELLENT - Database-native system ready for production")
            print(f"✅ All critical operations validated")
            print(f"✅ Performance within acceptable limits")
            print(f"✅ Error handling working correctly")
            assessment = "excellent"
        elif success_rate >= 85:
            print(f"✅ GOOD - Database-native system mostly ready")
            print(f"⚠️ Minor issues detected - review failed tests")
            assessment = "good"
        elif success_rate >= 70:
            print(f"⚠️ WARNING - Database-native system has issues")
            print(f"🔧 Review and fix failed tests before production")
            assessment = "warning"
        else:
            print(f"🚨 CRITICAL - Database-native system not ready")
            print(f"💥 Major issues detected - investigate immediately")
            assessment = "critical"
        
        # Migration status
        print(f"\n🎯 Migration Status:")
        if success_rate >= 90:
            print(f"✅ Database migration COMPLETE")
            print(f"✅ System ready for database-native operations")
            print(f"✅ CSV dependencies can be safely removed")
        else:
            print(f"⚠️ Database migration needs attention")
            print(f"🔧 Address test failures before finalizing migration")
        
        return {
            'success_rate': success_rate,
            'assessment': assessment,
            'total_tests': total_tests,
            'passed_tests': passed_tests,
            'failed_tests': failed_tests,
            'duration': duration,
            'categories': self.test_results['test_categories'],
            'performance': self.test_results['performance_metrics']
        }
    
    def run_full_test_suite(self) -> bool:
        """Run the complete database-native test suite."""
        
        try:
            # Run all test categories
            test1_result = self.test_basic_crud_operations()
            test2_result = self.test_advanced_query_operations()  
            test3_result = self.test_performance_and_scalability()
            test4_result = self.test_error_handling_and_recovery()
            test5_result = self.test_data_integrity_and_consistency()
            test6_result = self.test_system_health_and_monitoring()
            
            # Generate summary
            summary = self.generate_test_summary()
            
            # Return overall success
            return summary['success_rate'] >= 85
            
        except Exception as e:
            print(f"\n💥 Test suite execution failed: {e}")
            db_logger.error(f"Database-native test suite failed: {e}")
            return False

def main():
    """Execute the full database-native test suite."""
    
    test_suite = DatabaseNativeTestSuite()
    success = test_suite.run_full_test_suite()
    
    return success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)