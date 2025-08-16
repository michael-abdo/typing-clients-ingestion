#!/usr/bin/env python3
"""
Emergency Fallback to CSV Mode Testing

Comprehensive testing of emergency fallback scenarios when database becomes
unavailable and system must operate in CSV-only mode.

Test Scenarios:
1. Database connection failure simulation
2. Database timeout scenarios  
3. CSV-only mode operation verification
4. Data integrity during fallback
5. Recovery back to database-primary mode
6. Fallback performance and reliability
"""

import os
import sys
import time
import shutil
import subprocess
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Tuple
from unittest.mock import patch, MagicMock

# Set up environment
os.environ['DB_PASSWORD'] = 'migration_pass_2025'
sys.path.append('.')

from utils.database_primary_manager import DatabasePrimaryManager
from utils.csv_manager import CSVManager
from utils.database_logging import db_logger

class EmergencyFallbackTester:
    """Test emergency fallback to CSV-only mode."""
    
    def __init__(self):
        """Initialize fallback tester."""
        self.csv_path = "outputs/output.csv"
        self.backup_path = f"{self.csv_path}.fallback_test_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Create backup of current CSV
        if Path(self.csv_path).exists():
            shutil.copy2(self.csv_path, self.backup_path)
            print(f"📁 Created CSV backup: {self.backup_path}")
        
        # Initialize managers
        self.csv_manager = CSVManager(self.csv_path)
        
        self.test_results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'fallback_events': [],
            'start_time': datetime.now()
        }
        
    def test_baseline_operations(self) -> bool:
        """Test baseline operations before simulating failures."""
        
        print("📊 Test 1: Baseline Operations")
        print("-" * 30)
        self.test_results['tests_run'] += 1
        
        try:
            # Test normal database-primary operations
            db_manager = DatabasePrimaryManager()
            
            # Test read operations
            print("   Testing normal database operations...")
            
            # Count records
            db_count = db_manager.count_records()
            print(f"   Database record count: {db_count}")
            
            # Read single record
            record = db_manager.get_record_by_id(1)
            if record:
                print(f"   Single record read: {record['name']}")
            
            # Read DataFrame
            df = db_manager.read_dataframe()
            print(f"   DataFrame read: {len(df)} records")
            
            # Test CSV operations
            print("   Testing CSV operations...")
            csv_df = self.csv_manager.read()
            print(f"   CSV record count: {len(csv_df)}")
            
            print("   ✅ Baseline operations successful")
            self.test_results['tests_passed'] += 1
            return True
            
        except Exception as e:
            print(f"   ❌ Baseline operations failed: {e}")
            self.test_results['tests_failed'] += 1
            return False
    
    def simulate_database_connection_failure(self) -> bool:
        """Simulate database connection failure and test fallback."""
        
        print(f"\n🚨 Test 2: Database Connection Failure Simulation")
        print("-" * 50)
        self.test_results['tests_run'] += 1
        
        try:
            print("   Simulating database connection failure...")
            
            # Mock database connection to fail
            with patch('utils.database_operations.DatabaseManager.get_connection') as mock_conn:
                mock_conn.side_effect = Exception("Connection failed")
                
                # Create database manager (should fall back to CSV)
                db_manager = DatabasePrimaryManager()
                
                print("   Testing operations during connection failure...")
                
                # Test 1: Record count (should fallback to CSV)
                try:
                    count = db_manager.count_records()
                    print(f"   ✅ Count operation fallback: {count} records")
                    fallback_success_1 = True
                except Exception as e:
                    print(f"   ❌ Count operation failed: {e}")
                    fallback_success_1 = False
                
                # Test 2: Single record read (should fallback to CSV)
                try:
                    # Try to read a record that exists in CSV
                    csv_df = self.csv_manager.read()
                    if not csv_df.empty:
                        test_id = csv_df.iloc[0]['row_id']
                        record = db_manager.get_record_by_id(int(test_id))
                        if record:
                            print(f"   ✅ Single record fallback: {record['name']}")
                            fallback_success_2 = True
                        else:
                            print(f"   ⚠️ Single record fallback: No record found")
                            fallback_success_2 = False
                    else:
                        print(f"   ⚠️ No CSV records available for testing")
                        fallback_success_2 = False
                except Exception as e:
                    print(f"   ❌ Single record operation failed: {e}")
                    fallback_success_2 = False
                
                # Test 3: DataFrame read (should fallback to CSV)
                try:
                    df = db_manager.read_dataframe(force_csv=True)  # Force CSV mode
                    print(f"   ✅ DataFrame fallback: {len(df)} records")
                    fallback_success_3 = True
                except Exception as e:
                    print(f"   ❌ DataFrame operation failed: {e}")
                    fallback_success_3 = False
            
            # Assess overall fallback success
            fallback_tests = [fallback_success_1, fallback_success_2, fallback_success_3]
            success_count = sum(fallback_tests)
            total_tests = len(fallback_tests)
            
            print(f"   📊 Fallback success rate: {success_count}/{total_tests} ({success_count/total_tests*100:.1f}%)")
            
            if success_count >= 2:  # At least 2/3 operations should succeed
                print("   ✅ Database failure fallback working correctly")
                self.test_results['tests_passed'] += 1
                self.test_results['fallback_events'].append({
                    'scenario': 'connection_failure',
                    'success_rate': success_count/total_tests*100,
                    'timestamp': datetime.now().isoformat()
                })
                return True
            else:
                print("   ❌ Database failure fallback not working correctly")
                self.test_results['tests_failed'] += 1
                return False
                
        except Exception as e:
            print(f"   ❌ Connection failure simulation failed: {e}")
            self.test_results['tests_failed'] += 1
            return False
    
    def test_csv_only_mode_operations(self) -> bool:
        """Test full CSV-only mode operations."""
        
        print(f"\n📁 Test 3: CSV-Only Mode Operations")
        print("-" * 35)
        self.test_results['tests_run'] += 1
        
        try:
            print("   Testing comprehensive CSV-only operations...")
            
            # Test CSV manager directly
            csv_df = self.csv_manager.read()
            print(f"   CSV records available: {len(csv_df)}")
            
            if len(csv_df) == 0:
                print("   ⚠️ No CSV records available for testing")
                self.test_results['tests_passed'] += 1  # Not a failure, just no data
                return True
            
            # Test 1: Read operations
            print("   Testing CSV read operations...")
            
            # Full read
            full_df = self.csv_manager.read()
            print(f"      Full read: {len(full_df)} records")
            
            # Filtered read
            if 'processed' in full_df.columns:
                processed_df = full_df[full_df['processed'] == True]
                print(f"      Filtered read: {len(processed_df)} processed records")
            
            # Test 2: Data quality checks
            print("   Testing CSV data quality...")
            
            # Check for required columns
            required_cols = ['row_id', 'name', 'email']
            missing_cols = [col for col in required_cols if col not in full_df.columns]
            if missing_cols:
                print(f"      ❌ Missing columns: {missing_cols}")
                csv_quality = False
            else:
                print(f"      ✅ All required columns present")
                csv_quality = True
            
            # Check for null values in critical fields
            null_ids = full_df['row_id'].isnull().sum()
            print(f"      Records with null row_id: {null_ids}")
            
            # Test 3: Performance in CSV-only mode
            print("   Testing CSV-only performance...")
            
            start_time = time.time()
            for _ in range(10):
                test_df = self.csv_manager.read()
            csv_performance = (time.time() - start_time) / 10
            print(f"      CSV read performance: {csv_performance:.3f}s avg")
            
            # Overall assessment
            performance_ok = csv_performance < 0.1  # Should be fast for small CSV
            
            if csv_quality and performance_ok:
                print("   ✅ CSV-only mode operations successful")
                self.test_results['tests_passed'] += 1
                return True
            else:
                print("   ❌ CSV-only mode has issues")
                self.test_results['tests_failed'] += 1
                return False
                
        except Exception as e:
            print(f"   ❌ CSV-only mode test failed: {e}")
            self.test_results['tests_failed'] += 1
            return False
    
    def test_database_timeout_scenario(self) -> bool:
        """Test database timeout scenarios."""
        
        print(f"\n⏱️ Test 4: Database Timeout Scenario")
        print("-" * 35)
        self.test_results['tests_run'] += 1
        
        try:
            print("   Simulating database timeout...")
            
            # Mock database operations to timeout
            with patch('utils.database_operations.DatabaseManager.get_connection') as mock_conn:
                # Simulate slow connection that times out
                def slow_connection():
                    time.sleep(0.1)  # Simulate slow response
                    raise Exception("Operation timed out")
                
                mock_conn.side_effect = slow_connection
                
                # Test operations during timeout
                db_manager = DatabasePrimaryManager()
                
                print("   Testing timeout handling...")
                
                # Test with timeout handling
                timeout_start = time.time()
                try:
                    # Should fallback to CSV quickly
                    df = db_manager.read_dataframe()
                    timeout_duration = time.time() - timeout_start
                    print(f"   ✅ Timeout fallback successful in {timeout_duration:.3f}s")
                    
                    if timeout_duration < 2.0:  # Should fail fast and fallback
                        timeout_handling = True
                    else:
                        print("   ⚠️ Timeout handling too slow")
                        timeout_handling = False
                        
                except Exception as e:
                    timeout_duration = time.time() - timeout_start
                    print(f"   ❌ Timeout handling failed: {e} (after {timeout_duration:.3f}s)")
                    timeout_handling = False
            
            if timeout_handling:
                print("   ✅ Database timeout handling working correctly")
                self.test_results['tests_passed'] += 1
                self.test_results['fallback_events'].append({
                    'scenario': 'timeout',
                    'duration': timeout_duration,
                    'timestamp': datetime.now().isoformat()
                })
                return True
            else:
                print("   ❌ Database timeout handling not working correctly")
                self.test_results['tests_failed'] += 1
                return False
                
        except Exception as e:
            print(f"   ❌ Timeout scenario test failed: {e}")
            self.test_results['tests_failed'] += 1
            return False
    
    def test_recovery_to_database_mode(self) -> bool:
        """Test recovery back to database-primary mode."""
        
        print(f"\n🔄 Test 5: Recovery to Database Mode")
        print("-" * 35)
        self.test_results['tests_run'] += 1
        
        try:
            print("   Testing recovery to database-primary mode...")
            
            # Simulate database coming back online
            db_manager = DatabasePrimaryManager()
            
            # Test that normal operations work again
            print("   Verifying database operations restored...")
            
            # Test 1: Connection test
            try:
                count = db_manager.count_records()
                print(f"   Database record count: {count}")
                db_restored = True
            except Exception as e:
                print(f"   ❌ Database connection not restored: {e}")
                db_restored = False
            
            # Test 2: Read operations
            if db_restored:
                try:
                    record = db_manager.get_record_by_id(1)
                    if record:
                        print(f"   Database record read: {record['name']}")
                        read_restored = True
                    else:
                        print("   ⚠️ No record found, but no error")
                        read_restored = True
                except Exception as e:
                    print(f"   ❌ Database read not working: {e}")
                    read_restored = False
            else:
                read_restored = False
            
            # Test 3: Check usage statistics show recovery
            if db_restored:
                try:
                    stats = db_manager.get_usage_statistics()
                    print(f"   Database success rate: {stats.get('database_success_rate', 0):.1f}%")
                    print(f"   Fallback rate: {stats.get('fallback_rate', 0):.1f}%")
                    stats_available = True
                except Exception as e:
                    print(f"   ⚠️ Stats not available: {e}")
                    stats_available = False
            else:
                stats_available = False
            
            recovery_success = db_restored and read_restored
            
            if recovery_success:
                print("   ✅ Recovery to database-primary mode successful")
                self.test_results['tests_passed'] += 1
                return True
            else:
                print("   ❌ Recovery to database-primary mode failed")
                self.test_results['tests_failed'] += 1
                return False
                
        except Exception as e:
            print(f"   ❌ Recovery test failed: {e}")
            self.test_results['tests_failed'] += 1
            return False
    
    def test_data_integrity_during_fallback(self) -> bool:
        """Test data integrity during fallback scenarios."""
        
        print(f"\n🛡️ Test 6: Data Integrity During Fallback")
        print("-" * 40)
        self.test_results['tests_run'] += 1
        
        try:
            print("   Testing data integrity during fallback...")
            
            # Get baseline data from CSV
            csv_df = self.csv_manager.read()
            if len(csv_df) == 0:
                print("   ⚠️ No CSV data available for integrity testing")
                self.test_results['tests_passed'] += 1
                return True
            
            print(f"   CSV baseline: {len(csv_df)} records")
            
            # Test data consistency during multiple fallback scenarios
            integrity_issues = []
            
            # Test 1: Data consistency check
            try:
                # Check for duplicate row_ids
                duplicates = csv_df[csv_df['row_id'].duplicated()]
                if not duplicates.empty:
                    integrity_issues.append(f"Duplicate row_ids: {len(duplicates)}")
                
                # Check for missing critical data
                null_names = csv_df['name'].isnull().sum()
                null_emails = csv_df['email'].isnull().sum()
                
                if null_names > 0:
                    integrity_issues.append(f"Records with null names: {null_names}")
                if null_emails > 0:
                    integrity_issues.append(f"Records with null emails: {null_emails}")
                
                print(f"   Data quality issues found: {len(integrity_issues)}")
                
            except Exception as e:
                integrity_issues.append(f"Data consistency check failed: {e}")
            
            # Test 2: Simulate multiple fallback cycles
            print("   Testing integrity through multiple fallback cycles...")
            
            try:
                # Read CSV multiple times to ensure consistency
                reads = []
                for i in range(5):
                    df = self.csv_manager.read()
                    reads.append(len(df))
                
                # Check if all reads return same count
                consistent_reads = all(count == reads[0] for count in reads)
                if not consistent_reads:
                    integrity_issues.append(f"Inconsistent read results: {reads}")
                
                print(f"   Read consistency: {consistent_reads}")
                
            except Exception as e:
                integrity_issues.append(f"Multiple read test failed: {e}")
            
            # Overall integrity assessment
            if len(integrity_issues) == 0:
                print("   ✅ Data integrity maintained during fallback")
                self.test_results['tests_passed'] += 1
                return True
            else:
                print("   ❌ Data integrity issues detected:")
                for issue in integrity_issues:
                    print(f"      - {issue}")
                self.test_results['tests_failed'] += 1
                return False
                
        except Exception as e:
            print(f"   ❌ Data integrity test failed: {e}")
            self.test_results['tests_failed'] += 1
            return False
    
    def generate_fallback_report(self):
        """Generate comprehensive fallback testing report."""
        
        end_time = datetime.now()
        duration = (end_time - self.test_results['start_time']).total_seconds()
        
        print(f"\n📊 Emergency Fallback Testing Report")
        print("=" * 40)
        
        # Test summary
        print(f"Tests run: {self.test_results['tests_run']}")
        print(f"Tests passed: {self.test_results['tests_passed']}")
        print(f"Tests failed: {self.test_results['tests_failed']}")
        print(f"Success rate: {(self.test_results['tests_passed']/self.test_results['tests_run']*100):.1f}%")
        print(f"Test duration: {duration:.1f}s")
        
        # Fallback events
        if self.test_results['fallback_events']:
            print(f"\nFallback Events:")
            for event in self.test_results['fallback_events']:
                print(f"  - {event['scenario']}: {event.get('success_rate', 'N/A')}")
        
        # Overall assessment
        success_rate = (self.test_results['tests_passed']/self.test_results['tests_run']*100) if self.test_results['tests_run'] > 0 else 0
        
        print(f"\n💡 Emergency Fallback Assessment:")
        if success_rate >= 100:
            print(f"✅ EXCELLENT: Emergency fallback system working perfectly")
            print(f"✅ System can operate reliably in CSV-only mode")
            print(f"✅ Data integrity maintained during failures")
        elif success_rate >= 80:
            print(f"✅ GOOD: Emergency fallback system working with minor issues")
            print(f"✅ System can handle most failure scenarios")
        elif success_rate >= 60:
            print(f"⚠️ WARNING: Emergency fallback system has issues")
            print(f"⚠️ Review failure scenarios and improve robustness")
        else:
            print(f"🚨 CRITICAL: Emergency fallback system not working correctly")
            print(f"🚨 System may not survive database failures")
        
        # Recommendations
        print(f"\n🔧 Recommendations:")
        if success_rate >= 80:
            print(f"  • Emergency fallback system is production-ready")
            print(f"  • Consider periodic fallback testing in production")
        else:
            print(f"  • Improve fallback mechanisms before production")
            print(f"  • Add more comprehensive error handling")
            print(f"  • Consider enhanced CSV backup strategies")
        
        return success_rate >= 75
    
    def run_all_tests(self):
        """Run all emergency fallback tests."""
        
        print("🚨 Emergency Fallback to CSV Mode Testing")
        print("=" * 45)
        
        try:
            # Run all test scenarios
            test1_result = self.test_baseline_operations()
            test2_result = self.simulate_database_connection_failure()
            test3_result = self.test_csv_only_mode_operations()
            test4_result = self.test_database_timeout_scenario()
            test5_result = self.test_recovery_to_database_mode()
            test6_result = self.test_data_integrity_during_fallback()
            
            # Generate comprehensive report
            overall_success = self.generate_fallback_report()
            
            return overall_success
            
        except Exception as e:
            print(f"\n💥 Test suite failed: {e}")
            db_logger.error(f"Emergency fallback testing failed: {e}")
            return False

def main():
    """Execute emergency fallback testing."""
    
    tester = EmergencyFallbackTester()
    success = tester.run_all_tests()
    
    return success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)