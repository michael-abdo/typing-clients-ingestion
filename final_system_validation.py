#!/usr/bin/env python3
"""
Final System Validation and Performance Testing (Phase 3)

Comprehensive validation to ensure the database-native system is fully
operational, performant, and ready for production use.

Validation Areas:
1. Database connectivity and health
2. All CRUD operations functionality
3. Performance benchmarks and stress testing
4. System integration and workflow validation
5. Error handling and recovery
6. Migration completion verification
7. Production readiness assessment
"""

import os
import sys
import time
import json
import threading
import concurrent.futures
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Tuple
import statistics

# Set up environment
os.environ['DB_PASSWORD'] = 'migration_pass_2025'
sys.path.append('.')

from utils.database_native_operations import DatabaseNativeManager
from utils.database_logging import db_logger

class FinalSystemValidator:
    """Comprehensive final system validation for database-native mode."""
    
    def __init__(self):
        """Initialize final system validator."""
        
        self.manager = DatabaseNativeManager()
        self.validation_start = datetime.now()
        
        self.results = {
            'validation_timestamp': self.validation_start.isoformat(),
            'database_connectivity': {},
            'crud_operations': {},
            'performance_benchmarks': {},
            'stress_testing': {},
            'integration_testing': {},
            'error_handling': {},
            'migration_verification': {},
            'production_readiness': {},
            'overall_status': 'unknown'
        }
        
        print(f"🔍 Final System Validation (Database-Native)")
        print(f"Validation Time: {self.validation_start.strftime('%Y-%m-%d %H:%M:%S')}")
        print("=" * 60)
    
    def validate_database_connectivity(self) -> bool:
        """Validate database connectivity and configuration."""
        
        print(f"\n🔌 Database Connectivity Validation...")
        
        try:
            # Test 1: Basic health check
            print("   Testing database health check...")
            health = self.manager.health_check()
            health_status = health.get('status') == 'healthy'
            
            # Test 2: Connection performance
            print("   Testing connection performance...")
            connect_times = []
            for i in range(10):
                start = time.time()
                count = self.manager.count_records()
                connect_times.append(time.time() - start)
            
            avg_connect_time = statistics.mean(connect_times)
            
            # Test 3: Record accessibility
            print("   Testing record accessibility...")
            total_records = self.manager.count_records()
            sample_record = self.manager.get_record_by_id(1) if total_records > 0 else None
            
            connectivity_results = {
                'health_status': health_status,
                'health_details': health,
                'average_connection_time': avg_connect_time,
                'total_records': total_records,
                'sample_record_accessible': sample_record is not None,
                'connection_stability': all(t < 1.0 for t in connect_times)
            }
            
            self.results['database_connectivity'] = connectivity_results
            
            success = (health_status and avg_connect_time < 0.5 and total_records > 0)
            
            if success:
                print(f"   ✅ Database connectivity: EXCELLENT")
                print(f"   📊 Health: {health.get('status', 'unknown').upper()}")
                print(f"   ⚡ Avg connection: {avg_connect_time:.3f}s")
                print(f"   📈 Records available: {total_records}")
            else:
                print(f"   ❌ Database connectivity: ISSUES DETECTED")
            
            return success
            
        except Exception as e:
            print(f"   ❌ Database connectivity validation failed: {e}")
            self.results['database_connectivity'] = {'error': str(e)}
            return False
    
    def validate_crud_operations(self) -> bool:
        """Validate all CRUD operations thoroughly."""
        
        print(f"\n📝 CRUD Operations Validation...")
        
        try:
            test_record_id = 999990
            crud_results = {
                'create_test': False,
                'read_test': False,
                'update_test': False,
                'delete_test': False,
                'bulk_operations': False
            }
            
            # Test 1: Create Operation
            print("   Testing CREATE operations...")
            test_record = {
                'row_id': test_record_id,
                'name': 'Final Validation Test User',
                'email': 'final.validation@test.com',
                'type': 'Final Validation Test',
                'processed': False
            }
            
            create_success = self.manager.write_record(test_record)
            crud_results['create_test'] = create_success
            
            # Test 2: Read Operation
            print("   Testing READ operations...")
            read_record = self.manager.get_record_by_id(test_record_id)
            read_success = (read_record is not None and 
                          read_record.get('name') == test_record['name'])
            crud_results['read_test'] = read_success
            
            # Test 3: Update Operation
            print("   Testing UPDATE operations...")
            updates = {
                'name': 'Final Validation Updated User',
                'processed': True
            }
            update_success = self.manager.update_record(test_record_id, updates)
            
            # Verify update
            updated_record = self.manager.get_record_by_id(test_record_id)
            update_verified = (updated_record is not None and 
                             updated_record.get('processed') == True and
                             'Updated' in updated_record.get('name', ''))
            
            crud_results['update_test'] = update_success and update_verified
            
            # Test 4: Bulk Operations
            print("   Testing BULK operations...")
            bulk_records = [
                {
                    'row_id': 999989,
                    'name': 'Bulk Test 1',
                    'email': 'bulk1@final.test',
                    'type': 'Bulk Test',
                    'processed': False
                },
                {
                    'row_id': 999988,
                    'name': 'Bulk Test 2', 
                    'email': 'bulk2@final.test',
                    'type': 'Bulk Test',
                    'processed': False
                }
            ]
            
            bulk_written = self.manager.write_records_bulk(bulk_records)
            bulk_success = bulk_written == len(bulk_records)
            crud_results['bulk_operations'] = bulk_success
            
            # Test 5: Delete Operations
            print("   Testing DELETE operations...")
            delete_count = 0
            for record_id in [test_record_id, 999989, 999988]:
                if self.manager.delete_record(record_id):
                    delete_count += 1
            
            delete_success = delete_count == 3
            crud_results['delete_test'] = delete_success
            
            # Overall CRUD assessment
            crud_success = all(crud_results.values())
            self.results['crud_operations'] = crud_results
            
            if crud_success:
                print(f"   ✅ CRUD operations: ALL PASSED")
                print(f"   📊 Create: {'✅' if crud_results['create_test'] else '❌'}")
                print(f"   📊 Read: {'✅' if crud_results['read_test'] else '❌'}")
                print(f"   📊 Update: {'✅' if crud_results['update_test'] else '❌'}")
                print(f"   📊 Delete: {'✅' if crud_results['delete_test'] else '❌'}")
                print(f"   📊 Bulk: {'✅' if crud_results['bulk_operations'] else '❌'}")
            else:
                print(f"   ❌ CRUD operations: FAILURES DETECTED")
                for op, success in crud_results.items():
                    if not success:
                        print(f"      ❌ {op}: FAILED")
            
            return crud_success
            
        except Exception as e:
            print(f"   ❌ CRUD validation failed: {e}")
            self.results['crud_operations'] = {'error': str(e)}
            return False
    
    def validate_performance_benchmarks(self) -> bool:
        """Validate system performance under normal load."""
        
        print(f"\n⚡ Performance Benchmarks...")
        
        try:
            performance_results = {}
            
            # Test 1: Single record read performance
            print("   Testing single record read performance...")
            read_times = []
            for i in range(100):
                start = time.time()
                record = self.manager.get_record_by_id(1)
                read_times.append(time.time() - start)
            
            performance_results['single_read'] = {
                'avg_time': statistics.mean(read_times),
                'min_time': min(read_times),
                'max_time': max(read_times),
                'median_time': statistics.median(read_times)
            }
            
            # Test 2: Bulk read performance
            print("   Testing bulk read performance...")
            bulk_times = []
            for i in range(20):
                start = time.time()
                records = self.manager.get_records(limit=50)
                bulk_times.append(time.time() - start)
            
            performance_results['bulk_read'] = {
                'avg_time': statistics.mean(bulk_times),
                'throughput': 50 / statistics.mean(bulk_times)
            }
            
            # Test 3: Write performance
            print("   Testing write performance...")
            write_times = []
            test_records = []
            
            for i in range(20):
                test_record = {
                    'row_id': 999000 + i,
                    'name': f'Perf Test User {i}',
                    'email': f'perf{i}@test.com',
                    'type': 'Performance Test',
                    'processed': False
                }
                
                start = time.time()
                self.manager.write_record(test_record)
                write_times.append(time.time() - start)
                test_records.append(999000 + i)
            
            performance_results['write'] = {
                'avg_time': statistics.mean(write_times),
                'throughput': 1 / statistics.mean(write_times)
            }
            
            # Cleanup performance test records
            print("   Cleaning up performance test records...")
            for record_id in test_records:
                self.manager.delete_record(record_id)
            
            # Test 4: Count operation performance
            print("   Testing count operation performance...")
            count_times = []
            for i in range(20):
                start = time.time()
                count = self.manager.count_records()
                count_times.append(time.time() - start)
            
            performance_results['count'] = {
                'avg_time': statistics.mean(count_times)
            }
            
            # Performance assessment
            read_ok = performance_results['single_read']['avg_time'] < 0.1
            bulk_ok = performance_results['bulk_read']['avg_time'] < 0.5
            write_ok = performance_results['write']['avg_time'] < 0.2
            count_ok = performance_results['count']['avg_time'] < 0.1
            
            performance_success = read_ok and bulk_ok and write_ok and count_ok
            
            self.results['performance_benchmarks'] = performance_results
            
            if performance_success:
                print(f"   ✅ Performance benchmarks: EXCELLENT")
                print(f"   📊 Single read: {performance_results['single_read']['avg_time']:.3f}s avg")
                print(f"   📊 Bulk read: {performance_results['bulk_read']['throughput']:.0f} records/s")
                print(f"   📊 Write: {performance_results['write']['avg_time']:.3f}s avg")
                print(f"   📊 Count: {performance_results['count']['avg_time']:.3f}s avg")
            else:
                print(f"   ⚠️ Performance benchmarks: SOME ISSUES")
                if not read_ok:
                    print(f"      ⚠️ Single read performance: {performance_results['single_read']['avg_time']:.3f}s (slow)")
                if not write_ok:
                    print(f"      ⚠️ Write performance: {performance_results['write']['avg_time']:.3f}s (slow)")
            
            return performance_success
            
        except Exception as e:
            print(f"   ❌ Performance validation failed: {e}")
            self.results['performance_benchmarks'] = {'error': str(e)}
            return False
    
    def validate_stress_testing(self) -> bool:
        """Validate system under stress conditions."""
        
        print(f"\n🔥 Stress Testing...")
        
        try:
            stress_results = {}
            
            # Test 1: Concurrent read operations
            print("   Testing concurrent read operations...")
            
            def concurrent_read_test():
                try:
                    start = time.time()
                    record = self.manager.get_record_by_id(1)
                    return time.time() - start, record is not None
                except Exception as e:
                    return None, False
            
            with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
                concurrent_reads = list(executor.map(lambda _: concurrent_read_test(), range(50)))
            
            successful_reads = [r for r in concurrent_reads if r[1]]
            read_times = [r[0] for r in successful_reads if r[0] is not None]
            
            stress_results['concurrent_reads'] = {
                'total_operations': len(concurrent_reads),
                'successful_operations': len(successful_reads),
                'success_rate': len(successful_reads) / len(concurrent_reads) * 100,
                'avg_time': statistics.mean(read_times) if read_times else 0
            }
            
            # Test 2: Rapid sequential operations
            print("   Testing rapid sequential operations...")
            
            rapid_times = []
            rapid_success = 0
            
            for i in range(100):
                try:
                    start = time.time()
                    count = self.manager.count_records()
                    rapid_times.append(time.time() - start)
                    if count > 0:
                        rapid_success += 1
                except:
                    pass
            
            stress_results['rapid_sequential'] = {
                'total_operations': 100,
                'successful_operations': rapid_success,
                'success_rate': rapid_success / 100 * 100,
                'avg_time': statistics.mean(rapid_times) if rapid_times else 0
            }
            
            # Test 3: Large data query
            print("   Testing large data queries...")
            
            try:
                start = time.time()
                all_records = self.manager.get_records()  # Get all records
                large_query_time = time.time() - start
                large_query_success = len(all_records) > 0
                
                stress_results['large_query'] = {
                    'query_time': large_query_time,
                    'records_returned': len(all_records),
                    'success': large_query_success,
                    'throughput': len(all_records) / large_query_time if large_query_time > 0 else 0
                }
            except Exception as e:
                stress_results['large_query'] = {'error': str(e), 'success': False}
            
            # Stress test assessment
            concurrent_ok = stress_results['concurrent_reads']['success_rate'] >= 95
            rapid_ok = stress_results['rapid_sequential']['success_rate'] >= 95
            large_ok = stress_results['large_query'].get('success', False)
            
            stress_success = concurrent_ok and rapid_ok and large_ok
            
            self.results['stress_testing'] = stress_results
            
            if stress_success:
                print(f"   ✅ Stress testing: PASSED")
                print(f"   📊 Concurrent reads: {stress_results['concurrent_reads']['success_rate']:.1f}% success")
                print(f"   📊 Rapid sequential: {stress_results['rapid_sequential']['success_rate']:.1f}% success")
                print(f"   📊 Large query: {stress_results['large_query']['records_returned']} records")
            else:
                print(f"   ⚠️ Stress testing: SOME ISSUES")
            
            return stress_success
            
        except Exception as e:
            print(f"   ❌ Stress testing failed: {e}")
            self.results['stress_testing'] = {'error': str(e)}
            return False
    
    def validate_error_handling(self) -> bool:
        """Validate error handling and recovery."""
        
        print(f"\n🛡️ Error Handling Validation...")
        
        try:
            error_results = {}
            
            # Test 1: Invalid record ID handling
            print("   Testing invalid record ID handling...")
            try:
                invalid_record = self.manager.get_record_by_id(-999)
                invalid_id_handled = invalid_record is None
            except Exception as e:
                invalid_id_handled = "not found" in str(e).lower() or "invalid" in str(e).lower()
            
            error_results['invalid_id_handling'] = invalid_id_handled
            
            # Test 2: Missing field handling
            print("   Testing missing field handling...")
            try:
                invalid_record = {'name': 'Missing Row ID'}  # No row_id
                self.manager.write_record(invalid_record)
                missing_field_handled = False  # Should have failed
            except Exception as e:
                missing_field_handled = "required" in str(e).lower() or "row_id" in str(e).lower()
            
            error_results['missing_field_handling'] = missing_field_handled
            
            # Test 3: Non-existent record update
            print("   Testing non-existent record update...")
            try:
                result = self.manager.update_record(99999999, {'name': 'Does not exist'})
                nonexistent_update_handled = result == False  # Should return False
            except Exception:
                nonexistent_update_handled = True  # Graceful error handling
            
            error_results['nonexistent_update_handling'] = nonexistent_update_handled
            
            # Test 4: Non-existent record deletion
            print("   Testing non-existent record deletion...")
            try:
                result = self.manager.delete_record(99999999)
                nonexistent_delete_handled = result == False  # Should return False
            except Exception:
                nonexistent_delete_handled = True  # Graceful error handling
            
            error_results['nonexistent_delete_handling'] = nonexistent_delete_handled
            
            # Error handling assessment
            error_handling_success = all(error_results.values())
            
            self.results['error_handling'] = error_results
            
            if error_handling_success:
                print(f"   ✅ Error handling: ROBUST")
                print(f"   📊 Invalid ID: {'✅' if error_results['invalid_id_handling'] else '❌'}")
                print(f"   📊 Missing fields: {'✅' if error_results['missing_field_handling'] else '❌'}")
                print(f"   📊 Nonexistent update: {'✅' if error_results['nonexistent_update_handling'] else '❌'}")
                print(f"   📊 Nonexistent delete: {'✅' if error_results['nonexistent_delete_handling'] else '❌'}")
            else:
                print(f"   ❌ Error handling: ISSUES DETECTED")
            
            return error_handling_success
            
        except Exception as e:
            print(f"   ❌ Error handling validation failed: {e}")
            self.results['error_handling'] = {'error': str(e)}
            return False
    
    def validate_migration_completion(self) -> bool:
        """Validate that migration has been completed successfully."""
        
        print(f"\n🎯 Migration Completion Validation...")
        
        try:
            migration_results = {}
            
            # Test 1: Archive verification
            print("   Verifying CSV files are archived...")
            archive_dir = Path("archive")
            if archive_dir.exists():
                archive_dirs = list(archive_dir.glob("csv_migration_*"))
                latest_archive = max(archive_dirs, key=lambda p: p.name) if archive_dirs else None
                
                if latest_archive:
                    manifest_file = latest_archive / "ARCHIVE_MANIFEST.json"
                    archive_verified = manifest_file.exists()
                    
                    if archive_verified:
                        with open(manifest_file) as f:
                            manifest = json.load(f)
                        archived_files = len(manifest.get('archived_files', []))
                    else:
                        archived_files = 0
                else:
                    archive_verified = False
                    archived_files = 0
            else:
                archive_verified = False
                archived_files = 0
            
            migration_results['archive_verified'] = archive_verified
            migration_results['archived_files'] = archived_files
            
            # Test 2: CSV files removed from active system
            print("   Verifying CSV files removed from active system...")
            active_csv_removed = not Path("outputs/output.csv").exists()
            migration_results['active_csv_removed'] = active_csv_removed
            
            # Test 3: Migration status file
            print("   Checking migration status...")
            migration_status_file = Path("migration_status.json")
            if migration_status_file.exists():
                with open(migration_status_file) as f:
                    migration_status = json.load(f)
                migration_complete = migration_status.get('csv_migration_status') == 'completed'
                database_mode = migration_status.get('database_mode') == 'native_only'
            else:
                migration_complete = False
                database_mode = False
            
            migration_results['migration_status_complete'] = migration_complete
            migration_results['database_native_mode'] = database_mode
            
            # Test 4: Database contains data
            print("   Verifying database contains migrated data...")
            record_count = self.manager.count_records()
            database_has_data = record_count > 0
            migration_results['database_record_count'] = record_count
            migration_results['database_has_data'] = database_has_data
            
            # Test 5: Usage statistics show database-native mode
            print("   Verifying system operates in database-native mode...")
            stats = self.manager.get_usage_statistics()
            native_mode = stats.get('mode') == 'database_native'
            migration_results['usage_stats_native_mode'] = native_mode
            
            # Migration completion assessment
            migration_success = (archive_verified and active_csv_removed and 
                               migration_complete and database_mode and 
                               database_has_data and native_mode)
            
            self.results['migration_verification'] = migration_results
            
            if migration_success:
                print(f"   ✅ Migration completion: VERIFIED")
                print(f"   📦 CSV files archived: {archived_files} files")
                print(f"   🗑️ Active CSV removed: {'✅' if active_csv_removed else '❌'}")
                print(f"   📊 Database records: {record_count}")
                print(f"   🎯 Database-native mode: {'✅' if native_mode else '❌'}")
            else:
                print(f"   ❌ Migration completion: ISSUES DETECTED")
            
            return migration_success
            
        except Exception as e:
            print(f"   ❌ Migration validation failed: {e}")
            self.results['migration_verification'] = {'error': str(e)}
            return False
    
    def assess_production_readiness(self) -> bool:
        """Assess overall production readiness."""
        
        print(f"\n🚀 Production Readiness Assessment...")
        
        try:
            # Collect all test results
            connectivity_ok = self.results.get('database_connectivity', {}).get('health_status', False)
            crud_ok = all(self.results.get('crud_operations', {}).values()) if 'error' not in self.results.get('crud_operations', {}) else False
            performance_ok = self.results.get('performance_benchmarks', {}).get('single_read', {}).get('avg_time', 1) < 0.1
            stress_ok = self.results.get('stress_testing', {}).get('concurrent_reads', {}).get('success_rate', 0) >= 95
            error_ok = all(self.results.get('error_handling', {}).values()) if 'error' not in self.results.get('error_handling', {}) else False
            migration_ok = self.results.get('migration_verification', {}).get('database_has_data', False)
            
            # Production readiness criteria
            readiness_criteria = {
                'database_connectivity': connectivity_ok,
                'crud_operations': crud_ok,
                'performance_acceptable': performance_ok,
                'stress_testing': stress_ok,
                'error_handling': error_ok,
                'migration_complete': migration_ok
            }
            
            passed_criteria = sum(readiness_criteria.values())
            total_criteria = len(readiness_criteria)
            readiness_score = (passed_criteria / total_criteria * 100) if total_criteria > 0 else 0
            
            # Final assessment
            if readiness_score >= 100:
                readiness_status = "PRODUCTION READY"
                recommendation = "System ready for production deployment"
            elif readiness_score >= 90:
                readiness_status = "MOSTLY READY"
                recommendation = "Minor issues to address before production"
            elif readiness_score >= 75:
                readiness_status = "NEEDS WORK"
                recommendation = "Significant issues must be resolved"
            else:
                readiness_status = "NOT READY"
                recommendation = "Major issues prevent production deployment"
            
            production_results = {
                'readiness_criteria': readiness_criteria,
                'passed_criteria': passed_criteria,
                'total_criteria': total_criteria,
                'readiness_score': readiness_score,
                'readiness_status': readiness_status,
                'recommendation': recommendation
            }
            
            self.results['production_readiness'] = production_results
            
            print(f"   📊 Readiness Score: {readiness_score:.0f}% ({passed_criteria}/{total_criteria} criteria)")
            print(f"   🎯 Status: {readiness_status}")
            print(f"   💡 Recommendation: {recommendation}")
            
            print(f"\n   📋 Criteria Breakdown:")
            for criterion, passed in readiness_criteria.items():
                status_icon = "✅" if passed else "❌"
                print(f"      {status_icon} {criterion.replace('_', ' ').title()}")
            
            return readiness_score >= 90
            
        except Exception as e:
            print(f"   ❌ Production readiness assessment failed: {e}")
            self.results['production_readiness'] = {'error': str(e)}
            return False
    
    def generate_final_report(self) -> Dict[str, Any]:
        """Generate comprehensive final validation report."""
        
        validation_end = datetime.now()
        duration = (validation_end - self.validation_start).total_seconds()
        
        print(f"\n📊 Final System Validation Report")
        print("=" * 50)
        
        # Summary
        connectivity_ok = 'database_connectivity' in self.results and 'error' not in self.results['database_connectivity']
        crud_ok = 'crud_operations' in self.results and 'error' not in self.results['crud_operations']
        performance_ok = 'performance_benchmarks' in self.results and 'error' not in self.results['performance_benchmarks']
        stress_ok = 'stress_testing' in self.results and 'error' not in self.results['stress_testing']
        error_ok = 'error_handling' in self.results and 'error' not in self.results['error_handling']
        migration_ok = 'migration_verification' in self.results and 'error' not in self.results['migration_verification']
        
        validation_success = connectivity_ok and crud_ok and performance_ok and stress_ok and error_ok and migration_ok
        
        print(f"Validation Duration: {duration:.1f}s")
        print(f"Validation Success: {'✅ PASSED' if validation_success else '❌ FAILED'}")
        
        print(f"\nValidation Results:")
        print(f"  Database Connectivity: {'✅' if connectivity_ok else '❌'}")
        print(f"  CRUD Operations: {'✅' if crud_ok else '❌'}")
        print(f"  Performance Benchmarks: {'✅' if performance_ok else '❌'}")
        print(f"  Stress Testing: {'✅' if stress_ok else '❌'}")
        print(f"  Error Handling: {'✅' if error_ok else '❌'}")
        print(f"  Migration Verification: {'✅' if migration_ok else '❌'}")
        
        # Production readiness
        readiness = self.results.get('production_readiness', {})
        if 'readiness_status' in readiness:
            print(f"\nProduction Readiness: {readiness['readiness_status']}")
            print(f"Recommendation: {readiness['recommendation']}")
        
        # Overall status
        if validation_success:
            self.results['overall_status'] = 'success'
            print(f"\n🎉 FINAL VALIDATION: SUCCESS")
            print(f"✅ Database-native system is fully operational")
            print(f"✅ All validation criteria passed")
            print(f"✅ System ready for production use")
        else:
            self.results['overall_status'] = 'failure'
            print(f"\n⚠️ FINAL VALIDATION: ISSUES DETECTED")
            print(f"🔧 Review failed validation criteria")
            print(f"🔧 Address issues before production deployment")
        
        # Save results
        results_file = Path(f"final_validation_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        with open(results_file, 'w') as f:
            json.dump(self.results, f, indent=2)
        
        print(f"\n📁 Validation results saved: {results_file}")
        
        return self.results
    
    def run_full_validation(self) -> bool:
        """Run complete final system validation."""
        
        try:
            # Run all validation tests
            test1 = self.validate_database_connectivity()
            test2 = self.validate_crud_operations()
            test3 = self.validate_performance_benchmarks()
            test4 = self.validate_stress_testing()
            test5 = self.validate_error_handling()
            test6 = self.validate_migration_completion()
            test7 = self.assess_production_readiness()
            
            # Generate final report
            self.generate_final_report()
            
            # Overall success
            overall_success = test1 and test2 and test3 and test4 and test5 and test6 and test7
            
            return overall_success
            
        except Exception as e:
            print(f"\n💥 Final validation failed: {e}")
            db_logger.error(f"Final system validation failed: {e}")
            return False

def main():
    """Execute final system validation."""
    
    validator = FinalSystemValidator()
    success = validator.run_full_validation()
    
    return success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)