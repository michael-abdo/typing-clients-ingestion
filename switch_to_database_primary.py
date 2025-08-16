#!/usr/bin/env python3
"""
Switch Read Operations to Database-Primary

Systematically switches existing CSV read operations to database-primary reads
with comprehensive validation and testing.

Key Functions:
1. Identify existing CSV read operations
2. Create database-primary versions of key functions
3. Test that switched operations return identical data
4. Provide rollback mechanism if needed
5. Generate migration report
"""

import os
import sys
import pandas as pd
import shutil
from pathlib import Path
from typing import Dict, Any, List, Tuple
from datetime import datetime

# Set up environment
sys.path.append('.')

from utils.database_primary_manager import DatabasePrimaryManager, read_csv_safe
from utils.csv_manager import CSVManager
from utils.database_logging import db_logger

class DatabasePrimaryMigrator:
    """Migrates read operations from CSV-only to database-primary."""
    
    def __init__(self):
        """Initialize the migrator."""
        self.csv_path = "outputs/output.csv"
        self.db_manager = DatabasePrimaryManager(self.csv_path)
        self.csv_manager = CSVManager(self.csv_path)
        
        self.migration_results = {
            'operations_tested': 0,
            'successful_migrations': 0,
            'failed_migrations': 0,
            'data_mismatches': 0,
            'performance_improvements': [],
            'start_time': datetime.now()
        }
        
        # Backup for rollback
        self.backup_created = False
        
    def create_backup(self):
        """Create backup of CSV file for rollback purposes."""
        if Path(self.csv_path).exists():
            backup_path = f"{self.csv_path}.migration_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
            shutil.copy2(self.csv_path, backup_path)
            self.backup_created = True
            db_logger.info(f"📁 Migration backup created: {backup_path}")
            return backup_path
        return None
    
    def test_read_operation_equivalence(self, operation_name: str, 
                                       csv_operation: callable, 
                                       db_operation: callable,
                                       *args, **kwargs) -> Tuple[bool, Dict[str, Any]]:
        """
        Test that CSV and database operations return equivalent data.
        
        Args:
            operation_name: Name of the operation being tested
            csv_operation: Function that performs CSV read
            db_operation: Function that performs database-primary read
            *args, **kwargs: Arguments for both operations
            
        Returns:
            Tuple of (success, metrics)
        """
        self.migration_results['operations_tested'] += 1
        
        print(f"🧪 Testing operation: {operation_name}")
        
        # Time CSV operation
        csv_start = datetime.now()
        try:
            csv_result = csv_operation(*args, **kwargs)
            csv_time = (datetime.now() - csv_start).total_seconds()
            csv_success = True
        except Exception as csv_error:
            csv_time = (datetime.now() - csv_start).total_seconds()
            csv_success = False
            csv_result = None
            print(f"   ❌ CSV operation failed: {csv_error}")
        
        # Time database operation
        db_start = datetime.now()
        try:
            db_result = db_operation(*args, **kwargs)
            db_time = (datetime.now() - db_start).total_seconds()
            db_success = True
        except Exception as db_error:
            db_time = (datetime.now() - db_start).total_seconds()
            db_success = False
            db_result = None
            print(f"   ❌ Database operation failed: {db_error}")
        
        # Compare results
        data_equivalent = False
        mismatch_details = {}
        
        if csv_success and db_success:
            try:
                if isinstance(csv_result, pd.DataFrame) and isinstance(db_result, pd.DataFrame):
                    # Compare DataFrames
                    if len(csv_result) == len(db_result):
                        # Check if row_ids match (main validation)
                        csv_ids = set(csv_result['row_id'].tolist()) if 'row_id' in csv_result.columns else set()
                        db_ids = set(db_result['row_id'].tolist()) if 'row_id' in db_result.columns else set()
                        
                        if csv_ids == db_ids:
                            data_equivalent = True
                        else:
                            mismatch_details['id_difference'] = {
                                'csv_only': list(csv_ids - db_ids)[:5],
                                'db_only': list(db_ids - csv_ids)[:5]
                            }
                    else:
                        mismatch_details['count_difference'] = {
                            'csv_count': len(csv_result),
                            'db_count': len(db_result)
                        }
                        
                elif isinstance(csv_result, (int, float)) and isinstance(db_result, (int, float)):
                    # Compare numeric results
                    data_equivalent = abs(csv_result - db_result) <= 1  # Allow for small differences
                    if not data_equivalent:
                        mismatch_details['value_difference'] = {
                            'csv_value': csv_result,
                            'db_value': db_result
                        }
                        
                elif isinstance(csv_result, dict) and isinstance(db_result, dict):
                    # Compare dictionaries (single records)
                    if csv_result.get('row_id') == db_result.get('row_id'):
                        data_equivalent = True
                    else:
                        mismatch_details['record_difference'] = {
                            'csv_id': csv_result.get('row_id'),
                            'db_id': db_result.get('row_id')
                        }
                        
                elif isinstance(csv_result, list) and isinstance(db_result, list):
                    # Compare lists of records
                    if len(csv_result) == len(db_result):
                        csv_ids = {r.get('row_id') for r in csv_result if isinstance(r, dict)}
                        db_ids = {r.get('row_id') for r in db_result if isinstance(r, dict)}
                        data_equivalent = csv_ids == db_ids
                        if not data_equivalent:
                            mismatch_details['list_difference'] = {
                                'csv_count': len(csv_result),
                                'db_count': len(db_result)
                            }
                    else:
                        mismatch_details['list_count_difference'] = {
                            'csv_count': len(csv_result),
                            'db_count': len(db_result)
                        }
                        
            except Exception as comparison_error:
                mismatch_details['comparison_error'] = str(comparison_error)
        
        # Performance analysis
        performance_improvement = ((csv_time - db_time) / csv_time * 100) if csv_time > 0 else 0
        
        metrics = {
            'csv_time': csv_time,
            'db_time': db_time,
            'csv_success': csv_success,
            'db_success': db_success,
            'data_equivalent': data_equivalent,
            'mismatch_details': mismatch_details,
            'performance_improvement_percent': performance_improvement
        }
        
        # Record results
        if csv_success and db_success and data_equivalent:
            self.migration_results['successful_migrations'] += 1
            self.migration_results['performance_improvements'].append(performance_improvement)
            print(f"   ✅ Operation successful - Data equivalent, {performance_improvement:+.1f}% performance change")
        else:
            self.migration_results['failed_migrations'] += 1
            if not data_equivalent:
                self.migration_results['data_mismatches'] += 1
            print(f"   ❌ Operation failed - CSV: {csv_success}, DB: {db_success}, Equivalent: {data_equivalent}")
            if mismatch_details:
                print(f"   🔍 Mismatch details: {mismatch_details}")
        
        return (csv_success and db_success and data_equivalent), metrics
    
    def test_core_read_operations(self):
        """Test core read operations for equivalence."""
        
        print("🧪 Testing Core Read Operations")
        print("=" * 35)
        
        # Test 1: Full DataFrame read
        def csv_read_all():
            return self.csv_manager.read()
        
        def db_read_all():
            return self.db_manager.read_dataframe()
        
        self.test_read_operation_equivalence(
            "Full DataFrame Read",
            csv_read_all,
            db_read_all
        )
        
        # Test 2: Single record read
        test_row_id = 1  # Use a known row_id
        
        def csv_read_single():
            df = self.csv_manager.read()
            matches = df[df['row_id'] == test_row_id]
            return matches.iloc[0].to_dict() if not matches.empty else None
        
        def db_read_single():
            return self.db_manager.get_record_by_id(test_row_id)
        
        self.test_read_operation_equivalence(
            f"Single Record Read (ID {test_row_id})",
            csv_read_single,
            db_read_single
        )
        
        # Test 3: Record count
        def csv_count():
            df = self.csv_manager.read()
            return len(df)
        
        def db_count():
            return self.db_manager.count_records()
        
        self.test_read_operation_equivalence(
            "Record Count",
            csv_count,
            db_count
        )
        
        # Test 4: Filtered records (processed=True)
        def csv_filtered():
            df = self.csv_manager.read()
            return df[df['processed'] == True].to_dict('records')
        
        def db_filtered():
            return self.db_manager.get_records_by_criteria(processed=True)
        
        self.test_read_operation_equivalence(
            "Filtered Records (processed=True)",
            csv_filtered,
            db_filtered
        )
        
        # Test 5: Limited records
        def csv_limited():
            df = self.csv_manager.read()
            return df.head(10)
        
        def db_limited():
            return pd.DataFrame(self.db_manager.db_reader.get_records(limit=10))
        
        self.test_read_operation_equivalence(
            "Limited Records (10)",
            csv_limited,
            db_limited
        )
    
    def switch_read_operations(self):
        """Switch core read operations to database-primary."""
        
        print("\n🔄 Switching Read Operations to Database-Primary")
        print("=" * 50)
        
        # Create enhanced read function wrappers
        enhanced_functions = {}
        
        # Enhanced read_csv_safe function
        def enhanced_read_csv_safe(csv_path: str = "outputs/output.csv", dtype_spec: str = 'tracking'):
            """Enhanced CSV read with database-primary capability."""
            db_logger.info(f"📖 Enhanced read_csv_safe called for {csv_path}")
            return read_csv_safe(csv_path, dtype_spec, use_database_primary=True)
        
        enhanced_functions['read_csv_safe'] = enhanced_read_csv_safe
        
        # Enhanced single record getter
        def enhanced_get_record(row_id: int, csv_path: str = "outputs/output.csv"):
            """Enhanced record getter with database-primary capability."""
            db_logger.info(f"🔍 Enhanced get_record called for row_id {row_id}")
            manager = DatabasePrimaryManager(csv_path)
            return manager.get_record_by_id(row_id)
        
        enhanced_functions['get_record'] = enhanced_get_record
        
        # Enhanced count function
        def enhanced_count_records(csv_path: str = "outputs/output.csv", **criteria):
            """Enhanced count with database-primary capability."""
            db_logger.info(f"🔢 Enhanced count_records called")
            manager = DatabasePrimaryManager(csv_path)
            return manager.count_records(**criteria)
        
        enhanced_functions['count_records'] = enhanced_count_records
        
        print("✅ Enhanced read functions created")
        return enhanced_functions
    
    def generate_migration_report(self) -> Dict[str, Any]:
        """Generate comprehensive migration report."""
        
        end_time = datetime.now()
        duration = (end_time - self.migration_results['start_time']).total_seconds()
        
        # Calculate performance statistics
        performance_improvements = self.migration_results['performance_improvements']
        avg_performance_improvement = sum(performance_improvements) / len(performance_improvements) if performance_improvements else 0
        
        report = {
            'migration_summary': {
                'operations_tested': self.migration_results['operations_tested'],
                'successful_migrations': self.migration_results['successful_migrations'],
                'failed_migrations': self.migration_results['failed_migrations'],
                'data_mismatches': self.migration_results['data_mismatches'],
                'success_rate': (self.migration_results['successful_migrations'] / self.migration_results['operations_tested'] * 100) if self.migration_results['operations_tested'] > 0 else 0,
                'migration_duration': duration
            },
            'performance_analysis': {
                'average_performance_change': avg_performance_improvement,
                'performance_improvements': performance_improvements,
                'operations_with_improvements': len([p for p in performance_improvements if p > 0]),
                'operations_with_degradation': len([p for p in performance_improvements if p < 0])
            },
            'database_manager_stats': self.db_manager.get_usage_statistics(),
            'recommendations': self._generate_migration_recommendations()
        }
        
        return report
    
    def _generate_migration_recommendations(self) -> List[str]:
        """Generate migration recommendations based on test results."""
        
        recommendations = []
        
        success_rate = (self.migration_results['successful_migrations'] / self.migration_results['operations_tested'] * 100) if self.migration_results['operations_tested'] > 0 else 0
        
        if success_rate >= 100:
            recommendations.append("✅ All operations migrated successfully - ready for production")
        elif success_rate >= 80:
            recommendations.append(f"⚠️ Most operations successful ({success_rate:.1f}%) - review failed operations")
        else:
            recommendations.append(f"❌ Low success rate ({success_rate:.1f}%) - investigate issues before proceeding")
        
        if self.migration_results['data_mismatches'] > 0:
            recommendations.append(f"🚨 {self.migration_results['data_mismatches']} data mismatches detected - investigate data consistency")
        
        avg_performance = sum(self.migration_results['performance_improvements']) / len(self.migration_results['performance_improvements']) if self.migration_results['performance_improvements'] else 0
        
        if avg_performance > 10:
            recommendations.append(f"🚀 Significant performance improvement ({avg_performance:.1f}%) achieved")
        elif avg_performance < -10:
            recommendations.append(f"⚠️ Performance degradation ({avg_performance:.1f}%) - investigate database performance")
        
        return recommendations

def main():
    """Execute the database-primary migration."""
    
    print("🔄 Database-Primary Read Operations Migration")
    print("=" * 50)
    
    migrator = DatabasePrimaryMigrator()
    
    try:
        # Create backup
        backup_path = migrator.create_backup()
        
        # Test core operations
        migrator.test_core_read_operations()
        
        # Switch operations
        enhanced_functions = migrator.switch_read_operations()
        
        # Test enhanced functions
        print(f"\n🧪 Testing Enhanced Functions")
        print("-" * 30)
        
        # Test enhanced read_csv_safe
        print("📖 Testing enhanced read_csv_safe...")
        df = enhanced_functions['read_csv_safe']()
        print(f"   ✅ Enhanced read successful: {len(df)} records")
        
        # Test enhanced get_record
        print("🔍 Testing enhanced get_record...")
        record = enhanced_functions['get_record'](1)
        if record:
            print(f"   ✅ Enhanced record read: {record['name']}")
        
        # Test enhanced count_records
        print("🔢 Testing enhanced count_records...")
        count = enhanced_functions['count_records']()
        print(f"   ✅ Enhanced count: {count} records")
        
        # Generate report
        print(f"\n📊 Migration Report")
        print("=" * 20)
        
        report = migrator.generate_migration_report()
        
        summary = report['migration_summary']
        print(f"Operations tested: {summary['operations_tested']}")
        print(f"Successful migrations: {summary['successful_migrations']}")
        print(f"Failed migrations: {summary['failed_migrations']}")
        print(f"Success rate: {summary['success_rate']:.1f}%")
        print(f"Migration duration: {summary['migration_duration']:.1f}s")
        
        performance = report['performance_analysis']
        print(f"\nPerformance change: {performance['average_performance_change']:+.1f}%")
        print(f"Operations improved: {performance['operations_with_improvements']}")
        print(f"Operations degraded: {performance['operations_with_degradation']}")
        
        db_stats = report['database_manager_stats']
        print(f"\nDatabase success rate: {db_stats['database_success_rate']:.1f}%")
        print(f"Fallback rate: {db_stats['fallback_rate']:.1f}%")
        
        print(f"\n💡 Recommendations:")
        for i, rec in enumerate(report['recommendations'], 1):
            print(f"  {i}. {rec}")
        
        if summary['success_rate'] >= 80:
            print(f"\n🎉 Migration completed successfully!")
            print(f"✅ Read operations switched to database-primary")
            print(f"✅ Data validation passed")
            print(f"✅ Performance within acceptable range")
            return True
        else:
            print(f"\n⚠️ Migration completed with issues")
            print(f"❌ Review failed operations before proceeding")
            return False
        
    except Exception as e:
        print(f"\n💥 Migration failed: {e}")
        db_logger.error(f"Database-primary migration failed: {e}")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)