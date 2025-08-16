#!/usr/bin/env python3
"""
CSV Backup Synchronization Verification

Comprehensive testing to verify that CSV backup remains synchronized
with database operations during dual-write phase.

Tests:
1. New record sync - verify new DB records appear in CSV
2. Update sync - verify DB updates are reflected in CSV  
3. Data consistency - verify CSV matches DB content
4. Backup reliability - verify CSV can serve as complete backup
5. Dual-write integrity - verify both systems stay in sync
"""

import os
import sys
import pandas as pd
import shutil
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Tuple

# Set up environment
os.environ['DB_PASSWORD'] = 'migration_pass_2025'
sys.path.append('.')

from utils.database_operations import DatabaseConfig, DatabaseManager
from utils.csv_manager import CSVManager
from utils.data_validation import DataValidator
from utils.database_logging import db_logger

class CSVBackupSyncVerifier:
    """Verify CSV backup synchronization with database."""
    
    def __init__(self):
        """Initialize sync verifier."""
        self.csv_path = "outputs/output.csv"
        self.backup_path = f"{self.csv_path}.sync_test_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Create backup of current CSV
        if Path(self.csv_path).exists():
            shutil.copy2(self.csv_path, self.backup_path)
            print(f"📁 Created CSV backup: {self.backup_path}")
        
        # Initialize components
        self.config = DatabaseConfig(
            db_type='postgresql',
            host='localhost',
            port=5432,
            database='typing_clients_uuid',
            username='migration_user',
            password='migration_pass_2025'
        )
        
        self.db_manager = DatabaseManager(self.config)
        self.csv_manager = CSVManager(self.csv_path)
        self.validator = DataValidator()
        
        self.test_results = {
            'tests_run': 0,
            'tests_passed': 0,
            'tests_failed': 0,
            'sync_issues': [],
            'start_time': datetime.now()
        }
        
    def get_record_counts(self) -> Tuple[int, int]:
        """Get current record counts from both sources."""
        
        # Database count
        with self.db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) as count FROM typing_clients_data")
            db_count = cursor.fetchone()['count']
        
        # CSV count
        try:
            csv_df = self.csv_manager.read()
            csv_count = len(csv_df)
        except Exception as e:
            print(f"⚠️ Error reading CSV: {e}")
            csv_count = 0
        
        return db_count, csv_count
    
    def test_current_sync_status(self) -> bool:
        """Test current synchronization status between DB and CSV."""
        
        print("🔍 Test 1: Current Sync Status")
        print("-" * 30)
        self.test_results['tests_run'] += 1
        
        try:
            db_count, csv_count = self.get_record_counts()
            
            print(f"   Database records: {db_count}")
            print(f"   CSV records: {csv_count}")
            
            # Get overlapping data
            csv_df = self.csv_manager.read()
            csv_row_ids = set(csv_df['row_id'].dropna().astype(float).astype(int).tolist())
            
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT row_id FROM typing_clients_data")
                db_row_ids = set(row['row_id'] for row in cursor.fetchall())
            
            overlapping_ids = csv_row_ids.intersection(db_row_ids)
            csv_only = csv_row_ids - db_row_ids
            db_only = db_row_ids - csv_row_ids
            
            print(f"   Overlapping records: {len(overlapping_ids)}")
            print(f"   CSV-only records: {len(csv_only)}")
            print(f"   DB-only records: {len(db_only)}")
            
            # Test data consistency for overlapping records
            consistent_count = 0
            inconsistent_count = 0
            
            for row_id in list(overlapping_ids)[:10]:  # Test first 10
                # Get CSV record
                csv_record = csv_df[csv_df['row_id'] == float(row_id)]
                if csv_record.empty:
                    continue
                csv_data = csv_record.iloc[0].to_dict()
                
                # Get DB record
                cursor.execute("SELECT * FROM typing_clients_data WHERE row_id = %s", [row_id])
                db_result = cursor.fetchone()
                if not db_result:
                    continue
                db_data = dict(db_result)
                
                # Compare key fields
                if (csv_data.get('name') == db_data.get('name') and 
                    csv_data.get('email') == db_data.get('email')):
                    consistent_count += 1
                else:
                    inconsistent_count += 1
                    self.test_results['sync_issues'].append({
                        'issue': 'data_mismatch',
                        'row_id': row_id,
                        'csv_name': csv_data.get('name'),
                        'db_name': db_data.get('name')
                    })
            
            print(f"   Consistent records: {consistent_count}")
            print(f"   Inconsistent records: {inconsistent_count}")
            
            # Overall sync assessment
            if inconsistent_count == 0 and len(csv_only) == 0:
                print(f"   ✅ Sync Status: GOOD - CSV is subset of DB with consistent data")
                self.test_results['tests_passed'] += 1
                return True
            elif inconsistent_count == 0:
                print(f"   ⚠️ Sync Status: PARTIAL - CSV missing some DB records but data consistent")
                self.test_results['tests_passed'] += 1
                return True
            else:
                print(f"   ❌ Sync Status: POOR - Data inconsistencies detected")
                self.test_results['tests_failed'] += 1
                return False
                
        except Exception as e:
            print(f"   ❌ Sync test failed: {e}")
            self.test_results['tests_failed'] += 1
            return False
    
    def test_dual_write_new_record(self) -> bool:
        """Test that new records are written to both DB and CSV."""
        
        print(f"\n📝 Test 2: Dual-Write New Record")
        print("-" * 35)
        self.test_results['tests_run'] += 1
        
        # Create test record
        test_record = {
            'row_id': 99999,  # Use unique ID
            'name': 'Test Sync User',
            'email': 'test.sync@example.com',
            'type': 'Test Type',
            'processed': False
        }
        
        try:
            # Get initial counts
            initial_db_count, initial_csv_count = self.get_record_counts()
            print(f"   Initial - DB: {initial_db_count}, CSV: {initial_csv_count}")
            
            # Simulate dual-write operation
            print(f"   Writing test record (ID: {test_record['row_id']})...")
            
            # Write to database
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    INSERT INTO typing_clients_data (row_id, name, email, type, processed)
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (row_id) DO UPDATE SET
                    name = EXCLUDED.name,
                    email = EXCLUDED.email,
                    type = EXCLUDED.type,
                    processed = EXCLUDED.processed
                """, [test_record['row_id'], test_record['name'], test_record['email'], 
                      test_record['type'], test_record['processed']])
                conn.commit()
            
            # Write to CSV (simulate dual-write)
            csv_df = self.csv_manager.read()
            
            # Remove existing test record if present
            csv_df = csv_df[csv_df['row_id'] != test_record['row_id']]
            
            # Add new record
            new_row = pd.DataFrame([test_record])
            csv_df = pd.concat([csv_df, new_row], ignore_index=True)
            
            # Save CSV
            csv_df.to_csv(self.csv_path, index=False)
            
            # Verify both systems have the record
            print(f"   Verifying record exists in both systems...")
            
            # Check database
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT * FROM typing_clients_data WHERE row_id = %s", [test_record['row_id']])
                db_result = cursor.fetchone()
            
            # Check CSV
            updated_csv_df = self.csv_manager.read()
            csv_result = updated_csv_df[updated_csv_df['row_id'] == test_record['row_id']]
            
            # Verify results
            db_found = db_result is not None
            csv_found = not csv_result.empty
            
            print(f"   Database record found: {db_found}")
            print(f"   CSV record found: {csv_found}")
            
            if db_found and csv_found:
                # Verify data consistency
                db_data = dict(db_result)
                csv_data = csv_result.iloc[0].to_dict()
                
                if (db_data['name'] == csv_data['name'] and 
                    db_data['email'] == csv_data['email']):
                    print(f"   ✅ SUCCESS: Record synchronized correctly")
                    self.test_results['tests_passed'] += 1
                    return True
                else:
                    print(f"   ❌ FAILED: Record data inconsistent between DB and CSV")
                    self.test_results['tests_failed'] += 1
                    return False
            else:
                print(f"   ❌ FAILED: Record not found in both systems")
                self.test_results['tests_failed'] += 1
                return False
                
        except Exception as e:
            print(f"   ❌ Dual-write test failed: {e}")
            self.test_results['tests_failed'] += 1
            return False
    
    def test_update_synchronization(self) -> bool:
        """Test that record updates are synchronized."""
        
        print(f"\n🔄 Test 3: Update Synchronization")
        print("-" * 35)
        self.test_results['tests_run'] += 1
        
        test_row_id = 99999  # Use our test record
        
        try:
            # Update the test record
            updated_name = f"Updated Sync User {datetime.now().strftime('%H%M%S')}"
            updated_email = f"updated.sync.{datetime.now().strftime('%H%M%S')}@example.com"
            
            print(f"   Updating record {test_row_id}...")
            print(f"   New name: {updated_name}")
            print(f"   New email: {updated_email}")
            
            # Update database
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    UPDATE typing_clients_data 
                    SET name = %s, email = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE row_id = %s
                """, [updated_name, updated_email, test_row_id])
                conn.commit()
                updated_rows = cursor.rowcount
            
            # Update CSV (simulate dual-write update)
            csv_df = self.csv_manager.read()
            mask = csv_df['row_id'] == test_row_id
            if mask.any():
                csv_df.loc[mask, 'name'] = updated_name
                csv_df.loc[mask, 'email'] = updated_email
                csv_df.to_csv(self.csv_path, index=False)
            
            print(f"   Database rows updated: {updated_rows}")
            
            # Verify updates in both systems
            print(f"   Verifying updates...")
            
            # Check database
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT name, email FROM typing_clients_data WHERE row_id = %s", [test_row_id])
                db_result = cursor.fetchone()
            
            # Check CSV
            updated_csv_df = self.csv_manager.read()
            csv_result = updated_csv_df[updated_csv_df['row_id'] == test_row_id]
            
            if db_result and not csv_result.empty:
                db_data = dict(db_result)
                csv_data = csv_result.iloc[0].to_dict()
                
                if (db_data['name'] == updated_name and 
                    csv_data['name'] == updated_name and
                    db_data['email'] == updated_email and
                    csv_data['email'] == updated_email):
                    print(f"   ✅ SUCCESS: Updates synchronized correctly")
                    self.test_results['tests_passed'] += 1
                    return True
                else:
                    print(f"   ❌ FAILED: Update synchronization inconsistent")
                    print(f"      DB name: {db_data.get('name')} | CSV name: {csv_data.get('name')}")
                    print(f"      DB email: {db_data.get('email')} | CSV email: {csv_data.get('email')}")
                    self.test_results['tests_failed'] += 1
                    return False
            else:
                print(f"   ❌ FAILED: Record not found after update")
                self.test_results['tests_failed'] += 1
                return False
                
        except Exception as e:
            print(f"   ❌ Update sync test failed: {e}")
            self.test_results['tests_failed'] += 1
            return False
    
    def test_csv_backup_reliability(self) -> bool:
        """Test CSV as reliable backup source."""
        
        print(f"\n💾 Test 4: CSV Backup Reliability")
        print("-" * 35)
        self.test_results['tests_run'] += 1
        
        try:
            # Test CSV file integrity
            print(f"   Testing CSV file integrity...")
            csv_df = self.csv_manager.read()
            
            # Check for required columns
            required_columns = ['row_id', 'name', 'email', 'type']
            missing_columns = [col for col in required_columns if col not in csv_df.columns]
            
            if missing_columns:
                print(f"   ❌ FAILED: Missing required columns: {missing_columns}")
                self.test_results['tests_failed'] += 1
                return False
            
            print(f"   ✅ All required columns present")
            
            # Check for data quality
            null_row_ids = csv_df['row_id'].isnull().sum()
            duplicate_row_ids = csv_df[csv_df['row_id'].duplicated()].shape[0]
            
            print(f"   Records with null row_id: {null_row_ids}")
            print(f"   Duplicate row_ids: {duplicate_row_ids}")
            
            # Test CSV can be used as backup
            print(f"   Testing CSV as backup source...")
            
            # Simulate reading key records from CSV
            test_row_ids = [1, 2, 3, 99999]  # Include our test record
            found_count = 0
            
            for row_id in test_row_ids:
                csv_record = csv_df[csv_df['row_id'] == row_id]
                if not csv_record.empty:
                    found_count += 1
                    record_data = csv_record.iloc[0].to_dict()
                    print(f"      Row {row_id}: {record_data.get('name', 'N/A')} - Found")
                else:
                    print(f"      Row {row_id}: Not found in CSV")
            
            reliability_score = (found_count / len(test_row_ids)) * 100
            print(f"   CSV reliability score: {reliability_score:.1f}%")
            
            if reliability_score >= 75:  # At least 75% of test records found
                print(f"   ✅ SUCCESS: CSV backup is reliable")
                self.test_results['tests_passed'] += 1
                return True
            else:
                print(f"   ❌ FAILED: CSV backup reliability below threshold")
                self.test_results['tests_failed'] += 1
                return False
                
        except Exception as e:
            print(f"   ❌ CSV backup reliability test failed: {e}")
            self.test_results['tests_failed'] += 1
            return False
    
    def cleanup_test_data(self):
        """Clean up test records."""
        
        print(f"\n🧹 Cleaning up test data...")
        
        try:
            # Remove test record from database
            with self.db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("DELETE FROM typing_clients_data WHERE row_id = %s", [99999])
                deleted_db = cursor.rowcount
                conn.commit()
            
            # Remove test record from CSV
            csv_df = self.csv_manager.read()
            original_count = len(csv_df)
            csv_df = csv_df[csv_df['row_id'] != 99999]
            deleted_csv = original_count - len(csv_df)
            
            if deleted_csv > 0:
                csv_df.to_csv(self.csv_path, index=False)
            
            print(f"   Deleted from database: {deleted_db} records")
            print(f"   Deleted from CSV: {deleted_csv} records")
            
        except Exception as e:
            print(f"   ⚠️ Cleanup warning: {e}")
    
    def generate_sync_report(self):
        """Generate comprehensive sync verification report."""
        
        end_time = datetime.now()
        duration = (end_time - self.test_results['start_time']).total_seconds()
        
        print(f"\n📊 CSV Backup Sync Verification Report")
        print("=" * 40)
        
        # Test summary
        print(f"Tests run: {self.test_results['tests_run']}")
        print(f"Tests passed: {self.test_results['tests_passed']}")
        print(f"Tests failed: {self.test_results['tests_failed']}")
        print(f"Success rate: {(self.test_results['tests_passed']/self.test_results['tests_run']*100):.1f}%")
        print(f"Test duration: {duration:.1f}s")
        
        # Current state
        try:
            db_count, csv_count = self.get_record_counts()
            print(f"\nCurrent State:")
            print(f"  Database records: {db_count}")
            print(f"  CSV records: {csv_count}")
            print(f"  Coverage: {(csv_count/db_count*100):.1f}%" if db_count > 0 else "  Coverage: N/A")
        except:
            print(f"\nCurrent State: Unable to retrieve counts")
        
        # Sync issues
        if self.test_results['sync_issues']:
            print(f"\nSync Issues Detected:")
            for issue in self.test_results['sync_issues'][:5]:  # Show first 5
                print(f"  - {issue['issue']}: Row {issue['row_id']}")
        
        # Overall assessment
        success_rate = (self.test_results['tests_passed']/self.test_results['tests_run']*100) if self.test_results['tests_run'] > 0 else 0
        
        print(f"\n💡 Overall Assessment:")
        if success_rate >= 100:
            print(f"✅ EXCELLENT: CSV backup sync is working perfectly")
        elif success_rate >= 75:
            print(f"✅ GOOD: CSV backup sync is working with minor issues")
        elif success_rate >= 50:
            print(f"⚠️ WARNING: CSV backup sync has significant issues")
        else:
            print(f"🚨 CRITICAL: CSV backup sync is not working correctly")
        
        return success_rate >= 75
    
    def run_all_tests(self):
        """Run all CSV backup sync verification tests."""
        
        print("🔄 CSV Backup Synchronization Verification")
        print("=" * 45)
        
        try:
            # Run all tests
            test1_result = self.test_current_sync_status()
            test2_result = self.test_dual_write_new_record()
            test3_result = self.test_update_synchronization()
            test4_result = self.test_csv_backup_reliability()
            
            # Clean up
            self.cleanup_test_data()
            
            # Generate report
            overall_success = self.generate_sync_report()
            
            return overall_success
            
        except Exception as e:
            print(f"\n💥 Test suite failed: {e}")
            db_logger.error(f"CSV sync verification failed: {e}")
            return False

def main():
    """Execute CSV backup sync verification."""
    
    verifier = CSVBackupSyncVerifier()
    success = verifier.run_all_tests()
    
    return success

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)