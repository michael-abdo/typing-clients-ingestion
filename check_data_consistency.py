#!/usr/bin/env python3
"""
Comprehensive Data Consistency Check between CSV and Database

Validates that every record in CSV matches exactly with database.
Fails fast on any mismatch to ensure data integrity.
"""

import os
import sys
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Tuple

# Set up environment
os.environ['DB_PASSWORD'] = 'migration_pass_2025'
sys.path.append('.')

from utils.database_operations import DatabaseConfig, DatabaseManager
from utils.data_validation import DataValidator
from utils.database_logging import db_logger

def check_data_consistency(csv_file: str = "outputs/output.csv"):
    """
    Comprehensive data consistency check between CSV and database.
    
    Returns:
        True if all data matches exactly, False or raises exception if mismatches found
    """
    
    print("🔍 Starting Comprehensive Data Consistency Check")
    print("="*55)
    
    # Configure database
    config = DatabaseConfig(
        db_type='postgresql',
        host='localhost',
        port=5432,
        database='typing_clients_uuid',
        username='migration_user',
        password='migration_pass_2025'
    )
    
    db_manager = DatabaseManager(config)
    validator = DataValidator()
    
    start_time = datetime.now()
    
    try:
        # Step 1: Load CSV data
        print(f"📁 Loading CSV file: {csv_file}")
        csv_df = pd.read_csv(csv_file)
        csv_count = len(csv_df)
        print(f"📊 CSV contains {csv_count} records")
        
        # Step 2: Load database data
        print(f"🗄️ Loading database data...")
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get total count
            cursor.execute("SELECT COUNT(*) as count FROM typing_clients_data")
            db_count = cursor.fetchone()['count']
            print(f"📊 Database contains {db_count} records")
            
            # Check counts match
            if csv_count != db_count:
                print(f"❌ CRITICAL: Count mismatch! CSV: {csv_count}, DB: {db_count}")
                return False
            
            # Load all database records
            cursor.execute("SELECT * FROM typing_clients_data ORDER BY row_id")
            db_records = cursor.fetchall()
            
            # Convert to dictionary keyed by row_id
            db_dict = {record['row_id']: record for record in db_records}
            
        print(f"✅ Record counts match: {csv_count} records")
        
        # Step 3: Compare every record
        print(f"\n🔍 Comparing {csv_count} records field-by-field...")
        
        mismatches = []
        successful_validations = 0
        
        for idx, csv_row in csv_df.iterrows():
            try:
                row_id = csv_row['row_id']
                
                # Check if record exists in database
                if row_id not in db_dict:
                    mismatch = f"Row {row_id}: Missing from database"
                    mismatches.append(mismatch)
                    print(f"❌ {mismatch}")
                    continue
                
                # Get corresponding database record
                db_record = db_dict[row_id]
                csv_record = csv_row.to_dict()
                
                # Validate using our comprehensive validator
                try:
                    validator.validate_dual_write(csv_record, db_record, row_id)
                    successful_validations += 1
                    
                    # Progress indicator
                    if successful_validations % 100 == 0:
                        print(f"   ✅ Validated {successful_validations} records...")
                        
                except Exception as validation_error:
                    mismatch = f"Row {row_id}: {validation_error}"
                    mismatches.append(mismatch)
                    print(f"❌ {mismatch}")
                    
                    # Show detailed comparison for first few errors
                    if len(mismatches) <= 3:
                        print(f"   📊 CSV: {csv_record}")
                        print(f"   🗄️ DB:  {dict(db_record)}")
                    
                    # Fail fast if too many errors
                    if len(mismatches) >= 10:
                        print(f"\n💥 FAIL FAST: Too many mismatches ({len(mismatches)})")
                        print("Stopping validation to prevent log spam")
                        break
                        
            except Exception as e:
                mismatch = f"Row {row_id}: Validation error - {e}"
                mismatches.append(mismatch)
                print(f"❌ {mismatch}")
        
        # Step 4: Check for orphaned database records
        print(f"\n🔍 Checking for orphaned database records...")
        csv_row_ids = set(csv_df['row_id'].tolist())
        db_row_ids = set(db_dict.keys())
        
        orphaned_db_records = db_row_ids - csv_row_ids
        orphaned_csv_records = csv_row_ids - db_row_ids
        
        if orphaned_db_records:
            print(f"❌ Found {len(orphaned_db_records)} orphaned database records: {list(orphaned_db_records)[:5]}...")
            for orphan_id in list(orphaned_db_records)[:3]:
                mismatches.append(f"Orphaned DB record: {orphan_id}")
        
        if orphaned_csv_records:
            print(f"❌ Found {len(orphaned_csv_records)} orphaned CSV records: {list(orphaned_csv_records)[:5]}...")
            for orphan_id in list(orphaned_csv_records)[:3]:
                mismatches.append(f"Orphaned CSV record: {orphan_id}")
        
        # Step 5: Summary and results
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        
        print(f"\n📊 DATA CONSISTENCY CHECK RESULTS")
        print("="*45)
        print(f"Total records checked: {csv_count}")
        print(f"Successful validations: {successful_validations}")
        print(f"Mismatches found: {len(mismatches)}")
        print(f"Orphaned DB records: {len(orphaned_db_records)}")
        print(f"Orphaned CSV records: {len(orphaned_csv_records)}")
        print(f"Check duration: {duration:.1f} seconds")
        print(f"Validation rate: {successful_validations/duration:.1f} records/sec")
        
        if mismatches:
            print(f"\n❌ DATA CONSISTENCY CHECK FAILED")
            print(f"Found {len(mismatches)} data integrity issues:")
            for i, mismatch in enumerate(mismatches[:10], 1):
                print(f"  {i}. {mismatch}")
            if len(mismatches) > 10:
                print(f"  ... and {len(mismatches) - 10} more issues")
            
            print(f"\n💥 CRITICAL: Data migration has integrity issues!")
            print(f"Action required: Review and fix data before proceeding")
            return False
        else:
            print(f"\n🎉 DATA CONSISTENCY CHECK PASSED")
            print(f"✅ All {csv_count} records match exactly between CSV and database")
            print(f"✅ No orphaned records found")
            print(f"✅ Data migration integrity verified")
            
            # Sample record validation
            print(f"\n🔍 Sample record validation:")
            sample_records = list(db_dict.items())[:3]
            for row_id, db_record in sample_records:
                print(f"   Row {row_id}: {db_record['name']} - {db_record['email']}")
            
            return True
            
    except Exception as e:
        print(f"\n💥 Consistency check failed with error: {e}")
        db_logger.error(f"Data consistency check failed: {e}")
        return False

def main():
    """Run the data consistency check."""
    
    print("🚀 Database Migration Data Consistency Validation")
    print("="*50)
    
    success = check_data_consistency()
    
    if success:
        print("\n✅ Data consistency check PASSED - migration integrity verified!")
        print("Ready to proceed with dual-write testing")
        return True
    else:
        print("\n❌ Data consistency check FAILED - critical integrity issues found!")
        print("Migration must be reviewed and fixed before proceeding")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)