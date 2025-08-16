#!/usr/bin/env python3
"""
Test single dual-write record with explicit database configuration
"""

import os
import json
import pandas as pd
from pathlib import Path
from datetime import datetime

# Set up environment
os.environ['DB_PASSWORD'] = 'migration_pass_2025'

# Import utilities with path fix
import sys
sys.path.append('.')

from utils.database_operations import DatabaseConfig, DatabaseManager
from utils.data_validation import DataValidator

def test_single_dual_write():
    """Test dual-write with a single record and verify CSV/DB match exactly."""
    
    print("🧪 Testing single record dual-write with validation...")
    
    # Configure database with explicit credentials that work
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
    
    # Test record
    test_record = {
        'row_id': 888888,
        'name': 'Single Dual Write Test',
        'email': 'single.test@example.com',
        'type': 'DUAL_WRITE_TEST',
        'processed': True,
        'file_uuids': {'test': 'single-dual-uuid'},
        'permanent_failure': False,
        'document_text': 'Test document for single dual-write validation'
    }
    
    csv_file = Path("outputs/output.csv")
    backup_csv = csv_file.with_suffix(f".backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
    
    try:
        # Step 1: Create backup of CSV
        if csv_file.exists():
            import shutil
            shutil.copy2(csv_file, backup_csv)
            print(f"📁 CSV backup created: {backup_csv}")
        
        # Step 2: Test database connection and write
        print("🗄️ Testing database write...")
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # Normalize record for database
            normalized_record = validator.normalize_csv_row(test_record)
            
            # Convert JSON fields
            for key in ['file_uuids', 's3_paths']:
                if key in normalized_record and isinstance(normalized_record[key], dict):
                    normalized_record[key] = json.dumps(normalized_record[key])
            
            # Remove metadata fields
            clean_record = {k: v for k, v in normalized_record.items() 
                          if k not in ['created_at', 'updated_at']}
            
            # Build insert query
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
            
            print("✅ Database write successful!")
            
            # Step 3: Test CSV write
            print("📊 Testing CSV write...")
            
            # Normalize for CSV
            csv_record = validator.normalize_csv_row(test_record)
            
            # Handle JSON fields for CSV
            for key in ['file_uuids', 's3_paths']:
                if key in csv_record and isinstance(csv_record[key], dict):
                    csv_record[key] = json.dumps(csv_record[key])
            
            # Write to CSV
            if csv_file.exists():
                df = pd.read_csv(csv_file)
                
                # Check if record exists
                if test_record['row_id'] in df['row_id'].values:
                    # Update existing
                    for key, value in csv_record.items():
                        df.loc[df['row_id'] == test_record['row_id'], key] = value
                    df.to_csv(csv_file, index=False)
                else:
                    # Append new
                    new_df = pd.concat([df, pd.DataFrame([csv_record])], ignore_index=True)
                    new_df.to_csv(csv_file, index=False)
            else:
                # Create new file
                df = pd.DataFrame([csv_record])
                df.to_csv(csv_file, index=False)
            
            print("✅ CSV write successful!")
            
            # Step 4: Validate both records match exactly
            print("🔍 Validating CSV and DB records match...")
            
            # Get from database
            cursor.execute("SELECT * FROM typing_clients_data WHERE row_id = %s", [test_record['row_id']])
            db_result = cursor.fetchone()
            
            if not db_result:
                raise RuntimeError("Record not found in database after insert")
            
            # Get from CSV
            df = pd.read_csv(csv_file)
            csv_matches = df[df['row_id'] == test_record['row_id']]
            
            if csv_matches.empty:
                raise RuntimeError("Record not found in CSV after write")
            
            csv_result = csv_matches.iloc[0].to_dict()
            
            print(f"📊 Database record: {db_result['name']}, processed: {db_result['processed']}")
            print(f"📄 CSV record: {csv_result['name']}, processed: {csv_result['processed']}")
            
            # Validate using data validator
            try:
                validator.validate_dual_write(csv_result, db_result, test_record['row_id'])
                print("✅ VALIDATION PASSED: CSV and DB records match exactly!")
                
                # Clean up test record
                cursor.execute("DELETE FROM typing_clients_data WHERE row_id = %s", [test_record['row_id']])
                
                # Remove from CSV
                df = pd.read_csv(csv_file)
                df = df[df['row_id'] != test_record['row_id']]
                df.to_csv(csv_file, index=False)
                
                conn.commit()
                print("🧹 Test cleanup completed")
                
                return True
                
            except Exception as validation_error:
                print(f"❌ VALIDATION FAILED: {validation_error}")
                print("🔍 Detailed comparison:")
                print(f"   CSV: {csv_result}")
                print(f"   DB:  {dict(db_result)}")
                return False
            
    except Exception as e:
        print(f"❌ Dual-write test failed: {e}")
        return False
    
    finally:
        # Restore backup if something went wrong
        try:
            if backup_csv.exists() and not csv_file.exists():
                import shutil
                shutil.move(backup_csv, csv_file)
                print(f"🔄 Restored CSV from backup")
        except:
            pass

if __name__ == "__main__":
    success = test_single_dual_write()
    if success:
        print("🎉 Single dual-write test PASSED - ready to proceed!")
    else:
        print("💥 Single dual-write test FAILED - need to fix issues before proceeding")