#!/usr/bin/env python3
"""
Import CSV data using direct database connection with explicit configuration
"""

import os
import json
import pandas as pd
from datetime import datetime
from typing import Dict, Any

# Set up environment
os.environ['DB_PASSWORD'] = 'migration_pass_2025'

import sys
sys.path.append('.')

from utils.database_operations import DatabaseConfig, DatabaseManager
from utils.data_validation import DataValidator
from utils.database_logging import db_logger

def import_csv_direct(csv_file: str = "outputs/output.csv"):
    """Import CSV data with explicit database configuration."""
    
    print("🚀 Starting Direct CSV Import with Explicit Database Config")
    print("="*65)
    
    # Configure database with working credentials
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
        # Load CSV data
        print(f"📁 Loading CSV file: {csv_file}")
        df = pd.read_csv(csv_file)
        total_records = len(df)
        print(f"📊 Found {total_records} records to import")
        
        # Connect to database
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # Check current database state
            cursor.execute("SELECT COUNT(*) as count FROM typing_clients_data")
            initial_count = cursor.fetchone()['count']
            print(f"🗄️ Database currently has {initial_count} records")
            
            # Import records
            successful = 0
            failed = 0
            batch_size = 50
            
            for i in range(0, total_records, batch_size):
                batch_end = min(i + batch_size, total_records)
                batch = df.iloc[i:batch_end]
                batch_num = (i // batch_size) + 1
                
                print(f"\n📦 Processing batch {batch_num}: records {i+1}-{batch_end}")
                
                for idx, row in batch.iterrows():
                    try:
                        # Convert row to dictionary
                        record = row.to_dict()
                        
                        # Normalize the record
                        normalized_record = validator.normalize_csv_row(record)
                        
                        # Clean up None values and metadata
                        clean_record = {k: v for k, v in normalized_record.items() 
                                      if v is not None or k in ['document_text', 'download_errors']}
                        
                        # Remove metadata fields
                        for field in ['created_at', 'updated_at']:
                            if field in clean_record:
                                del clean_record[field]
                        
                        # Convert JSON fields
                        for key in ['file_uuids', 's3_paths']:
                            if key in clean_record and isinstance(clean_record[key], dict):
                                clean_record[key] = json.dumps(clean_record[key])
                        
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
                        successful += 1
                        
                        if successful % 100 == 0:
                            print(f"   ✅ Imported {successful} records...")
                        
                    except Exception as e:
                        failed += 1
                        row_id = record.get('row_id', 'unknown')
                        print(f"   ❌ Failed to import record {row_id}: {e}")
                        
                        # Fail fast if too many errors
                        if failed > 50:  # More than 50 failures
                            raise RuntimeError(f"Too many failures: {failed} records failed")
                
                # Commit batch
                conn.commit()
                print(f"   📊 Batch {batch_num}: {successful - failed} successful, {failed} failed")
            
            # Final statistics
            cursor.execute("SELECT COUNT(*) as count FROM typing_clients_data")
            final_count = cursor.fetchone()['count']
            imported_count = final_count - initial_count
            
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            print(f"\n📊 IMPORT COMPLETED")
            print("="*30)
            print(f"Total CSV records: {total_records}")
            print(f"Successful imports: {successful}")
            print(f"Failed imports: {failed}")
            print(f"Success rate: {successful/total_records*100:.1f}%")
            print(f"Database records before: {initial_count}")
            print(f"Database records after: {final_count}")
            print(f"Net imported: {imported_count}")
            print(f"Duration: {duration:.1f} seconds")
            print(f"Import rate: {successful/duration:.1f} records/sec")
            
            if failed == 0:
                print("\n🎉 All records imported successfully!")
                
                # Run validation sample
                print("\n🔍 Running validation sample...")
                cursor.execute("SELECT * FROM typing_clients_data ORDER BY created_at DESC LIMIT 10")
                sample_records = cursor.fetchall()
                
                print(f"✅ Sample validation: {len(sample_records)} recent records found")
                for record in sample_records[:3]:
                    print(f"   - ID {record['row_id']}: {record['name']}")
                
                return True
            else:
                failure_rate = failed / total_records * 100
                if failure_rate > 5:
                    print(f"\n❌ High failure rate: {failure_rate:.1f}%")
                    return False
                else:
                    print(f"\n⚠️ Acceptable failure rate: {failure_rate:.1f}%")
                    return True
            
    except Exception as e:
        print(f"\n💥 Import failed: {e}")
        db_logger.error(f"Direct CSV import failed: {e}")
        return False

if __name__ == "__main__":
    success = import_csv_direct()
    if success:
        print("\n✅ CSV import completed - ready for next phase!")
        exit(0)
    else:
        print("\n❌ CSV import failed - check errors and retry")
        exit(1)