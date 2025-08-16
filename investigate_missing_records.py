#!/usr/bin/env python3
"""
Investigate missing records between CSV and database
"""

import os
import sys
import pandas as pd

# Set up environment
os.environ['DB_PASSWORD'] = 'migration_pass_2025'
sys.path.append('.')

from utils.database_operations import DatabaseConfig, DatabaseManager

def investigate_missing_records():
    """Find which records are missing from database."""
    
    print("🔍 Investigating Missing Records")
    print("="*35)
    
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
    
    # Load CSV data
    csv_df = pd.read_csv("outputs/output.csv")
    csv_row_ids = set(csv_df['row_id'].tolist())
    print(f"📊 CSV row_ids: {len(csv_row_ids)} records")
    print(f"   Range: {min(csv_row_ids)} to {max(csv_row_ids)}")
    
    # Load database data
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("SELECT row_id FROM typing_clients_data ORDER BY row_id")
        db_records = cursor.fetchall()
        db_row_ids = set(record['row_id'] for record in db_records)
        
        print(f"🗄️ Database row_ids: {len(db_row_ids)} records")
        print(f"   Range: {min(db_row_ids)} to {max(db_row_ids)}")
        
        # Find missing records
        missing_from_db = csv_row_ids - db_row_ids
        extra_in_db = db_row_ids - csv_row_ids
        
        print(f"\n❌ Missing from database: {len(missing_from_db)} records")
        if missing_from_db:
            missing_list = sorted(list(missing_from_db))
            print(f"   Missing row_ids: {missing_list}")
            
            # Get details of missing records
            for missing_id in missing_list[:5]:  # Show first 5
                missing_record = csv_df[csv_df['row_id'] == missing_id].iloc[0]
                print(f"   - Row {missing_id}: {missing_record['name']} ({missing_record['email']})")
        
        print(f"\n➕ Extra in database: {len(extra_in_db)} records")
        if extra_in_db:
            extra_list = sorted(list(extra_in_db))
            print(f"   Extra row_ids: {extra_list}")
        
        # Check for duplicates in CSV
        csv_duplicates = csv_df[csv_df.duplicated(subset=['row_id'], keep=False)]
        if not csv_duplicates.empty:
            print(f"\n🔄 CSV has {len(csv_duplicates)} duplicate row_ids:")
            for _, dup in csv_duplicates.iterrows():
                print(f"   - Row {dup['row_id']}: {dup['name']}")
        
        # Check for duplicates in database
        cursor.execute("""
            SELECT row_id, COUNT(*) as count 
            FROM typing_clients_data 
            GROUP BY row_id 
            HAVING COUNT(*) > 1
        """)
        db_duplicates = cursor.fetchall()
        if db_duplicates:
            print(f"\n🔄 Database has {len(db_duplicates)} duplicate row_ids:")
            for dup in db_duplicates:
                print(f"   - Row {dup['row_id']}: {dup['count']} occurrences")
        
        return missing_from_db, extra_in_db

if __name__ == "__main__":
    missing, extra = investigate_missing_records()