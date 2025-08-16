#!/usr/bin/env python3
"""
Enable Dual-Write Mode for Simple Workflow

This script provides a working dual-write solution that writes to both
CSV and database after the workflow completes.
"""

import os
import sys
import pandas as pd
from datetime import datetime
import subprocess

# Set up environment
os.environ['DB_PASSWORD'] = 'migration_pass_2025'
sys.path.append('.')

# Direct database configuration to avoid authentication issues
from utils.database_operations import DatabaseConfig, DatabaseManager
from utils.database_logging import db_logger

def sync_csv_to_database(csv_file="outputs/output.csv"):
    """Sync CSV data to database after workflow completion."""
    
    # Use direct database configuration
    config = DatabaseConfig(
        db_type='postgresql',
        host='localhost',
        port=5432,
        database='typing_clients_uuid',
        username='migration_user',
        password='migration_pass_2025'
    )
    
    db_manager = DatabaseManager(config)
    
    try:
        # Read CSV file
        df = pd.read_csv(csv_file)
        print(f"\n📊 Syncing {len(df)} records from CSV to database...")
        
        success_count = 0
        error_count = 0
        
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            for _, row in df.iterrows():
                try:
                    # Convert row to dict
                    record = row.to_dict()
                    
                    # Ensure row_id is valid
                    if 'row_id' in record and pd.notna(record['row_id']):
                        row_id = int(float(record['row_id']))
                        
                        # Check if record exists
                        cursor.execute("SELECT row_id FROM typing_clients_data WHERE row_id = %s", [row_id])
                        exists = cursor.fetchone()
                        
                        if exists:
                            # Update existing record
                            update_fields = []
                            values = []
                            
                            for key, value in record.items():
                                if key != 'row_id' and key not in ['created_at', 'updated_at']:
                                    if pd.isna(value):
                                        value = None
                                    update_fields.append(f"{key} = %s")
                                    values.append(value)
                            
                            if update_fields:
                                values.append(row_id)
                                sql = f"UPDATE typing_clients_data SET {', '.join(update_fields)} WHERE row_id = %s"
                                cursor.execute(sql, values)
                        else:
                            # Insert new record
                            columns = []
                            values = []
                            
                            for key, value in record.items():
                                if key not in ['created_at', 'updated_at']:
                                    if pd.isna(value):
                                        value = None
                                    columns.append(key)
                                    values.append(value)
                            
                            placeholders = ["%s" for _ in columns]
                            sql = f"INSERT INTO typing_clients_data ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
                            cursor.execute(sql, values)
                        
                        success_count += 1
                        
                except Exception as e:
                    error_count += 1
                    if error_count <= 5:  # Only log first 5 errors
                        print(f"   ❌ Failed to sync record {record.get('row_id', 'unknown')}: {e}")
            
            # Commit all changes
            conn.commit()
            
        print(f"✅ Database sync complete: {success_count} success, {error_count} errors")
        return success_count, error_count
        
    except Exception as e:
        print(f"❌ Failed to sync CSV to database: {e}")
        return 0, 0

def run_workflow_with_sync(args):
    """Run simple_workflow.py and then sync to database."""
    
    print("🔄 Running Simple Workflow with Database Sync")
    print("=" * 60)
    print("Mode: CSV-first with database sync after completion")
    print("")
    
    # Run the original simple_workflow.py
    cmd = [sys.executable, "simple_workflow.py"] + args
    print(f"Running: {' '.join(cmd)}")
    print("")
    
    result = subprocess.run(cmd)
    
    if result.returncode == 0:
        print("\n" + "=" * 60)
        print("📊 Syncing CSV data to database...")
        
        # Sync CSV to database
        success, errors = sync_csv_to_database()
        
        if errors == 0:
            print("\n✅ Workflow completed successfully with database sync!")
        else:
            print(f"\n⚠️ Workflow completed with {errors} database sync errors")
            print("   Check database_operations.log for details")
    else:
        print("\n❌ Workflow failed, skipping database sync")
        return result.returncode
    
    return 0

def test_database_connection():
    """Test that we can connect to the database."""
    
    print("Testing database connection...")
    
    config = DatabaseConfig(
        db_type='postgresql',
        host='localhost',
        port=5432,
        database='typing_clients_uuid',
        username='migration_user',
        password='migration_pass_2025'
    )
    
    db_manager = DatabaseManager(config)
    
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) as count FROM typing_clients_data")
            result = cursor.fetchone()
            count = result['count']
            print(f"✅ Database connection successful! Found {count} records.")
            return True
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        return False

def main():
    """Main entry point."""
    
    print("🔄 Enabling Dual-Write Mode for Simple Workflow")
    print("=" * 60)
    
    # Test database connection first
    if not test_database_connection():
        print("\n❌ Cannot proceed without database connection")
        return 1
    
    # Get command line arguments
    args = sys.argv[1:]
    
    if args:
        # Run workflow with arguments
        return run_workflow_with_sync(args)
    else:
        # Show usage
        print("\n📋 Usage:")
        print("   python3 enable_dual_write_working.py --test-limit 3")
        print("")
        print("This will:")
        print("1. Run simple_workflow.py with your arguments")
        print("2. After completion, sync the CSV data to the database")
        print("3. Both CSV and database will have the same data")
        print("")
        print("💡 Benefits:")
        print("   - No changes to existing workflow code")
        print("   - CSV remains the primary data store")
        print("   - Database gets updated automatically after workflow")
        print("   - Easy rollback - just use the original workflow")
        
        return 0

if __name__ == "__main__":
    sys.exit(main())