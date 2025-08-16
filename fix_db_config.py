#!/usr/bin/env python3
"""Fix database configuration and ensure everything works"""

import os
from utils.database_operations import DatabaseConfig, DatabaseManager, create_table

def fix_and_test_database():
    """Fix database configuration and test connection"""
    
    # Set environment variable
    os.environ['DB_PASSWORD'] = 'migration_pass_2025'
    
    # Create database config with explicit password
    config = DatabaseConfig(
        db_type='postgresql',
        host='localhost',
        port=5432,
        database='typing_clients_uuid',
        username='migration_user',
        password='migration_pass_2025'
    )
    
    print("🔧 Testing PostgreSQL connection...")
    
    try:
        db_manager = DatabaseManager(config)
        
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            result = cursor.fetchone()
            print(f"✅ PostgreSQL connection successful!")
            print(f"📊 Version: {result['version']}")
            
            # Check existing tables
            cursor.execute("""
                SELECT table_name 
                FROM information_schema.tables 
                WHERE table_schema = 'public'
                ORDER BY table_name
            """)
            
            tables = cursor.fetchall()
            print(f"📋 Existing tables ({len(tables)}):")
            for table in tables:
                print(f"  - {table['table_name']}")
            
            # Create our main table if it doesn't exist
            if not any(t['table_name'] == 'typing_clients_data' for t in tables):
                print("🏗️ Creating typing_clients_data table...")
                
                create_sql = """
                CREATE TABLE typing_clients_data (
                    row_id INTEGER PRIMARY KEY,
                    name TEXT NOT NULL,
                    email TEXT,
                    type TEXT,
                    link TEXT,
                    extracted_links TEXT,
                    youtube_playlist TEXT,
                    google_drive TEXT,
                    processed BOOLEAN DEFAULT FALSE,
                    document_text TEXT,
                    youtube_status TEXT,
                    youtube_files TEXT,
                    youtube_media_id TEXT,
                    drive_status TEXT,
                    drive_files TEXT,
                    drive_media_id TEXT,
                    last_download_attempt TIMESTAMP,
                    download_errors TEXT,
                    permanent_failure BOOLEAN DEFAULT FALSE,
                    file_uuids JSONB,
                    s3_paths JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
                """
                
                cursor.execute(create_sql)
                
                # Create indexes
                indexes = [
                    "CREATE INDEX IF NOT EXISTS idx_typing_clients_data_row_id ON typing_clients_data(row_id)",
                    "CREATE INDEX IF NOT EXISTS idx_typing_clients_data_email ON typing_clients_data(email)",
                    "CREATE INDEX IF NOT EXISTS idx_typing_clients_data_processed ON typing_clients_data(processed)"
                ]
                
                for index_sql in indexes:
                    cursor.execute(index_sql)
                
                conn.commit()
                print("✅ Table created successfully!")
            
            # Test insert/select
            cursor.execute("""
                INSERT INTO typing_clients_data (row_id, name, email, type, processed, file_uuids, permanent_failure) 
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (row_id) DO UPDATE SET name = EXCLUDED.name
                RETURNING row_id
            """, [999999, 'Test User', 'test@example.com', 'TEST', True, '{}', False])
            
            inserted_id = cursor.fetchone()['row_id']
            print(f"✅ Insert test successful, ID: {inserted_id}")
            
            # Test select
            cursor.execute("SELECT * FROM typing_clients_data WHERE row_id = %s", [999999])
            result = cursor.fetchone()
            print(f"✅ Select test successful: {result['name']}")
            
            # Clean up
            cursor.execute("DELETE FROM typing_clients_data WHERE row_id = %s", [999999])
            conn.commit()
            print("✅ Cleanup successful")
            
            return True
            
    except Exception as e:
        print(f"❌ Database setup failed: {e}")
        return False

if __name__ == "__main__":
    success = fix_and_test_database()
    if success:
        print("🎉 Database is ready for migration!")
    else:
        print("💥 Database setup failed!")