#!/usr/bin/env python3
"""Direct database connection test"""

import os
from utils.database_operations import DatabaseConfig, DatabaseManager

# Direct connection with explicit password
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
        print(f"✅ PostgreSQL connection successful!")
        print(f"📊 Record count: {result['count']}")
        
        # Test insert
        cursor.execute("""
            INSERT INTO typing_clients_data (row_id, name, email, type) 
            VALUES (%s, %s, %s, %s)
            RETURNING row_id
        """, [999999, 'Test User', 'test@example.com', 'TEST'])
        
        inserted_id = cursor.fetchone()['row_id']
        print(f"✅ Insert successful, ID: {inserted_id}")
        
        # Clean up
        cursor.execute("DELETE FROM typing_clients_data WHERE row_id = %s", [999999])
        print("✅ Cleanup successful")
        
        conn.commit()
        
except Exception as e:
    print(f"❌ Database test failed: {e}")