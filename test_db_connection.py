#!/usr/bin/env python3
"""Test database connection for migration verification."""

import os
import sys
from utils.database_operations import DatabaseConfig, DatabaseManager

def test_db_connection():
    """Test database connection with fail-fast approach."""
    try:
        # Create database config from config.yaml
        config = DatabaseConfig(
            db_type='postgresql',
            host='localhost',
            port=5432,
            database='typing_clients_uuid',
            username='migration_user',
            password=os.environ.get('DB_PASSWORD', '')
        )
        
        # Test connection
        db_manager = DatabaseManager(config)
        
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT version()")
            result = cursor.fetchone()
            print(f"✅ PostgreSQL connection successful: {result}")
            return True
            
    except ImportError as e:
        print(f"❌ Missing dependency: {e}")
        print("Install with: pip install psycopg2-binary")
        return False
        
    except Exception as e:
        print(f"❌ Database connection failed: {e}")
        print("Possible issues:")
        print("1. PostgreSQL not running")
        print("2. Database 'typing_clients_uuid' doesn't exist")
        print("3. User 'migration_user' doesn't exist or lacks permissions")
        print("4. DB_PASSWORD environment variable not set")
        return False

if __name__ == "__main__":
    success = test_db_connection()
    sys.exit(0 if success else 1)