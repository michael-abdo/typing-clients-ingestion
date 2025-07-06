#!/usr/bin/env python3
"""
Database Connection Module - Fail FAST validation
Provides centralized database connection with immediate failure on errors
Following CLAUDE.md principles: Fail FAST, Determine Root Cause, DRY
"""

import sys
import traceback
from typing import Optional
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine
from sqlalchemy.exc import SQLAlchemyError

# Import existing config (DRY)
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from utils.config import get_config


class DatabaseConnectionError(Exception):
    """Custom exception for database connection failures"""
    pass


def validate_db_config() -> dict:
    """
    Fail FAST: Validate database configuration
    Returns validated config or aborts with clear error
    """
    try:
        config = get_config()
        
        # Required fields for database connection
        required_fields = {
            'database.driver': 'Database driver (e.g., postgresql)',
            'database.host': 'Database host address',
            'database.port': 'Database port number',
            'database.database': 'Database name',
            'database.username': 'Database username',
            'database.password': 'Database password'
        }
        
        db_config = {}
        missing_fields = []
        
        for field, description in required_fields.items():
            value = config.get(field)
            if value is None:
                missing_fields.append(f"{field} ({description})")
            else:
                # Extract field name for db_config
                key = field.split('.')[-1]
                db_config[key] = value
        
        if missing_fields:
            error_msg = "Missing required database configuration fields:\n"
            for field in missing_fields:
                error_msg += f"  - {field}\n"
            error_msg += "\nPlease update config/config.yaml with database settings"
            raise DatabaseConnectionError(error_msg)
        
        return db_config
        
    except Exception as e:
        # Determine root cause and report
        print(f"❌ FAIL FAST: Database configuration validation failed")
        print(f"Root cause: {str(e)}")
        if not isinstance(e, DatabaseConnectionError):
            traceback.print_exc()
        sys.exit(1)


def get_database_engine(fail_on_error: bool = True) -> Optional[Engine]:
    """
    Get database engine with connection pooling
    
    Args:
        fail_on_error: If True, exits on connection failure (Fail FAST)
                      If False, returns None on failure (for optional DB features)
    
    Returns:
        SQLAlchemy Engine or None if connection fails and fail_on_error=False
    """
    try:
        # Validate configuration first
        db_config = validate_db_config()
        
        # Build connection URL
        connection_url = (
            f"{db_config['driver']}://"
            f"{db_config['username']}:{db_config['password']}"
            f"@{db_config['host']}:{db_config['port']}"
            f"/{db_config['database']}"
        )
        
        # Get pool settings from config
        config = get_config()
        pool_config = {
            'pool_size': config.get('database.pool_size', 5),
            'max_overflow': config.get('database.max_overflow', 10),
            'pool_timeout': config.get('database.pool_timeout', 30),
            'pool_recycle': config.get('database.pool_recycle', 3600),
            'pool_pre_ping': True  # Test connections before using
        }
        
        # Create engine
        engine = create_engine(connection_url, **pool_config)
        
        # Test connection immediately (Fail FAST)
        with engine.connect() as conn:
            result = conn.execute(text("SELECT 1"))
            result.fetchone()
        
        return engine
        
    except SQLAlchemyError as e:
        error_msg = f"Database connection failed: {str(e)}"
        
        # Provide helpful troubleshooting based on error type
        if "password authentication failed" in str(e):
            error_msg += "\n\nTroubleshooting:"
            error_msg += "\n- Check username/password in config/config.yaml"
            error_msg += "\n- Verify PostgreSQL user exists and has correct password"
            error_msg += "\n- Run: sudo -u postgres psql -c \"ALTER USER typing_user PASSWORD 'new_password';\""
        
        elif "could not connect to server" in str(e):
            error_msg += "\n\nTroubleshooting:"
            error_msg += "\n- Check if PostgreSQL is running: sudo systemctl status postgresql"
            error_msg += "\n- Start PostgreSQL: sudo systemctl start postgresql"
            error_msg += "\n- Check host/port in config/config.yaml"
        
        elif "database" in str(e) and "does not exist" in str(e):
            error_msg += "\n\nTroubleshooting:"
            error_msg += "\n- Create database: sudo -u postgres createdb typing_clients"
            error_msg += "\n- Or update database name in config/config.yaml"
        
        if fail_on_error:
            print(f"❌ FAIL FAST: {error_msg}")
            sys.exit(1)
        else:
            print(f"⚠️  Warning: {error_msg}")
            return None
            
    except Exception as e:
        error_msg = f"Unexpected error getting database engine: {str(e)}"
        if fail_on_error:
            print(f"❌ FAIL FAST: {error_msg}")
            traceback.print_exc()
            sys.exit(1)
        else:
            print(f"⚠️  Warning: {error_msg}")
            return None


def test_database_connection() -> bool:
    """
    Test database connection without failing
    Returns True if connection successful, False otherwise
    """
    engine = get_database_engine(fail_on_error=False)
    if engine:
        engine.dispose()  # Clean up connection pool
        return True
    return False


if __name__ == "__main__":
    # Test the connection when run directly
    print("🔍 Testing database connection...")
    
    if test_database_connection():
        print("✅ Database connection successful!")
        
        # Test with fail_on_error=True
        engine = get_database_engine()
        print(f"✅ Engine created: {engine}")
        engine.dispose()
    else:
        print("❌ Database connection failed")
        sys.exit(1)