#!/usr/bin/env python3
"""
Database Schema Creation Script

Creates PostgreSQL tables with exact CSV schema mapping for database migration.
Implements fail-fast, fail-loud, fail-safely principles.
"""

import os
import sys
import json
from datetime import datetime
from typing import Dict, Any

# Import our utilities
from utils.database_operations import (
    DatabaseConfig, DatabaseManager, get_database_manager,
    create_table, table_exists, execute_sql
)
from utils.database_logging import (
    db_logger, log_migration_phase, log_schema_operation,
    DatabaseOperationLogger, log_database_operation
)

def get_csv_schema_mapping() -> Dict[str, str]:
    """
    Define the exact schema mapping from CSV to PostgreSQL.
    
    Returns:
        Dictionary mapping column names to PostgreSQL types
    """
    
    return {
        # Core identification
        'row_id': 'INTEGER PRIMARY KEY',
        'name': 'TEXT NOT NULL',
        'email': 'TEXT',
        'type': 'TEXT',
        'link': 'TEXT',
        
        # Extracted data (pipe-separated lists stored as TEXT)
        'extracted_links': 'TEXT',
        'youtube_playlist': 'TEXT',
        'google_drive': 'TEXT',
        
        # Processing status
        'processed': 'BOOLEAN DEFAULT FALSE',
        'document_text': 'TEXT',
        
        # YouTube processing
        'youtube_status': 'TEXT',
        'youtube_files': 'TEXT',
        'youtube_media_id': 'TEXT',
        
        # Drive processing
        'drive_status': 'TEXT',
        'drive_files': 'TEXT',
        'drive_media_id': 'TEXT',
        
        # Download tracking
        'last_download_attempt': 'TIMESTAMP',
        'download_errors': 'TEXT',
        'permanent_failure': 'BOOLEAN DEFAULT FALSE',
        
        # UUID and S3 tracking (JSON fields)
        'file_uuids': 'JSONB',
        's3_paths': 'JSONB',
        
        # Metadata
        'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
        'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
    }

def create_normalized_schema() -> Dict[str, Dict[str, str]]:
    """
    Create normalized database schema for future optimization.
    
    This creates separate tables for different entity types while maintaining
    compatibility with the CSV structure.
    
    Returns:
        Dictionary of table schemas
    """
    
    return {
        # Main people/records table
        'people': {
            'row_id': 'INTEGER PRIMARY KEY',
            'name': 'TEXT NOT NULL',
            'email': 'TEXT',
            'type': 'TEXT',
            'link': 'TEXT',
            'processed': 'BOOLEAN DEFAULT FALSE',
            'document_text': 'TEXT',
            'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
            'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
        },
        
        # Processing status table
        'processing_status': {
            'id': 'SERIAL PRIMARY KEY',
            'row_id': 'INTEGER REFERENCES people(row_id) ON DELETE CASCADE',
            'youtube_status': 'TEXT',
            'drive_status': 'TEXT',
            'last_download_attempt': 'TIMESTAMP',
            'download_errors': 'TEXT',
            'permanent_failure': 'BOOLEAN DEFAULT FALSE',
            'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
            'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
        },
        
        # S3 and file metadata
        's3_metadata': {
            'id': 'SERIAL PRIMARY KEY',
            'row_id': 'INTEGER REFERENCES people(row_id) ON DELETE CASCADE',
            'file_uuids': 'JSONB',
            's3_paths': 'JSONB',
            'youtube_files': 'TEXT',
            'youtube_media_id': 'TEXT',
            'drive_files': 'TEXT',
            'drive_media_id': 'TEXT',
            'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP',
            'updated_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
        },
        
        # Extracted links (normalized)
        'extracted_links': {
            'id': 'SERIAL PRIMARY KEY',
            'row_id': 'INTEGER REFERENCES people(row_id) ON DELETE CASCADE',
            'link_type': 'TEXT', # 'extracted', 'youtube_playlist', 'google_drive'
            'url': 'TEXT NOT NULL',
            'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
        }
    }

@log_database_operation("Database Connection Test")
def test_database_connection():
    """Test database connection before schema creation."""
    
    config = DatabaseConfig(
        db_type='postgresql',
        host='localhost',
        port=5432,
        database='typing_clients_uuid',
        username='migration_user',
        password=os.environ.get('DB_PASSWORD', '')
    )
    
    db_manager = DatabaseManager(config)
    
    with db_manager.get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT version()")
        result = cursor.fetchone()
        db_logger.info(f"Database connection successful: {result['version']}")
        return True

@log_database_operation("CSV Schema Table Creation")
def create_csv_compatible_table():
    """Create main table with exact CSV schema mapping."""
    
    schema = get_csv_schema_mapping()
    table_name = 'typing_clients_data'
    
    with DatabaseOperationLogger("Creating CSV-compatible table"):
        # Drop table if exists (for clean migration)
        if table_exists(table_name):
            db_logger.warning(f"Table {table_name} already exists - dropping for clean migration")
            execute_sql(f"DROP TABLE IF EXISTS {table_name} CASCADE")
        
        # Create table
        success = create_table(table_name, schema, if_not_exists=False)
        
        if not success:
            raise RuntimeError(f"Failed to create table {table_name}")
        
        # Create indexes for performance
        indexes = [
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_row_id ON {table_name}(row_id)",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_email ON {table_name}(email)",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_processed ON {table_name}(processed)",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_youtube_status ON {table_name}(youtube_status)",
            f"CREATE INDEX IF NOT EXISTS idx_{table_name}_drive_status ON {table_name}(drive_status)"
        ]
        
        for index_sql in indexes:
            execute_sql(index_sql)
            db_logger.debug(f"Created index: {index_sql}")
        
        log_schema_operation(table_name, "CREATE", schema)
        return True

@log_database_operation("Normalized Schema Creation")
def create_normalized_tables():
    """Create normalized table structure for future optimization."""
    
    schemas = create_normalized_schema()
    
    with DatabaseOperationLogger("Creating normalized tables"):
        for table_name, schema in schemas.items():
            # Drop table if exists
            if table_exists(table_name):
                db_logger.warning(f"Normalized table {table_name} already exists - dropping")
                execute_sql(f"DROP TABLE IF EXISTS {table_name} CASCADE")
            
            # Create table
            success = create_table(table_name, schema, if_not_exists=False)
            
            if not success:
                raise RuntimeError(f"Failed to create normalized table {table_name}")
            
            log_schema_operation(table_name, "CREATE_NORMALIZED", schema)
        
        # Create additional indexes for normalized tables
        normalized_indexes = [
            "CREATE INDEX IF NOT EXISTS idx_processing_status_row_id ON processing_status(row_id)",
            "CREATE INDEX IF NOT EXISTS idx_s3_metadata_row_id ON s3_metadata(row_id)",
            "CREATE INDEX IF NOT EXISTS idx_extracted_links_row_id ON extracted_links(row_id)",
            "CREATE INDEX IF NOT EXISTS idx_extracted_links_type ON extracted_links(link_type)"
        ]
        
        for index_sql in normalized_indexes:
            execute_sql(index_sql)
            db_logger.debug(f"Created normalized index: {index_sql}")
        
        return True

@log_database_operation("Migration Tracking Setup")
def create_migration_tracking():
    """Create tables for tracking migration progress."""
    
    migration_schema = {
        'migration_log': {
            'id': 'SERIAL PRIMARY KEY',
            'operation': 'TEXT NOT NULL',
            'phase': 'TEXT NOT NULL',
            'status': 'TEXT NOT NULL', # 'started', 'completed', 'failed'
            'record_count': 'INTEGER',
            'duration_seconds': 'REAL',
            'error_message': 'TEXT',
            'metadata': 'JSONB',
            'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
        },
        
        'data_validation': {
            'id': 'SERIAL PRIMARY KEY',
            'validation_type': 'TEXT NOT NULL', # 'consistency', 'dual_write', 'integrity'
            'table_name': 'TEXT NOT NULL',
            'csv_count': 'INTEGER',
            'db_count': 'INTEGER',
            'mismatch_details': 'JSONB',
            'status': 'TEXT NOT NULL', # 'passed', 'failed'
            'created_at': 'TIMESTAMP DEFAULT CURRENT_TIMESTAMP'
        }
    }
    
    with DatabaseOperationLogger("Creating migration tracking tables"):
        for table_name, schema in migration_schema.items():
            if table_exists(table_name):
                db_logger.info(f"Migration table {table_name} already exists - keeping")
            else:
                success = create_table(table_name, schema, if_not_exists=True)
                if not success:
                    raise RuntimeError(f"Failed to create migration table {table_name}")
                
                log_schema_operation(table_name, "CREATE_MIGRATION", schema)
        
        return True

def verify_schema_creation():
    """Verify all tables were created successfully."""
    
    expected_tables = [
        'typing_clients_data',  # Main CSV-compatible table
        'people',               # Normalized tables
        'processing_status',
        's3_metadata', 
        'extracted_links',
        'migration_log',        # Migration tracking
        'data_validation'
    ]
    
    with DatabaseOperationLogger("Schema verification"):
        missing_tables = []
        
        for table_name in expected_tables:
            if not table_exists(table_name):
                missing_tables.append(table_name)
        
        if missing_tables:
            error_msg = f"Missing tables: {missing_tables}"
            db_logger.error(error_msg)
            raise RuntimeError(error_msg)
        
        db_logger.info(f"✅ All {len(expected_tables)} tables created successfully")
        
        # Get table statistics
        for table_name in expected_tables:
            stats = execute_sql(f"SELECT COUNT(*) as count FROM {table_name}")
            count = stats[0]['count'] if stats else 0
            db_logger.info(f"📊 Table {table_name}: {count} rows")
        
        return True

def main():
    """Main schema creation process."""
    
    log_migration_phase("Schema Creation", "starting")
    
    try:
        # Set environment variable for database password
        if not os.environ.get('DB_PASSWORD'):
            os.environ['DB_PASSWORD'] = 'migration_pass_2025'
        
        # Step 1: Test database connection
        db_logger.info("🔍 Testing database connection...")
        test_database_connection()
        
        # Step 2: Create CSV-compatible table
        db_logger.info("🏗️ Creating CSV-compatible main table...")
        create_csv_compatible_table()
        
        # Step 3: Create normalized tables for future optimization
        db_logger.info("🔄 Creating normalized table structure...")
        create_normalized_tables()
        
        # Step 4: Create migration tracking tables
        db_logger.info("📊 Creating migration tracking tables...")
        create_migration_tracking()
        
        # Step 5: Verify everything was created
        db_logger.info("✅ Verifying schema creation...")
        verify_schema_creation()
        
        log_migration_phase("Schema Creation", "completed", 
                          "All database tables created successfully")
        
        # Record migration step
        execute_sql("""
            INSERT INTO migration_log (operation, phase, status, record_count, metadata)
            VALUES (?, ?, ?, ?, ?)
        """, [
            'Schema Creation',
            'Phase 1',
            'completed',
            0,
            json.dumps({
                'tables_created': 7,
                'timestamp': datetime.now().isoformat(),
                'schema_version': '1.0'
            })
        ])
        
        db_logger.info("🎉 DATABASE SCHEMA CREATION COMPLETED SUCCESSFULLY")
        return True
        
    except Exception as e:
        log_migration_phase("Schema Creation", "failed", str(e))
        db_logger.error(f"💥 Schema creation failed: {e}")
        
        # Record failure
        try:
            execute_sql("""
                INSERT INTO migration_log (operation, phase, status, error_message, metadata)
                VALUES (?, ?, ?, ?, ?)
            """, [
                'Schema Creation',
                'Phase 1', 
                'failed',
                str(e),
                json.dumps({'timestamp': datetime.now().isoformat()})
            ])
        except:
            pass  # Don't fail if we can't record the failure
        
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)