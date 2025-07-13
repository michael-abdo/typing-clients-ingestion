#!/usr/bin/env python3
"""
Database Manager for UUID-based S3 System
Created: 2025-07-13

Provides database connection and query functionality to replace CSV-based operations.
Includes fallback to CSV for compatibility during transition.
"""

import os
import yaml
import psycopg2
import pandas as pd
from psycopg2.extras import RealDictCursor
from psycopg2.pool import SimpleConnectionPool
from typing import Dict, List, Optional, Union, Any
import logging
from pathlib import Path
from contextlib import contextmanager
import json

# Configure logging
logger = logging.getLogger(__name__)

class DatabaseManager:
    """Manages database connections and provides query interface"""
    
    def __init__(self, config_path: str = "config/config.yaml"):
        self.config_path = config_path
        self.config = self._load_config()
        self.db_config = self.config.get('database', {})
        self.fallback_to_csv = self.db_config.get('fallback_to_csv', True)
        self.csv_path = self.config.get('paths', {}).get('output_csv', 'outputs/output.csv')
        
        # Connection pool
        self.pool = None
        self._initialize_pool()
        
    def _load_config(self) -> Dict:
        """Load configuration from YAML file"""
        try:
            with open(self.config_path, 'r') as f:
                return yaml.safe_load(f)
        except Exception as e:
            logger.error(f"Failed to load config: {e}")
            return {}
    
    def _initialize_pool(self):
        """Initialize database connection pool"""
        if not self.db_config:
            logger.warning("No database configuration found")
            return
            
        try:
            pool_config = self.db_config.get('connection_pool', {})
            
            self.pool = SimpleConnectionPool(
                minconn=pool_config.get('min_connections', 1),
                maxconn=pool_config.get('max_connections', 10),
                host=self.db_config.get('host', 'localhost'),
                port=self.db_config.get('port', 5432),
                database=self.db_config.get('name', 'typing_clients_production'),
                user=self.db_config.get('user', 'migration_user'),
                password=self.db_config.get('password', 'simple123')
            )
            
            logger.info("Database connection pool initialized successfully")
            
        except Exception as e:
            logger.error(f"Failed to initialize database pool: {e}")
            if not self.fallback_to_csv:
                raise
    
    @contextmanager
    def get_connection(self):
        """Get database connection from pool"""
        conn = None
        try:
            if self.pool:
                conn = self.pool.getconn()
                yield conn
            else:
                raise Exception("No database pool available")
        except Exception as e:
            logger.error(f"Database connection error: {e}")
            if not self.fallback_to_csv:
                raise
            # Fallback handled by calling method
            yield None
        finally:
            if conn and self.pool:
                self.pool.putconn(conn)
    
    def test_connection(self) -> bool:
        """Test database connectivity"""
        try:
            with self.get_connection() as conn:
                if conn:
                    with conn.cursor() as cur:
                        cur.execute("SELECT 1")
                        cur.fetchone()
                    logger.info("Database connection test successful")
                    return True
                else:
                    return False
        except Exception as e:
            logger.error(f"Database connection test failed: {e}")
            return False
    
    def get_all_people(self) -> pd.DataFrame:
        """Get all people records"""
        try:
            with self.get_connection() as conn:
                if conn:
                    query = """
                        SELECT person_id, row_id, name, email, type
                        FROM person 
                        ORDER BY row_id
                    """
                    df = pd.read_sql_query(query, conn)
                    logger.info(f"Retrieved {len(df)} people from database")
                    return df
                else:
                    raise Exception("No database connection")
                    
        except Exception as e:
            logger.warning(f"Database query failed, falling back to CSV: {e}")
            if self.fallback_to_csv:
                return self._fallback_get_people_from_csv()
            else:
                raise
    
    def get_person_by_row_id(self, row_id: int) -> Optional[Dict]:
        """Get person by row_id"""
        try:
            with self.get_connection() as conn:
                if conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("""
                            SELECT person_id, row_id, name, email, type
                            FROM person 
                            WHERE row_id = %s
                        """, (row_id,))
                        
                        result = cur.fetchone()
                        if result:
                            return dict(result)
                        return None
                else:
                    raise Exception("No database connection")
                    
        except Exception as e:
            logger.warning(f"Database query failed, falling back to CSV: {e}")
            if self.fallback_to_csv:
                return self._fallback_get_person_from_csv(row_id)
            else:
                raise
    
    def get_person_files(self, row_id: int) -> List[Dict]:
        """Get all files for a person by row_id"""
        try:
            with self.get_connection() as conn:
                if conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("""
                            SELECT f.file_id, f.storage_path, f.original_filename,
                                   f.file_type, f.file_extension, f.file_size,
                                   f.mime_type, f.uploaded_at, f.metadata
                            FROM file f
                            JOIN person p ON f.person_id = p.person_id
                            WHERE p.row_id = %s
                            ORDER BY f.uploaded_at DESC
                        """, (row_id,))
                        
                        results = cur.fetchall()
                        return [dict(row) for row in results]
                else:
                    raise Exception("No database connection")
                    
        except Exception as e:
            logger.warning(f"Database query failed: {e}")
            if self.fallback_to_csv:
                # CSV doesn't have file info, return empty list
                return []
            else:
                raise
    
    def get_files_by_type(self, file_type: str) -> List[Dict]:
        """Get all files of a specific type"""
        try:
            with self.get_connection() as conn:
                if conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("""
                            SELECT f.file_id, f.storage_path, f.original_filename,
                                   f.file_type, f.file_extension, f.file_size,
                                   f.mime_type, f.uploaded_at, f.metadata,
                                   p.row_id, p.name as person_name
                            FROM file f
                            JOIN person p ON f.person_id = p.person_id
                            WHERE f.file_type = %s
                            ORDER BY p.row_id, f.original_filename
                        """, (file_type,))
                        
                        results = cur.fetchall()
                        return [dict(row) for row in results]
                else:
                    raise Exception("No database connection")
                    
        except Exception as e:
            logger.warning(f"Database query failed: {e}")
            if self.fallback_to_csv:
                return []  # CSV doesn't have file type info
            else:
                raise
    
    def search_files(self, search_term: str) -> List[Dict]:
        """Search files by filename or person name"""
        try:
            with self.get_connection() as conn:
                if conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("""
                            SELECT f.file_id, f.storage_path, f.original_filename,
                                   f.file_type, f.file_extension, f.file_size,
                                   f.mime_type, f.uploaded_at, f.metadata,
                                   p.row_id, p.name as person_name
                            FROM file f
                            JOIN person p ON f.person_id = p.person_id
                            WHERE f.original_filename ILIKE %s
                               OR p.name ILIKE %s
                               OR f.file_type ILIKE %s
                            ORDER BY p.row_id, f.original_filename
                        """, (f'%{search_term}%', f'%{search_term}%', f'%{search_term}%'))
                        
                        results = cur.fetchall()
                        return [dict(row) for row in results]
                else:
                    raise Exception("No database connection")
                    
        except Exception as e:
            logger.warning(f"Database query failed: {e}")
            if self.fallback_to_csv:
                return self._fallback_search_people_csv(search_term)
            else:
                raise
    
    def get_person_file_summary(self) -> List[Dict]:
        """Get summary of files per person"""
        try:
            with self.get_connection() as conn:
                if conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("""
                            SELECT * FROM person_file_summary
                            ORDER BY row_id
                        """)
                        
                        results = cur.fetchall()
                        return [dict(row) for row in results]
                else:
                    raise Exception("No database connection")
                    
        except Exception as e:
            logger.warning(f"Database query failed: {e}")
            if self.fallback_to_csv:
                return self._fallback_get_summary_from_csv()
            else:
                raise
    
    def get_file_by_uuid(self, file_uuid: str) -> Optional[Dict]:
        """Get file information by UUID"""
        try:
            with self.get_connection() as conn:
                if conn:
                    with conn.cursor(cursor_factory=RealDictCursor) as cur:
                        cur.execute("""
                            SELECT f.file_id, f.storage_path, f.original_filename,
                                   f.file_type, f.file_extension, f.file_size,
                                   f.mime_type, f.uploaded_at, f.metadata,
                                   p.row_id, p.name as person_name
                            FROM file f
                            JOIN person p ON f.person_id = p.person_id
                            WHERE f.file_id = %s
                        """, (file_uuid,))
                        
                        result = cur.fetchone()
                        if result:
                            return dict(result)
                        return None
                else:
                    raise Exception("No database connection")
                    
        except Exception as e:
            logger.warning(f"Database query failed: {e}")
            return None  # UUID lookups not possible with CSV fallback
    
    # Fallback methods for CSV compatibility
    def _fallback_get_people_from_csv(self) -> pd.DataFrame:
        """Fallback to CSV for people data"""
        try:
            if Path(self.csv_path).exists():
                df = pd.read_csv(self.csv_path)
                logger.info(f"Retrieved {len(df)} people from CSV fallback")
                return df[['row_id', 'name', 'email', 'type']].copy()
            else:
                logger.error(f"CSV file not found: {self.csv_path}")
                return pd.DataFrame(columns=['row_id', 'name', 'email', 'type'])
        except Exception as e:
            logger.error(f"CSV fallback failed: {e}")
            return pd.DataFrame(columns=['row_id', 'name', 'email', 'type'])
    
    def _fallback_get_person_from_csv(self, row_id: int) -> Optional[Dict]:
        """Fallback to CSV for single person lookup"""
        try:
            df = self._fallback_get_people_from_csv()
            person_row = df[df['row_id'] == row_id]
            if not person_row.empty:
                return person_row.iloc[0].to_dict()
            return None
        except Exception as e:
            logger.error(f"CSV person lookup failed: {e}")
            return None
    
    def _fallback_search_people_csv(self, search_term: str) -> List[Dict]:
        """Fallback to CSV for people search"""
        try:
            df = self._fallback_get_people_from_csv()
            matches = df[df['name'].str.contains(search_term, case=False, na=False)]
            return matches.to_dict('records')
        except Exception as e:
            logger.error(f"CSV search failed: {e}")
            return []
    
    def _fallback_get_summary_from_csv(self) -> List[Dict]:
        """Fallback to CSV for summary (no file info available)"""
        try:
            df = self._fallback_get_people_from_csv()
            summary = []
            for _, row in df.iterrows():
                summary.append({
                    'row_id': row['row_id'],
                    'name': row['name'],
                    'email': row.get('email'),
                    'type': row.get('type'),
                    'total_files': 0,  # Not available in CSV
                    'audio_files': 0,
                    'video_files': 0,
                    'document_files': 0,
                    'total_size_bytes': 0,
                    'total_size_mb': 0.0
                })
            return summary
        except Exception as e:
            logger.error(f"CSV summary fallback failed: {e}")
            return []
    
    def close(self):
        """Close database connection pool"""
        if self.pool:
            self.pool.closeall()
            logger.info("Database connection pool closed")

# Convenience function for global access
_global_db_manager = None

def get_database_manager(config_path: str = "config/config.yaml") -> DatabaseManager:
    """Get global database manager instance"""
    global _global_db_manager
    if _global_db_manager is None:
        _global_db_manager = DatabaseManager(config_path)
    return _global_db_manager

def test_database_connection(config_path: str = "config/config.yaml") -> bool:
    """Test database connection"""
    try:
        db = DatabaseManager(config_path)
        return db.test_connection()
    except Exception as e:
        logger.error(f"Database test failed: {e}")
        return False

if __name__ == "__main__":
    # Test the database manager
    logging.basicConfig(level=logging.INFO)
    
    print("Testing database connection...")
    if test_database_connection():
        print("✅ Database connection successful!")
        
        db = get_database_manager()
        
        # Test queries
        print("\nTesting queries...")
        
        # Get all people
        people_df = db.get_all_people()
        print(f"📊 Found {len(people_df)} people in database")
        
        if len(people_df) > 0:
            # Test single person lookup
            first_row_id = people_df.iloc[0]['row_id']
            person = db.get_person_by_row_id(first_row_id)
            print(f"👤 Person {first_row_id}: {person['name'] if person else 'Not found'}")
            
            # Test files for person
            files = db.get_person_files(first_row_id)
            print(f"📁 Files for person {first_row_id}: {len(files)}")
            
        # Test file type query
        audio_files = db.get_files_by_type('audio')
        print(f"🎵 Audio files: {len(audio_files)}")
        
        # Test search
        search_results = db.search_files('mp3')
        print(f"🔍 Search results for 'mp3': {len(search_results)}")
        
        # Test summary
        summary = db.get_person_file_summary()
        print(f"📈 Person file summary: {len(summary)} entries")
        
        db.close()
    else:
        print("❌ Database connection failed, will use CSV fallback")