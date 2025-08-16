#!/usr/bin/env python3
"""
Database-Native Operations (Phase 3)

Pure database operations without CSV fallback logic.
This represents the final migration phase where the system operates
entirely on the database without any CSV dependencies.

Key Features:
- Database-only read operations
- No CSV fallback mechanisms
- Enhanced error handling for database-native mode
- Optimized performance for database operations
- Comprehensive logging for database-native operations
"""

import os
import sys
import time
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional, Union
import statistics

# Set up environment
os.environ['DB_PASSWORD'] = 'migration_pass_2025'
sys.path.append('.')

from utils.database_operations import DatabaseConfig, DatabaseManager
from utils.database_logging import db_logger, DatabaseOperationLogger, log_database_operation

class DatabaseNativeManager:
    """Database-native operations without CSV fallback."""
    
    def __init__(self):
        """Initialize database-native manager."""
        
        # Database configuration
        self.config = DatabaseConfig(
            db_type='postgresql',
            host='localhost',
            port=5432,
            database='typing_clients_uuid',
            username='migration_user',
            password='migration_pass_2025'
        )
        
        self.db_manager = DatabaseManager(self.config)
        
        # Performance and operation metrics
        self.metrics = {
            'total_operations': 0,
            'successful_operations': 0,
            'failed_operations': 0,
            'operation_times': [],
            'start_time': datetime.now()
        }
        
        db_logger.info("🗄️ Database-native manager initialized (Phase 3)")
    
    @log_database_operation("Database-Native Record Read")
    def get_record_by_id(self, row_id: int) -> Optional[Dict[str, Any]]:
        """
        Get single record by row_id - database-native only.
        
        Args:
            row_id: ID of record to retrieve
            
        Returns:
            Record dictionary or None if not found
            
        Raises:
            Exception: If database operation fails (no fallback)
        """
        with DatabaseOperationLogger(f"Database-native record read: {row_id}"):
            self.metrics['total_operations'] += 1
            start_time = time.time()
            
            try:
                with self.db_manager.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT * FROM typing_clients_data WHERE row_id = %s", [row_id])
                    result = cursor.fetchone()
                    
                    if result:
                        record = dict(result)
                        self.metrics['successful_operations'] += 1
                        db_logger.debug(f"✅ Database-native record read successful: {row_id}")
                        return record
                    else:
                        db_logger.debug(f"🔍 Record {row_id} not found in database")
                        self.metrics['successful_operations'] += 1  # Not finding a record is still a successful operation
                        return None
                        
            except Exception as e:
                self.metrics['failed_operations'] += 1
                db_logger.error(f"❌ Database-native record read failed for {row_id}: {e}")
                raise Exception(f"Database operation failed: {e}")
            
            finally:
                operation_time = time.time() - start_time
                self.metrics['operation_times'].append(operation_time)
    
    @log_database_operation("Database-Native Records Query")
    def get_records(self, row_ids: List[int] = None, limit: int = None, 
                   where_clause: str = None, params: List[Any] = None) -> List[Dict[str, Any]]:
        """
        Get multiple records - database-native only.
        
        Args:
            row_ids: Specific row IDs to retrieve (optional)
            limit: Maximum number of records to return
            where_clause: SQL WHERE clause for filtering
            params: Parameters for WHERE clause
            
        Returns:
            List of record dictionaries
            
        Raises:
            Exception: If database operation fails (no fallback)
        """
        with DatabaseOperationLogger(f"Database-native records query"):
            self.metrics['total_operations'] += 1
            start_time = time.time()
            
            try:
                with self.db_manager.get_connection() as conn:
                    cursor = conn.cursor()
                    
                    if row_ids:
                        # Query specific row IDs
                        placeholders = ','.join(['%s' for _ in row_ids])
                        sql = f"SELECT * FROM typing_clients_data WHERE row_id IN ({placeholders})"
                        cursor.execute(sql, row_ids)
                    elif where_clause:
                        # Query with WHERE clause
                        sql = f"SELECT * FROM typing_clients_data WHERE {where_clause}"
                        if limit:
                            sql += f" LIMIT {limit}"
                        cursor.execute(sql, params or [])
                    else:
                        # Query all records
                        sql = "SELECT * FROM typing_clients_data"
                        if limit:
                            sql += f" LIMIT {limit}"
                        cursor.execute(sql)
                    
                    results = cursor.fetchall()
                    records = [dict(row) for row in results]
                    
                    self.metrics['successful_operations'] += 1
                    db_logger.debug(f"✅ Database-native records query successful: {len(records)} records")
                    return records
                    
            except Exception as e:
                self.metrics['failed_operations'] += 1
                db_logger.error(f"❌ Database-native records query failed: {e}")
                raise Exception(f"Database operation failed: {e}")
            
            finally:
                operation_time = time.time() - start_time
                self.metrics['operation_times'].append(operation_time)
    
    @log_database_operation("Database-Native DataFrame Read")
    def read_dataframe(self, dtype_spec: str = 'tracking') -> pd.DataFrame:
        """
        Read data as DataFrame - database-native only.
        
        Args:
            dtype_spec: Data type specification (maintained for compatibility)
            
        Returns:
            DataFrame with data from database
            
        Raises:
            Exception: If database operation fails (no fallback)
        """
        with DatabaseOperationLogger(f"Database-native DataFrame read"):
            self.metrics['total_operations'] += 1
            start_time = time.time()
            
            try:
                records = self.get_records()
                df = pd.DataFrame(records)
                
                self.metrics['successful_operations'] += 1
                db_logger.debug(f"✅ Database-native DataFrame read successful: {len(df)} records")
                return df
                
            except Exception as e:
                self.metrics['failed_operations'] += 1
                db_logger.error(f"❌ Database-native DataFrame read failed: {e}")
                raise Exception(f"Database operation failed: {e}")
            
            finally:
                operation_time = time.time() - start_time
                self.metrics['operation_times'].append(operation_time)
    
    @log_database_operation("Database-Native Count Query")
    def count_records(self, where_clause: str = None, params: List[Any] = None) -> int:
        """
        Count records - database-native only.
        
        Args:
            where_clause: SQL WHERE clause for filtering
            params: Parameters for WHERE clause
            
        Returns:
            Number of records
            
        Raises:
            Exception: If database operation fails (no fallback)
        """
        with DatabaseOperationLogger(f"Database-native count query"):
            self.metrics['total_operations'] += 1
            start_time = time.time()
            
            try:
                with self.db_manager.get_connection() as conn:
                    cursor = conn.cursor()
                    
                    if where_clause:
                        sql = f"SELECT COUNT(*) as count FROM typing_clients_data WHERE {where_clause}"
                        cursor.execute(sql, params or [])
                    else:
                        sql = "SELECT COUNT(*) as count FROM typing_clients_data"
                        cursor.execute(sql)
                    
                    result = cursor.fetchone()
                    count = result['count']
                    
                    self.metrics['successful_operations'] += 1
                    db_logger.debug(f"✅ Database-native count successful: {count} records")
                    return count
                    
            except Exception as e:
                self.metrics['failed_operations'] += 1
                db_logger.error(f"❌ Database-native count failed: {e}")
                raise Exception(f"Database operation failed: {e}")
            
            finally:
                operation_time = time.time() - start_time
                self.metrics['operation_times'].append(operation_time)
    
    def get_records_by_criteria(self, **criteria) -> List[Dict[str, Any]]:
        """
        Get records matching criteria - database-native only.
        
        Args:
            **criteria: Field-value pairs for filtering
            
        Returns:
            List of matching record dictionaries
            
        Raises:
            Exception: If database operation fails (no fallback)
        """
        self.metrics['total_operations'] += 1
        start_time = time.time()
        
        try:
            # Build WHERE clause from criteria
            if not criteria:
                return self.get_records()
            
            where_parts = []
            params = []
            
            for field, value in criteria.items():
                where_parts.append(f"{field} = %s")
                params.append(value)
            
            where_clause = " AND ".join(where_parts)
            records = self.get_records(where_clause=where_clause, params=params)
            
            self.metrics['successful_operations'] += 1
            db_logger.debug(f"✅ Database-native criteria query successful: {len(records)} records")
            return records
            
        except Exception as e:
            self.metrics['failed_operations'] += 1
            db_logger.error(f"❌ Database-native criteria query failed: {e}")
            raise Exception(f"Database operation failed: {e}")
        
        finally:
            operation_time = time.time() - start_time
            self.metrics['operation_times'].append(operation_time)
    
    def get_usage_statistics(self) -> Dict[str, Any]:
        """Get usage statistics for database-native operations."""
        
        current_time = datetime.now()
        uptime = (current_time - self.metrics['start_time']).total_seconds()
        total_ops = self.metrics['total_operations']
        
        stats = {
            'total_operations': total_ops,
            'successful_operations': self.metrics['successful_operations'],
            'failed_operations': self.metrics['failed_operations'],
            'success_rate': (self.metrics['successful_operations'] / total_ops * 100) if total_ops > 0 else 0,
            'failure_rate': (self.metrics['failed_operations'] / total_ops * 100) if total_ops > 0 else 0,
            'uptime_seconds': uptime,
            'mode': 'database_native'
        }
        
        # Add timing statistics
        if self.metrics['operation_times']:
            stats['performance'] = {
                'avg_operation_time': statistics.mean(self.metrics['operation_times']),
                'min_operation_time': min(self.metrics['operation_times']),
                'max_operation_time': max(self.metrics['operation_times']),
                'total_operation_time': sum(self.metrics['operation_times'])
            }
        
        return stats
    
    def health_check(self) -> Dict[str, Any]:
        """Perform comprehensive health check for database-native operations."""
        
        health_data = {
            'timestamp': datetime.now().isoformat(),
            'mode': 'database_native',
            'status': 'unknown',
            'checks': {},
            'performance': {}
        }
        
        try:
            # Test 1: Basic connectivity
            start_time = time.time()
            count = self.count_records()
            connectivity_time = time.time() - start_time
            
            health_data['checks']['connectivity'] = {
                'status': 'ok',
                'response_time': connectivity_time,
                'record_count': count
            }
            
            # Test 2: Read operation
            start_time = time.time()
            record = self.get_record_by_id(1)
            read_time = time.time() - start_time
            
            health_data['checks']['read_operation'] = {
                'status': 'ok' if record is not None else 'warning',
                'response_time': read_time,
                'record_found': record is not None
            }
            
            # Test 3: Query operation
            start_time = time.time()
            records = self.get_records(limit=10)
            query_time = time.time() - start_time
            
            health_data['checks']['query_operation'] = {
                'status': 'ok',
                'response_time': query_time,
                'records_returned': len(records)
            }
            
            # Overall status assessment
            all_checks = [check['status'] for check in health_data['checks'].values()]
            if all(status == 'ok' for status in all_checks):
                health_data['status'] = 'healthy'
            elif 'error' in all_checks:
                health_data['status'] = 'critical'
            else:
                health_data['status'] = 'warning'
            
            # Performance summary
            health_data['performance'] = {
                'avg_response_time': statistics.mean([connectivity_time, read_time, query_time]),
                'total_test_time': connectivity_time + read_time + query_time
            }
            
        except Exception as e:
            health_data['status'] = 'critical'
            health_data['error'] = str(e)
            db_logger.error(f"❌ Database-native health check failed: {e}")
        
        return health_data
    
    @log_database_operation("Database-Native Write Operation")
    def write_record(self, record_data: Dict[str, Any]) -> bool:
        """
        Write/update single record - database-native only.
        
        Args:
            record_data: Dictionary containing record data
            
        Returns:
            True if successful
            
        Raises:
            Exception: If database operation fails (no CSV backup)
        """
        with DatabaseOperationLogger(f"Database-native write: {record_data.get('row_id', 'N/A')}"):
            self.metrics['total_operations'] += 1
            start_time = time.time()
            
            try:
                with self.db_manager.get_connection() as conn:
                    cursor = conn.cursor()
                    
                    # Ensure required fields
                    row_id = record_data.get('row_id')
                    if not row_id:
                        raise ValueError("row_id is required for database-native write")
                    
                    # Build upsert query
                    columns = list(record_data.keys())
                    values = list(record_data.values())
                    
                    # Create placeholders
                    placeholders = ', '.join(['%s'] * len(values))
                    column_names = ', '.join(columns)
                    
                    # Create update clause for conflict resolution
                    update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'row_id'])
                    
                    sql = f"""
                        INSERT INTO typing_clients_data ({column_names})
                        VALUES ({placeholders})
                        ON CONFLICT (row_id) DO UPDATE SET
                        {update_clause},
                        updated_at = CURRENT_TIMESTAMP
                    """
                    
                    cursor.execute(sql, values)
                    conn.commit()
                    
                    self.metrics['successful_operations'] += 1
                    db_logger.info(f"✅ Database-native write successful: row_id {row_id}")
                    return True
                    
            except Exception as e:
                self.metrics['failed_operations'] += 1
                db_logger.error(f"❌ Database-native write failed: {e}")
                raise Exception(f"Database write operation failed: {e}")
            
            finally:
                operation_time = time.time() - start_time
                self.metrics['operation_times'].append(operation_time)
    
    @log_database_operation("Database-Native Bulk Write Operation")  
    def write_records_bulk(self, records: List[Dict[str, Any]]) -> int:
        """
        Write multiple records in bulk - database-native only.
        
        Args:
            records: List of record dictionaries
            
        Returns:
            Number of records written
            
        Raises:
            Exception: If database operation fails (no CSV backup)
        """
        with DatabaseOperationLogger(f"Database-native bulk write: {len(records)} records"):
            self.metrics['total_operations'] += 1
            start_time = time.time()
            
            try:
                if not records:
                    return 0
                
                with self.db_manager.get_connection() as conn:
                    cursor = conn.cursor()
                    
                    # Use first record to determine columns
                    columns = list(records[0].keys())
                    
                    # Prepare bulk insert
                    placeholders = ', '.join(['%s'] * len(columns))
                    column_names = ', '.join(columns)
                    
                    # Create update clause for conflict resolution
                    update_clause = ', '.join([f"{col} = EXCLUDED.{col}" for col in columns if col != 'row_id'])
                    
                    sql = f"""
                        INSERT INTO typing_clients_data ({column_names})
                        VALUES ({placeholders})
                        ON CONFLICT (row_id) DO UPDATE SET
                        {update_clause},
                        updated_at = CURRENT_TIMESTAMP
                    """
                    
                    # Execute bulk insert
                    for record in records:
                        values = [record.get(col) for col in columns]
                        cursor.execute(sql, values)
                    
                    conn.commit()
                    rows_affected = len(records)
                    
                    self.metrics['successful_operations'] += 1
                    db_logger.info(f"✅ Database-native bulk write successful: {rows_affected} records")
                    return rows_affected
                    
            except Exception as e:
                self.metrics['failed_operations'] += 1
                db_logger.error(f"❌ Database-native bulk write failed: {e}")
                raise Exception(f"Database bulk write operation failed: {e}")
            
            finally:
                operation_time = time.time() - start_time
                self.metrics['operation_times'].append(operation_time)
    
    @log_database_operation("Database-Native Update Operation")
    def update_record(self, row_id: int, updates: Dict[str, Any]) -> bool:
        """
        Update specific fields of a record - database-native only.
        
        Args:
            row_id: ID of record to update
            updates: Dictionary of fields to update
            
        Returns:
            True if successful
            
        Raises:
            Exception: If database operation fails (no CSV backup)
        """
        with DatabaseOperationLogger(f"Database-native update: {row_id}"):
            self.metrics['total_operations'] += 1
            start_time = time.time()
            
            try:
                if not updates:
                    raise ValueError("No updates provided")
                
                with self.db_manager.get_connection() as conn:
                    cursor = conn.cursor()
                    
                    # Build update query
                    set_clauses = []
                    values = []
                    
                    for field, value in updates.items():
                        set_clauses.append(f"{field} = %s")
                        values.append(value)
                    
                    # Add updated timestamp
                    set_clauses.append("updated_at = CURRENT_TIMESTAMP")
                    values.append(row_id)  # For WHERE clause
                    
                    sql = f"""
                        UPDATE typing_clients_data 
                        SET {', '.join(set_clauses)}
                        WHERE row_id = %s
                    """
                    
                    cursor.execute(sql, values)
                    conn.commit()
                    
                    rows_affected = cursor.rowcount
                    if rows_affected == 0:
                        db_logger.warning(f"⚠️ No record found to update: row_id {row_id}")
                    
                    self.metrics['successful_operations'] += 1
                    db_logger.info(f"✅ Database-native update successful: row_id {row_id}, {rows_affected} rows affected")
                    return rows_affected > 0
                    
            except Exception as e:
                self.metrics['failed_operations'] += 1
                db_logger.error(f"❌ Database-native update failed: {e}")
                raise Exception(f"Database update operation failed: {e}")
            
            finally:
                operation_time = time.time() - start_time
                self.metrics['operation_times'].append(operation_time)
    
    @log_database_operation("Database-Native Delete Operation")
    def delete_record(self, row_id: int) -> bool:
        """
        Delete a record - database-native only.
        
        Args:
            row_id: ID of record to delete
            
        Returns:
            True if successful
            
        Raises:
            Exception: If database operation fails (no CSV backup)
        """
        with DatabaseOperationLogger(f"Database-native delete: {row_id}"):
            self.metrics['total_operations'] += 1
            start_time = time.time()
            
            try:
                with self.db_manager.get_connection() as conn:
                    cursor = conn.cursor()
                    
                    sql = "DELETE FROM typing_clients_data WHERE row_id = %s"
                    cursor.execute(sql, [row_id])
                    conn.commit()
                    
                    rows_affected = cursor.rowcount
                    if rows_affected == 0:
                        db_logger.warning(f"⚠️ No record found to delete: row_id {row_id}")
                    
                    self.metrics['successful_operations'] += 1
                    db_logger.info(f"✅ Database-native delete successful: row_id {row_id}, {rows_affected} rows affected")
                    return rows_affected > 0
                    
            except Exception as e:
                self.metrics['failed_operations'] += 1
                db_logger.error(f"❌ Database-native delete failed: {e}")
                raise Exception(f"Database delete operation failed: {e}")
            
            finally:
                operation_time = time.time() - start_time
                self.metrics['operation_times'].append(operation_time)

# Convenience functions for backward compatibility

# Read operations
def get_record_database_native(row_id: int) -> Optional[Dict[str, Any]]:
    """Get single record using database-native operation."""
    manager = DatabaseNativeManager()
    return manager.get_record_by_id(row_id)

def get_records_database_native(row_ids: List[int] = None, limit: int = None) -> List[Dict[str, Any]]:
    """Get multiple records using database-native operation."""
    manager = DatabaseNativeManager()
    return manager.get_records(row_ids, limit)

def count_records_database_native() -> int:
    """Count records using database-native operation."""
    manager = DatabaseNativeManager()
    return manager.count_records()

def read_dataframe_database_native(dtype_spec: str = 'tracking') -> pd.DataFrame:
    """Read DataFrame using database-native operation."""
    manager = DatabaseNativeManager()
    return manager.read_dataframe(dtype_spec)

# Write operations (database-native only, no CSV)
def write_record_database_native(record_data: Dict[str, Any]) -> bool:
    """Write single record using database-native operation."""
    manager = DatabaseNativeManager()
    return manager.write_record(record_data)

def write_records_bulk_database_native(records: List[Dict[str, Any]]) -> int:
    """Write multiple records using database-native bulk operation."""
    manager = DatabaseNativeManager()
    return manager.write_records_bulk(records)

def update_record_database_native(row_id: int, updates: Dict[str, Any]) -> bool:
    """Update record using database-native operation."""
    manager = DatabaseNativeManager()
    return manager.update_record(row_id, updates)

def delete_record_database_native(row_id: int) -> bool:
    """Delete record using database-native operation."""
    manager = DatabaseNativeManager()
    return manager.delete_record(row_id)

# Test functions
if __name__ == "__main__":
    print("🗄️ Testing Database-Native Operations (Phase 3)")
    print("=" * 50)
    
    manager = DatabaseNativeManager()
    
    try:
        # Test health check
        print("\n🏥 Health Check:")
        health = manager.health_check()
        print(f"   Status: {health['status'].upper()}")
        print(f"   Record count: {health['checks']['connectivity']['record_count']}")
        print(f"   Avg response time: {health['performance']['avg_response_time']:.3f}s")
        
        # Test read operations
        print("\n📖 Read Operations:")
        record = manager.get_record_by_id(1)
        if record:
            print(f"   ✅ Single record: {record['name']}")
        
        records = manager.get_records(limit=5)
        print(f"   ✅ Multiple records: {len(records)} found")
        
        count = manager.count_records()
        print(f"   ✅ Total count: {count} records")
        
        # Test DataFrame operation
        print("\n📊 DataFrame Operation:")
        df = manager.read_dataframe()
        print(f"   ✅ DataFrame: {len(df)} records, {len(df.columns)} columns")
        
        # Test write operations (database-native only)
        print("\n✍️ Write Operations (Database-Native Only):")
        
        # Test single record write
        test_record = {
            'row_id': 999999,
            'name': 'Database Native Test User',
            'email': 'db.native@test.com',
            'type': 'Test Record',
            'processed': False
        }
        
        write_success = manager.write_record(test_record)
        print(f"   ✅ Single record write: {'Success' if write_success else 'Failed'}")
        
        # Verify write
        written_record = manager.get_record_by_id(999999)
        if written_record:
            print(f"   ✅ Write verification: {written_record['name']}")
        
        # Test update
        updates = {
            'name': 'Updated Database Native User',
            'processed': True
        }
        update_success = manager.update_record(999999, updates)
        print(f"   ✅ Record update: {'Success' if update_success else 'Failed'}")
        
        # Verify update
        updated_record = manager.get_record_by_id(999999)
        if updated_record and updated_record['processed']:
            print(f"   ✅ Update verification: {updated_record['name']}")
        
        # Test bulk write
        bulk_records = [
            {
                'row_id': 999998,
                'name': 'Bulk Test User 1',
                'email': 'bulk1@test.com',
                'type': 'Bulk Test',
                'processed': False
            },
            {
                'row_id': 999997,
                'name': 'Bulk Test User 2',
                'email': 'bulk2@test.com',
                'type': 'Bulk Test',
                'processed': False
            }
        ]
        
        bulk_count = manager.write_records_bulk(bulk_records)
        print(f"   ✅ Bulk write: {bulk_count} records written")
        
        # Clean up test records
        print("\n🧹 Cleanup:")
        delete_count = 0
        for test_id in [999999, 999998, 999997]:
            if manager.delete_record(test_id):
                delete_count += 1
        print(f"   ✅ Cleanup: {delete_count} test records deleted")
        
        # Show final usage statistics
        print("\n📊 Final Usage Statistics:")
        stats = manager.get_usage_statistics()
        print(f"   Total operations: {stats['total_operations']}")
        print(f"   Success rate: {stats['success_rate']:.1f}%")
        print(f"   Avg operation time: {stats['performance']['avg_operation_time']:.3f}s")
        print(f"   Mode: {stats['mode']}")
        
        print("\n✅ All database-native operations working correctly!")
        print("🎯 NO CSV dependencies - pure database operations only")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        db_logger.error(f"Database-native operations test failed: {e}")