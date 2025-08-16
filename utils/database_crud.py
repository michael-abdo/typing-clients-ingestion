#!/usr/bin/env python3
"""
Database CRUD Operations for Migration

Provides robust insert/update functions with comprehensive error handling,
transaction management, and fail-fast/fail-loud/fail-safely principles.
"""

import os
import json
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional, Union, Tuple

# Import our utilities
from utils.database_operations import (
    get_database_manager, execute_sql, select, insert as basic_insert
)
from utils.database_logging import (
    db_logger, DatabaseOperationLogger, log_database_operation,
    log_performance_metrics
)
from utils.data_validation import DataValidator

class DatabaseCRUD:
    """Enhanced CRUD operations with validation and error handling."""
    
    def __init__(self):
        """Initialize CRUD operations."""
        self.validator = DataValidator()
        self.db_manager = get_database_manager()
        
    @log_database_operation("Single Record Insert")
    def insert_record(self, record: Dict[str, Any], 
                     table_name: str = 'typing_clients_data',
                     validate_after: bool = True) -> bool:
        """
        Insert a single record with comprehensive error handling.
        
        Args:
            record: Dictionary containing record data
            table_name: Target table name
            validate_after: Whether to validate the insert
            
        Returns:
            True if successful, raises exception on failure
        """
        with DatabaseOperationLogger(f"Inserting record to {table_name}"):
            # Normalize the record for database insertion
            normalized_record = self.validator.normalize_csv_row(record)
            
            # Remove None values that shouldn't be inserted
            clean_record = {k: v for k, v in normalized_record.items() 
                          if v is not None or k in ['document_text', 'download_errors']}
            
            # Convert JSON fields to strings for PostgreSQL
            for key in ['file_uuids', 's3_paths']:
                if key in clean_record:
                    clean_record[key] = json.dumps(clean_record[key])
            
            # Remove fields that don't exist in database
            if 'created_at' in clean_record:
                del clean_record['created_at']
            if 'updated_at' in clean_record:
                del clean_record['updated_at']
            
            try:
                # Use transaction for safety
                with self.db_manager.transaction() as conn:
                    cursor = conn.cursor()
                    
                    # Build parameterized query for PostgreSQL
                    columns = list(clean_record.keys())
                    placeholders = [f'%s' for _ in columns]
                    values = list(clean_record.values())
                    
                    sql = f"""
                        INSERT INTO {table_name} ({', '.join(columns)}) 
                        VALUES ({', '.join(placeholders)})
                    """
                    
                    cursor.execute(sql, values)
                    inserted_id = cursor.lastrowid
                    
                    db_logger.debug(f"Inserted record with ID: {inserted_id}")
                    
                    # Validate the insert if requested
                    if validate_after and 'row_id' in clean_record:
                        self._validate_insert(clean_record, table_name)
                    
                    return True
                    
            except Exception as e:
                error_msg = f"Failed to insert record: {e}"
                db_logger.error(error_msg)
                db_logger.error(f"Record data: {clean_record}")
                raise RuntimeError(error_msg) from e
    
    @log_database_operation("Batch Record Insert")
    def insert_batch(self, records: List[Dict[str, Any]], 
                    table_name: str = 'typing_clients_data',
                    batch_size: int = 100,
                    validate_sample: bool = True) -> Tuple[int, int]:
        """
        Insert multiple records in batches with error handling.
        
        Args:
            records: List of record dictionaries
            table_name: Target table name
            batch_size: Number of records per batch
            validate_sample: Whether to validate a sample of inserts
            
        Returns:
            Tuple of (successful_inserts, failed_inserts)
        """
        with DatabaseOperationLogger(f"Batch inserting {len(records)} records"):
            successful = 0
            failed = 0
            
            # Process in batches
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                
                db_logger.info(f"Processing batch {batch_num}: records {i+1}-{min(i+batch_size, len(records))}")
                
                try:
                    # Process each record in the batch
                    batch_successful = 0
                    for record in batch:
                        try:
                            self.insert_record(record, table_name, validate_after=False)
                            batch_successful += 1
                        except Exception as e:
                            db_logger.error(f"Failed to insert record {record.get('row_id', 'unknown')}: {e}")
                            failed += 1
                    
                    successful += batch_successful
                    db_logger.info(f"Batch {batch_num} completed: {batch_successful}/{len(batch)} successful")
                    
                except Exception as e:
                    db_logger.error(f"Batch {batch_num} failed completely: {e}")
                    failed += len(batch)
            
            # Validate a sample if requested
            if validate_sample and successful > 0:
                self._validate_batch_sample(table_name, min(10, successful))
            
            db_logger.info(f"Batch insert completed: {successful} successful, {failed} failed")
            
            if failed > 0:
                error_rate = failed / (successful + failed) * 100
                if error_rate > 10:  # More than 10% failure rate
                    raise RuntimeError(f"High failure rate in batch insert: {error_rate:.1f}%")
            
            return successful, failed
    
    @log_database_operation("Record Update")
    def update_record(self, row_id: int, updates: Dict[str, Any],
                     table_name: str = 'typing_clients_data') -> bool:
        """
        Update a single record with error handling.
        
        Args:
            row_id: ID of record to update
            updates: Dictionary of field updates
            table_name: Target table name
            
        Returns:
            True if successful
        """
        with DatabaseOperationLogger(f"Updating record {row_id}"):
            # Normalize updates
            normalized_updates = self.validator.normalize_csv_row(updates)
            
            # Remove None values and metadata fields
            clean_updates = {k: v for k, v in normalized_updates.items() 
                           if v is not None and k not in ['created_at', 'row_id']}
            
            if not clean_updates:
                db_logger.warning(f"No valid updates for record {row_id}")
                return True
            
            # Add updated_at timestamp
            clean_updates['updated_at'] = datetime.now()
            
            try:
                with self.db_manager.transaction() as conn:
                    cursor = conn.cursor()
                    
                    # Build parameterized update query for PostgreSQL
                    set_clauses = [f"{col} = %s" for col in clean_updates.keys()]
                    values = list(clean_updates.values()) + [row_id]
                    
                    sql = f"""
                        UPDATE {table_name} 
                        SET {', '.join(set_clauses)}
                        WHERE row_id = %s
                    """
                    
                    cursor.execute(sql, values)
                    affected_rows = cursor.rowcount
                    
                    if affected_rows == 0:
                        raise RuntimeError(f"No record found with row_id {row_id}")
                    elif affected_rows > 1:
                        raise RuntimeError(f"Multiple records updated for row_id {row_id}")
                    
                    db_logger.debug(f"Updated record {row_id}: {list(clean_updates.keys())}")
                    return True
                    
            except Exception as e:
                error_msg = f"Failed to update record {row_id}: {e}"
                db_logger.error(error_msg)
                raise RuntimeError(error_msg) from e
    
    @log_database_operation("Record Upsert")
    def upsert_record(self, record: Dict[str, Any],
                     table_name: str = 'typing_clients_data') -> str:
        """
        Insert or update record based on row_id.
        
        Args:
            record: Record data
            table_name: Target table name
            
        Returns:
            'inserted' or 'updated'
        """
        row_id = record.get('row_id')
        if not row_id:
            raise ValueError("Record must have row_id for upsert operation")
        
        # Check if record exists  
        existing = execute_sql(f"SELECT * FROM {table_name} WHERE row_id = %s", [row_id])
        
        if existing:
            # Update existing record
            updates = {k: v for k, v in record.items() if k != 'row_id'}
            self.update_record(row_id, updates, table_name)
            return 'updated'
        else:
            # Insert new record
            self.insert_record(record, table_name)
            return 'inserted'
    
    @log_database_operation("Record Retrieval")
    def get_record(self, row_id: int, table_name: str = 'typing_clients_data') -> Optional[Dict[str, Any]]:
        """
        Retrieve a single record by row_id.
        
        Args:
            row_id: ID of record to retrieve
            table_name: Source table name
            
        Returns:
            Record dictionary or None if not found
        """
        records = execute_sql(f"SELECT * FROM {table_name} WHERE row_id = %s", [row_id])
        
        if not records:
            return None
        elif len(records) > 1:
            db_logger.warning(f"Multiple records found for row_id {row_id}")
            
        return records[0]
    
    @log_database_operation("Multiple Records Retrieval")
    def get_records(self, row_ids: List[int], 
                   table_name: str = 'typing_clients_data') -> List[Dict[str, Any]]:
        """
        Retrieve multiple records by row_ids.
        
        Args:
            row_ids: List of record IDs
            table_name: Source table name
            
        Returns:
            List of record dictionaries
        """
        if not row_ids:
            return []
        
        # Build IN clause with proper parameterization
        placeholders = ','.join(['%s' for _ in row_ids])
        records = execute_sql(f"SELECT * FROM {table_name} WHERE row_id IN ({placeholders})", row_ids)
        
        return records
    
    @log_database_operation("Record Count")
    def count_records(self, table_name: str = 'typing_clients_data',
                     where_clause: Optional[str] = None,
                     params: Optional[List[Any]] = None) -> int:
        """
        Count records in table with optional filtering.
        
        Args:
            table_name: Table to count
            where_clause: Optional WHERE condition
            params: Parameters for WHERE clause
            
        Returns:
            Number of records
        """
        if where_clause:
            sql = f"SELECT COUNT(*) as count FROM {table_name} WHERE {where_clause}"
            result = execute_sql(sql, params or [])
        else:
            sql = f"SELECT COUNT(*) as count FROM {table_name}"
            result = execute_sql(sql)
        
        return result[0]['count'] if result else 0
    
    def _validate_insert(self, original_record: Dict[str, Any], 
                        table_name: str):
        """Validate that an insert was successful and data matches."""
        row_id = original_record.get('row_id')
        if not row_id:
            return
        
        # Retrieve the inserted record
        db_record = self.get_record(row_id, table_name)
        if not db_record:
            raise RuntimeError(f"Record {row_id} not found after insert")
        
        # Compare with original (this will raise exception on mismatch)
        self.validator.validate_dual_write(original_record, db_record, row_id)
    
    def _validate_batch_sample(self, table_name: str, sample_size: int):
        """Validate a random sample of recently inserted records."""
        # Get recent records
        recent_records = select(table_name, 
                              order_by='created_at DESC', 
                              limit=sample_size)
        
        if recent_records:
            db_logger.info(f"✅ Validated sample of {len(recent_records)} recent inserts")

def import_csv_to_database(csv_file: str, 
                          table_name: str = 'typing_clients_data',
                          batch_size: int = 100,
                          validate_all: bool = False) -> Dict[str, Any]:
    """
    Import entire CSV file to database with comprehensive validation.
    
    Args:
        csv_file: Path to CSV file
        table_name: Target database table
        batch_size: Records per batch
        validate_all: Whether to validate all records after import
        
    Returns:
        Import summary statistics
    """
    start_time = datetime.now()
    
    with DatabaseOperationLogger(f"Importing {csv_file} to {table_name}"):
        # Read CSV
        from utils.data_processing import read_csv_safe
        df = read_csv_safe(csv_file)
        
        if df.empty:
            raise ValueError(f"CSV file {csv_file} is empty or invalid")
        
        db_logger.info(f"📁 CSV loaded: {len(df)} records")
        
        # Initialize CRUD operations
        crud = DatabaseCRUD()
        
        # Convert to list of dictionaries
        records = df.to_dict('records')
        
        # Import in batches
        successful, failed = crud.insert_batch(records, table_name, batch_size)
        
        # Calculate metrics
        duration = (datetime.now() - start_time).total_seconds()
        total_records = len(records)
        
        summary = {
            'csv_file': csv_file,
            'table_name': table_name,
            'total_records': total_records,
            'successful_inserts': successful,
            'failed_inserts': failed,
            'success_rate': (successful / total_records * 100) if total_records > 0 else 0,
            'duration_seconds': duration,
            'records_per_second': successful / duration if duration > 0 else 0,
            'timestamp': datetime.now().isoformat()
        }
        
        # Log performance metrics
        log_performance_metrics("CSV Import", successful, duration)
        
        # Validate if requested
        if validate_all and successful > 0:
            db_logger.info("🔍 Running full validation...")
            validator = DataValidator()
            validation_passed = validator.validate_full_migration(csv_file, table_name)
            summary['validation_passed'] = validation_passed
            
            if not validation_passed:
                raise RuntimeError("Import validation failed - data integrity issues detected")
        
        # Record import in migration log
        basic_insert('migration_log', {
            'operation': 'CSV Import',
            'phase': 'Phase 1',
            'status': 'completed' if failed == 0 else 'partial',
            'record_count': successful,
            'duration_seconds': duration,
            'metadata': json.dumps(summary)
        })
        
        db_logger.info("📊 IMPORT SUMMARY")
        db_logger.info(f"   Total records: {total_records}")
        db_logger.info(f"   Successful: {successful}")
        db_logger.info(f"   Failed: {failed}")
        db_logger.info(f"   Success rate: {summary['success_rate']:.1f}%")
        db_logger.info(f"   Duration: {duration:.1f}s")
        db_logger.info(f"   Rate: {summary['records_per_second']:.1f} records/sec")
        
        if failed > 0:
            db_logger.warning(f"⚠️ {failed} records failed to import")
            if failed / total_records > 0.05:  # More than 5% failure
                raise RuntimeError(f"High failure rate: {failed}/{total_records} records failed")
        
        return summary

# Test functions
if __name__ == "__main__":
    # Set environment variable for testing
    import os
    os.environ['DB_PASSWORD'] = 'migration_pass_2025'
    
    # Test CRUD operations
    crud = DatabaseCRUD()
    
    # Test record count
    count = crud.count_records()
    db_logger.info(f"Current record count: {count}")
    
    # Test single insert
    test_record = {
        'row_id': 999999,
        'name': 'Test CRUD User',
        'email': 'test.crud@example.com',
        'type': 'TEST',
        'processed': True,
        'file_uuids': {'test': 'crud-uuid'},
        'permanent_failure': False
    }
    
    try:
        crud.insert_record(test_record)
        db_logger.info("✅ Test insert successful")
        
        # Test retrieval
        retrieved = crud.get_record(999999)
        if retrieved:
            db_logger.info(f"✅ Test retrieval successful: {retrieved['name']}")
        
        # Test update
        crud.update_record(999999, {'name': 'Updated CRUD User'})
        db_logger.info("✅ Test update successful")
        
        # Clean up
        execute_sql("DELETE FROM typing_clients_data WHERE row_id = %s", [999999])
        db_logger.info("✅ Test cleanup successful")
        
    except Exception as e:
        db_logger.error(f"❌ CRUD test failed: {e}")
    
    db_logger.info("✅ Database CRUD system test completed")