#!/usr/bin/env python3
"""
Dual-Write System for Database Migration

Implements simultaneous writing to both CSV and database with fail-fast validation.
Ensures perfect data synchronization during migration phase.
"""

import os
import csv
import json
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional, Union
from pathlib import Path
import shutil

# Import our utilities
from utils.database_operations import DatabaseConfig, DatabaseManager
from utils.database_crud import DatabaseCRUD
from utils.data_validation import DataValidator
from utils.database_logging import (
    db_logger, DatabaseOperationLogger, log_database_operation,
    log_dual_write_comparison, log_fallback_usage
)

class DualWriteManager:
    """Manages dual-write operations with fail-fast validation."""
    
    def __init__(self, csv_file: str = "outputs/output.csv"):
        """Initialize dual-write manager."""
        self.csv_file = Path(csv_file)
        self.backup_csv = self.csv_file.with_suffix(f".backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv")
        
        # Set up database with explicit config
        os.environ['DB_PASSWORD'] = 'migration_pass_2025'
        self.db_config = DatabaseConfig(
            db_type='postgresql',
            host='localhost',
            port=5432,
            database='typing_clients_uuid',
            username='migration_user',
            password='migration_pass_2025'
        )
        
        self.db_manager = DatabaseManager(self.db_config)
        self.crud = DatabaseCRUD()
        self.validator = DataValidator()
        
        # Initialize dual-write mode
        self.dual_write_enabled = False
        self.fallback_to_csv = True
        
        # Statistics
        self.stats = {
            'total_writes': 0,
            'successful_dual_writes': 0,
            'csv_only_writes': 0,
            'db_only_writes': 0,
            'validation_failures': 0,
            'fallback_events': 0
        }
        
    @log_database_operation("Dual Write Initialization")
    def initialize_dual_write(self) -> bool:
        """Initialize dual-write mode with safety checks."""
        
        with DatabaseOperationLogger("Initializing dual-write mode"):
            try:
                # Create backup of current CSV
                if self.csv_file.exists():
                    shutil.copy2(self.csv_file, self.backup_csv)
                    db_logger.info(f"📁 CSV backup created: {self.backup_csv}")
                
                # Test database connection
                with self.db_manager.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT COUNT(*) as count FROM typing_clients_data")
                    db_count = cursor.fetchone()['count']
                    db_logger.info(f"🗄️ Database ready: {db_count} existing records")
                
                # Load CSV for validation
                if self.csv_file.exists():
                    df = pd.read_csv(self.csv_file)
                    csv_count = len(df)
                    db_logger.info(f"📊 CSV ready: {csv_count} records")
                else:
                    db_logger.info("📊 CSV file doesn't exist - will be created")
                
                self.dual_write_enabled = True
                db_logger.info("✅ Dual-write mode initialized successfully")
                return True
                
            except Exception as e:
                db_logger.error(f"❌ Failed to initialize dual-write: {e}")
                return False
    
    @log_database_operation("Dual Write Record")
    def write_record(self, record: Dict[str, Any], 
                    validate_immediately: bool = True) -> bool:
        """
        Write record to both CSV and database with validation.
        
        Args:
            record: Record data to write
            validate_immediately: Whether to validate the write immediately
            
        Returns:
            True if successful (may fall back to CSV-only)
        """
        if not self.dual_write_enabled:
            raise RuntimeError("Dual-write mode not initialized")
        
        self.stats['total_writes'] += 1
        
        with DatabaseOperationLogger(f"Dual-writing record {record.get('row_id', 'unknown')}"):
            try:
                # Phase 1: Write to database first (fail fast if DB issues)
                try:
                    self.crud.insert_record(record, validate_after=False)
                    db_write_success = True
                    db_logger.debug(f"✅ Database write successful")
                except Exception as db_error:
                    db_write_success = False
                    db_logger.error(f"❌ Database write failed: {db_error}")
                    
                    if not self.fallback_to_csv:
                        # Fail fast if no fallback allowed
                        raise RuntimeError(f"Database write failed and fallback disabled: {db_error}")
                    
                    # Log fallback usage
                    log_fallback_usage("Record Write", str(db_error))
                    self.stats['fallback_events'] += 1
                
                # Phase 2: Write to CSV
                try:
                    self._write_to_csv(record)
                    csv_write_success = True
                    db_logger.debug(f"✅ CSV write successful")
                except Exception as csv_error:
                    csv_write_success = False
                    db_logger.error(f"❌ CSV write failed: {csv_error}")
                    
                    if not db_write_success:
                        # Both failed - this is critical
                        raise RuntimeError(f"Both CSV and database writes failed!")
                
                # Phase 3: Validate if both succeeded
                if db_write_success and csv_write_success and validate_immediately:
                    try:
                        self._validate_dual_write(record)
                        self.stats['successful_dual_writes'] += 1
                        db_logger.debug(f"✅ Dual-write validation successful")
                    except Exception as validation_error:
                        self.stats['validation_failures'] += 1
                        db_logger.error(f"❌ Dual-write validation failed: {validation_error}")
                        # FAIL LOUD - validation failure is critical
                        raise validation_error
                
                # Update statistics
                if db_write_success and not csv_write_success:
                    self.stats['db_only_writes'] += 1
                elif csv_write_success and not db_write_success:
                    self.stats['csv_only_writes'] += 1
                
                return True
                
            except Exception as e:
                db_logger.error(f"💥 Dual-write failed completely: {e}")
                raise
    
    @log_database_operation("Batch Dual Write")
    def write_batch(self, records: List[Dict[str, Any]], 
                   batch_size: int = 50) -> Dict[str, Any]:
        """
        Write multiple records with dual-write validation.
        
        Args:
            records: List of records to write
            batch_size: Records per batch
            
        Returns:
            Statistics dictionary
        """
        with DatabaseOperationLogger(f"Dual-write batch: {len(records)} records"):
            batch_stats = {
                'total_records': len(records),
                'successful_writes': 0,
                'failed_writes': 0,
                'validation_errors': 0,
                'start_time': datetime.now()
            }
            
            # Process in batches
            for i in range(0, len(records), batch_size):
                batch = records[i:i + batch_size]
                batch_num = (i // batch_size) + 1
                
                db_logger.info(f"📦 Processing batch {batch_num}: {len(batch)} records")
                
                for record in batch:
                    try:
                        self.write_record(record, validate_immediately=True)
                        batch_stats['successful_writes'] += 1
                    except Exception as e:
                        batch_stats['failed_writes'] += 1
                        db_logger.error(f"❌ Failed to write record {record.get('row_id')}: {e}")
                        
                        # Fail fast if error rate too high
                        error_rate = batch_stats['failed_writes'] / (batch_stats['successful_writes'] + batch_stats['failed_writes'])
                        if error_rate > 0.1:  # More than 10% failure
                            raise RuntimeError(f"High error rate in batch write: {error_rate:.1%}")
            
            batch_stats['end_time'] = datetime.now()
            batch_stats['duration'] = (batch_stats['end_time'] - batch_stats['start_time']).total_seconds()
            
            db_logger.info(f"📊 Batch complete: {batch_stats['successful_writes']} success, {batch_stats['failed_writes']} failed")
            return batch_stats
    
    def _write_to_csv(self, record: Dict[str, Any]):
        """Write single record to CSV file."""
        
        # Normalize record for CSV
        csv_record = self.validator.normalize_csv_row(record)
        
        # Handle JSON fields for CSV
        for key in ['file_uuids', 's3_paths']:
            if key in csv_record and isinstance(csv_record[key], dict):
                csv_record[key] = json.dumps(csv_record[key])
        
        # Determine if we need to create new file or append
        file_exists = self.csv_file.exists()
        
        if not file_exists:
            # Create new file with header
            df = pd.DataFrame([csv_record])
            df.to_csv(self.csv_file, index=False)
        else:
            # Check if record already exists (update vs insert)
            existing_df = pd.read_csv(self.csv_file)
            row_id = csv_record.get('row_id')
            
            if row_id and row_id in existing_df['row_id'].values:
                # Update existing record
                for key, value in csv_record.items():
                    existing_df.loc[existing_df['row_id'] == row_id, key] = value
                existing_df.to_csv(self.csv_file, index=False)
            else:
                # Append new record
                new_df = pd.concat([existing_df, pd.DataFrame([csv_record])], ignore_index=True)
                new_df.to_csv(self.csv_file, index=False)
    
    def _validate_dual_write(self, original_record: Dict[str, Any]):
        """Validate that dual-write was successful and data matches."""
        
        row_id = original_record.get('row_id')
        if not row_id:
            return  # Can't validate without row_id
        
        # Get record from database
        db_record = self.crud.get_record(row_id)
        if not db_record:
            raise RuntimeError(f"Record {row_id} not found in database after write")
        
        # Get record from CSV
        df = pd.read_csv(self.csv_file)
        csv_matches = df[df['row_id'] == row_id]
        
        if csv_matches.empty:
            raise RuntimeError(f"Record {row_id} not found in CSV after write")
        
        csv_record = csv_matches.iloc[0].to_dict()
        
        # Validate using our validator (this will raise exception on mismatch)
        self.validator.validate_dual_write(csv_record, db_record, row_id)
    
    @log_database_operation("Dual Write Statistics")
    def get_statistics(self) -> Dict[str, Any]:
        """Get dual-write statistics."""
        
        stats = self.stats.copy()
        
        if stats['total_writes'] > 0:
            stats['success_rate'] = stats['successful_dual_writes'] / stats['total_writes'] * 100
            stats['fallback_rate'] = stats['fallback_events'] / stats['total_writes'] * 100
            stats['validation_failure_rate'] = stats['validation_failures'] / stats['total_writes'] * 100
        else:
            stats['success_rate'] = 0
            stats['fallback_rate'] = 0
            stats['validation_failure_rate'] = 0
        
        return stats
    
    @log_database_operation("Dual Write Shutdown")
    def shutdown(self) -> Dict[str, Any]:
        """Shutdown dual-write mode and return final statistics."""
        
        with DatabaseOperationLogger("Shutting down dual-write mode"):
            final_stats = self.get_statistics()
            
            db_logger.info("📊 DUAL-WRITE FINAL STATISTICS")
            db_logger.info(f"   Total writes: {final_stats['total_writes']}")
            db_logger.info(f"   Successful dual-writes: {final_stats['successful_dual_writes']}")
            db_logger.info(f"   CSV-only writes: {final_stats['csv_only_writes']}")
            db_logger.info(f"   DB-only writes: {final_stats['db_only_writes']}")
            db_logger.info(f"   Validation failures: {final_stats['validation_failures']}")
            db_logger.info(f"   Fallback events: {final_stats['fallback_events']}")
            db_logger.info(f"   Success rate: {final_stats['success_rate']:.1f}%")
            
            self.dual_write_enabled = False
            
            return final_stats

# Test function
if __name__ == "__main__":
    # Test dual-write system
    dual_writer = DualWriteManager()
    
    try:
        # Initialize
        if not dual_writer.initialize_dual_write():
            raise RuntimeError("Failed to initialize dual-write")
        
        # Test single record
        test_record = {
            'row_id': 999998,
            'name': 'Dual Write Test',
            'email': 'dualwrite@example.com',
            'type': 'TEST',
            'processed': True,
            'file_uuids': {'test': 'dual-write-uuid'},
            'permanent_failure': False,
            'document_text': 'Test document for dual-write validation'
        }
        
        db_logger.info("🧪 Testing single record dual-write...")
        dual_writer.write_record(test_record)
        
        # Test batch write
        test_batch = [
            {
                'row_id': 999997,
                'name': 'Batch Test 1',
                'email': 'batch1@example.com',
                'type': 'BATCH_TEST',
                'processed': False
            },
            {
                'row_id': 999996,
                'name': 'Batch Test 2', 
                'email': 'batch2@example.com',
                'type': 'BATCH_TEST',
                'processed': True
            }
        ]
        
        db_logger.info("🧪 Testing batch dual-write...")
        batch_stats = dual_writer.write_batch(test_batch)
        
        # Get final statistics
        final_stats = dual_writer.shutdown()
        
        db_logger.info("✅ Dual-write system test completed successfully")
        
    except Exception as e:
        db_logger.error(f"❌ Dual-write test failed: {e}")
        dual_writer.shutdown()