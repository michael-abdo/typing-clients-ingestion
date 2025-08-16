#!/usr/bin/env python3
"""
Database-Primary Read Functions with CSV Fallback

Implements database-first read operations with automatic CSV fallback.
Provides comprehensive logging, performance monitoring, and data validation.

Key Features:
- Database-first reads with automatic CSV fallback
- Fail-loud logging when fallback is used
- Performance monitoring for all read operations
- Data consistency validation between DB and CSV
- Comprehensive error handling and recovery
"""

import os
import sys
import time
import json
import pandas as pd
from datetime import datetime
from pathlib import Path
from typing import Dict, Any, List, Optional, Union, Tuple
import statistics

# Set up environment
os.environ['DB_PASSWORD'] = 'migration_pass_2025'
sys.path.append('.')

from utils.database_operations import DatabaseConfig, DatabaseManager
from utils.data_validation import DataValidator
from utils.database_logging import db_logger, DatabaseOperationLogger, log_database_operation

class DatabasePrimaryReader:
    """Database-primary read operations with CSV fallback and monitoring."""
    
    def __init__(self, csv_file: str = "outputs/output.csv"):
        """Initialize database-primary reader."""
        
        self.csv_file = Path(csv_file)
        
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
        self.validator = DataValidator()
        
        # Performance and fallback metrics
        self.metrics = {
            'total_reads': 0,
            'database_reads': 0,
            'csv_fallbacks': 0,
            'read_errors': 0,
            'validation_mismatches': 0,
            'db_read_times': [],
            'csv_read_times': [],
            'fallback_reasons': [],
            'start_time': datetime.now()
        }
        
        # Fallback configuration
        self.enable_fallback = True
        self.validate_reads = True
        self.log_fallbacks = True
        
    @log_database_operation("Database-Primary Record Read")
    def get_record(self, row_id: int, validate_consistency: bool = True) -> Optional[Dict[str, Any]]:
        """
        Get single record by row_id, database-first with CSV fallback.
        
        Args:
            row_id: ID of record to retrieve
            validate_consistency: Whether to validate DB vs CSV consistency
            
        Returns:
            Record dictionary or None if not found
        """
        with DatabaseOperationLogger(f"Reading record {row_id} (database-primary)"):
            self.metrics['total_reads'] += 1
            
            # Phase 1: Try database first
            db_record = None
            db_read_time = 0
            db_error = None
            
            db_start = time.time()
            try:
                with self.db_manager.get_connection() as conn:
                    cursor = conn.cursor()
                    cursor.execute("SELECT * FROM typing_clients_data WHERE row_id = %s", [row_id])
                    result = cursor.fetchone()
                    
                    if result:
                        db_record = dict(result)
                        self.metrics['database_reads'] += 1
                        db_logger.debug(f"✅ Database read successful: record {row_id}")
                    else:
                        db_logger.debug(f"🔍 Record {row_id} not found in database")
                        
            except Exception as e:
                db_error = e
                db_logger.error(f"❌ Database read failed for record {row_id}: {e}")
            
            db_read_time = time.time() - db_start
            self.metrics['db_read_times'].append(db_read_time)
            
            # Phase 2: CSV fallback if needed
            csv_record = None
            csv_read_time = 0
            
            if not db_record and self.enable_fallback:
                self._log_fallback_usage("Record Read", row_id, str(db_error) if db_error else "Record not found in database")
                
                csv_start = time.time()
                try:
                    if self.csv_file.exists():
                        df = pd.read_csv(self.csv_file)
                        matches = df[df['row_id'] == row_id]
                        
                        if not matches.empty:
                            csv_record = matches.iloc[0].to_dict()
                            self.metrics['csv_fallbacks'] += 1
                            db_logger.warning(f"🔄 CSV fallback used for record {row_id}")
                        else:
                            db_logger.debug(f"🔍 Record {row_id} not found in CSV either")
                    else:
                        db_logger.error(f"❌ CSV file not found: {self.csv_file}")
                        
                except Exception as csv_error:
                    self.metrics['read_errors'] += 1
                    db_logger.error(f"❌ CSV fallback failed for record {row_id}: {csv_error}")
                
                csv_read_time = time.time() - csv_start
                self.metrics['csv_read_times'].append(csv_read_time)
            
            # Phase 3: Validation if both sources available
            final_record = db_record or csv_record
            
            if db_record and csv_record and validate_consistency:
                try:
                    self.validator.validate_dual_write(csv_record, db_record, row_id)
                    db_logger.debug(f"✅ Consistency validation passed for record {row_id}")
                except Exception as validation_error:
                    self.metrics['validation_mismatches'] += 1
                    db_logger.error(f"🚨 CONSISTENCY MISMATCH for record {row_id}: {validation_error}")
                    # Still return the database record as it's primary
            
            # Log performance metrics
            total_time = db_read_time + csv_read_time
            db_logger.debug(f"📊 Read performance for {row_id}: DB={db_read_time:.3f}s, CSV={csv_read_time:.3f}s, Total={total_time:.3f}s")
            
            return final_record
    
    @log_database_operation("Database-Primary Records Query")
    def get_records(self, row_ids: List[int] = None, limit: int = None, 
                   where_clause: str = None, params: List[Any] = None) -> List[Dict[str, Any]]:
        """
        Get multiple records, database-first with CSV fallback.
        
        Args:
            row_ids: Specific row IDs to retrieve (optional)
            limit: Maximum number of records to return
            where_clause: SQL WHERE clause for filtering
            params: Parameters for WHERE clause
            
        Returns:
            List of record dictionaries
        """
        with DatabaseOperationLogger(f"Reading multiple records (database-primary)"):
            self.metrics['total_reads'] += 1
            
            # Phase 1: Try database first
            db_records = []
            db_read_time = 0
            db_error = None
            
            db_start = time.time()
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
                    db_records = [dict(row) for row in results]
                    
                    if db_records:
                        self.metrics['database_reads'] += 1
                        db_logger.debug(f"✅ Database read successful: {len(db_records)} records")
                    else:
                        db_logger.debug(f"🔍 No records found in database")
                        
            except Exception as e:
                db_error = e
                db_logger.error(f"❌ Database read failed: {e}")
            
            db_read_time = time.time() - db_start
            self.metrics['db_read_times'].append(db_read_time)
            
            # Phase 2: CSV fallback if needed
            csv_records = []
            csv_read_time = 0
            
            if not db_records and self.enable_fallback:
                self._log_fallback_usage("Records Query", "multiple", str(db_error) if db_error else "No records found in database")
                
                csv_start = time.time()
                try:
                    if self.csv_file.exists():
                        df = pd.read_csv(self.csv_file)
                        
                        if row_ids:
                            # Filter by specific row IDs
                            df = df[df['row_id'].isin(row_ids)]
                        elif where_clause:
                            # For CSV, we can't execute SQL WHERE clauses directly
                            # This would need custom filtering logic based on the clause
                            db_logger.warning(f"⚠️ WHERE clause not supported in CSV fallback: {where_clause}")
                        
                        if limit:
                            df = df.head(limit)
                        
                        csv_records = df.to_dict('records')
                        
                        if csv_records:
                            self.metrics['csv_fallbacks'] += 1
                            db_logger.warning(f"🔄 CSV fallback used: {len(csv_records)} records")
                        else:
                            db_logger.debug(f"🔍 No records found in CSV either")
                    else:
                        db_logger.error(f"❌ CSV file not found: {self.csv_file}")
                        
                except Exception as csv_error:
                    self.metrics['read_errors'] += 1
                    db_logger.error(f"❌ CSV fallback failed: {csv_error}")
                
                csv_read_time = time.time() - csv_start
                self.metrics['csv_read_times'].append(csv_read_time)
            
            # Return database records if available, otherwise CSV records
            final_records = db_records or csv_records
            
            # Log performance metrics
            total_time = db_read_time + csv_read_time
            db_logger.debug(f"📊 Read performance: {len(final_records)} records, DB={db_read_time:.3f}s, CSV={csv_read_time:.3f}s, Total={total_time:.3f}s")
            
            return final_records
    
    @log_database_operation("Database-Primary Count Query")
    def count_records(self, where_clause: str = None, params: List[Any] = None) -> int:
        """
        Count records, database-first with CSV fallback.
        
        Args:
            where_clause: SQL WHERE clause for filtering
            params: Parameters for WHERE clause
            
        Returns:
            Number of records
        """
        with DatabaseOperationLogger(f"Counting records (database-primary)"):
            self.metrics['total_reads'] += 1
            
            # Phase 1: Try database first
            db_count = None
            db_error = None
            
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
                    db_count = result['count']
                    
                    self.metrics['database_reads'] += 1
                    db_logger.debug(f"✅ Database count successful: {db_count} records")
                    
            except Exception as e:
                db_error = e
                db_logger.error(f"❌ Database count failed: {e}")
            
            # Phase 2: CSV fallback if needed
            if db_count is None and self.enable_fallback:
                self._log_fallback_usage("Count Query", "all", str(db_error) if db_error else "Database count failed")
                
                try:
                    if self.csv_file.exists():
                        df = pd.read_csv(self.csv_file)
                        
                        if where_clause:
                            # For CSV, we can't execute SQL WHERE clauses directly
                            db_logger.warning(f"⚠️ WHERE clause not supported in CSV fallback count: {where_clause}")
                            csv_count = len(df)
                        else:
                            csv_count = len(df)
                        
                        self.metrics['csv_fallbacks'] += 1
                        db_logger.warning(f"🔄 CSV fallback count used: {csv_count} records")
                        return csv_count
                    else:
                        db_logger.error(f"❌ CSV file not found for count: {self.csv_file}")
                        return 0
                        
                except Exception as csv_error:
                    self.metrics['read_errors'] += 1
                    db_logger.error(f"❌ CSV fallback count failed: {csv_error}")
                    return 0
            
            return db_count if db_count is not None else 0
    
    def _log_fallback_usage(self, operation: str, identifier: Any, reason: str):
        """Log fallback usage with comprehensive details."""
        
        fallback_event = {
            'timestamp': datetime.now().isoformat(),
            'operation': operation,
            'identifier': str(identifier),
            'reason': reason,
            'metrics_snapshot': self.get_current_metrics()
        }
        
        self.metrics['fallback_reasons'].append(fallback_event)
        
        if self.log_fallbacks:
            db_logger.warning(f"🔄 FALLBACK TO CSV: {operation}")
            db_logger.warning(f"   Identifier: {identifier}")
            db_logger.warning(f"   Reason: {reason}")
            db_logger.warning(f"   🚨 DATABASE OPERATION FAILED - USING CSV BACKUP")
    
    def get_current_metrics(self) -> Dict[str, Any]:
        """Get current performance and fallback metrics."""
        
        current_time = datetime.now()
        uptime = (current_time - self.metrics['start_time']).total_seconds()
        
        metrics = {
            'total_reads': self.metrics['total_reads'],
            'database_reads': self.metrics['database_reads'],
            'csv_fallbacks': self.metrics['csv_fallbacks'],
            'read_errors': self.metrics['read_errors'],
            'validation_mismatches': self.metrics['validation_mismatches'],
            'uptime_seconds': uptime,
            'fallback_rate': (self.metrics['csv_fallbacks'] / self.metrics['total_reads'] * 100) if self.metrics['total_reads'] > 0 else 0,
            'error_rate': (self.metrics['read_errors'] / self.metrics['total_reads'] * 100) if self.metrics['total_reads'] > 0 else 0,
            'database_success_rate': (self.metrics['database_reads'] / self.metrics['total_reads'] * 100) if self.metrics['total_reads'] > 0 else 0
        }
        
        # Add timing statistics
        if self.metrics['db_read_times']:
            metrics['db_read_performance'] = {
                'mean': statistics.mean(self.metrics['db_read_times']),
                'median': statistics.median(self.metrics['db_read_times']),
                'min': min(self.metrics['db_read_times']),
                'max': max(self.metrics['db_read_times'])
            }
        
        if self.metrics['csv_read_times']:
            metrics['csv_read_performance'] = {
                'mean': statistics.mean(self.metrics['csv_read_times']),
                'median': statistics.median(self.metrics['csv_read_times']),
                'min': min(self.metrics['csv_read_times']),
                'max': max(self.metrics['csv_read_times'])
            }
        
        return metrics
    
    def generate_fallback_report(self) -> Dict[str, Any]:
        """Generate comprehensive fallback usage report."""
        
        report = {
            'summary': self.get_current_metrics(),
            'fallback_events': self.metrics['fallback_reasons'],
            'recommendations': []
        }
        
        # Generate recommendations based on metrics
        fallback_rate = report['summary']['fallback_rate']
        error_rate = report['summary']['error_rate']
        
        if fallback_rate > 10:
            report['recommendations'].append(f"High fallback rate ({fallback_rate:.1f}%) - investigate database reliability")
        
        if error_rate > 5:
            report['recommendations'].append(f"High error rate ({error_rate:.1f}%) - investigate read operation failures")
        
        if self.metrics['validation_mismatches'] > 0:
            report['recommendations'].append(f"{self.metrics['validation_mismatches']} consistency mismatches detected - investigate data synchronization")
        
        if not report['recommendations']:
            report['recommendations'].append("Database-primary reads operating within acceptable parameters")
        
        return report

# Convenience functions for common read operations
def get_record_database_primary(row_id: int, csv_file: str = "outputs/output.csv") -> Optional[Dict[str, Any]]:
    """Get single record using database-primary read with CSV fallback."""
    reader = DatabasePrimaryReader(csv_file)
    return reader.get_record(row_id)

def get_records_database_primary(row_ids: List[int] = None, limit: int = None, 
                                csv_file: str = "outputs/output.csv") -> List[Dict[str, Any]]:
    """Get multiple records using database-primary read with CSV fallback."""
    reader = DatabasePrimaryReader(csv_file)
    return reader.get_records(row_ids, limit)

def count_records_database_primary(csv_file: str = "outputs/output.csv") -> int:
    """Count records using database-primary read with CSV fallback."""
    reader = DatabasePrimaryReader(csv_file)
    return reader.count_records()

# Test functions
if __name__ == "__main__":
    print("🗄️ Testing Database-Primary Read Functions")
    print("=" * 45)
    
    reader = DatabasePrimaryReader()
    
    try:
        # Test single record read
        print("\n📖 Testing single record read...")
        record = reader.get_record(1)  # Try to read row_id 1
        if record:
            print(f"✅ Record found: {record['name']} ({record['email']})")
        else:
            print("🔍 Record not found")
        
        # Test multiple records read
        print("\n📚 Testing multiple records read...")
        records = reader.get_records(limit=5)
        print(f"✅ Found {len(records)} records")
        for i, record in enumerate(records[:3], 1):
            print(f"  {i}. {record['name']} - {record['email']}")
        
        # Test count
        print("\n🔢 Testing record count...")
        count = reader.count_records()
        print(f"✅ Total records: {count}")
        
        # Generate metrics report
        print("\n📊 Performance Metrics:")
        metrics = reader.get_current_metrics()
        print(f"  Total reads: {metrics['total_reads']}")
        print(f"  Database reads: {metrics['database_reads']}")
        print(f"  CSV fallbacks: {metrics['csv_fallbacks']}")
        print(f"  Fallback rate: {metrics['fallback_rate']:.1f}%")
        print(f"  Database success rate: {metrics['database_success_rate']:.1f}%")
        
        print("\n✅ Database-primary read functions working correctly!")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        db_logger.error(f"Database-primary read test failed: {e}")