#!/usr/bin/env python3
"""
Database-Primary Manager

Extends CSV operations with database-primary reads and fallback functionality.
Maintains backward compatibility while providing enhanced database-first operations.

Key Features:
- Drop-in replacement for CSV read operations
- Database-first reads with automatic CSV fallback
- Comprehensive logging and performance monitoring
- Fail-loud logging when fallback is used
- Data consistency validation
"""

import os
import sys
import pandas as pd
from pathlib import Path
from typing import Dict, Any, List, Optional, Union
from datetime import datetime

# Set up environment
os.environ['DB_PASSWORD'] = 'migration_pass_2025'
sys.path.append('.')

from utils.database_primary_reads import DatabasePrimaryReader
from utils.csv_manager import CSVManager
from utils.database_logging import db_logger

class DatabasePrimaryManager:
    """
    Database-primary manager that extends CSV operations with database-first reads.
    Provides backward compatibility with existing CSV operations.
    """
    
    def __init__(self, csv_path: str = "outputs/output.csv", enable_validation: bool = True):
        """
        Initialize database-primary manager.
        
        Args:
            csv_path: Path to CSV file for fallback operations
            enable_validation: Whether to validate consistency between DB and CSV
        """
        self.csv_path = csv_path
        self.enable_validation = enable_validation
        
        # Initialize database-primary reader
        self.db_reader = DatabasePrimaryReader(csv_path)
        
        # Initialize traditional CSV manager for fallback
        self.csv_manager = CSVManager(csv_path)
        
        # Track usage statistics
        self.usage_stats = {
            'database_reads': 0,
            'csv_fallbacks': 0,
            'total_operations': 0,
            'start_time': datetime.now()
        }
    
    def read_dataframe(self, dtype_spec: str = 'tracking', force_csv: bool = False) -> pd.DataFrame:
        """
        Read data as DataFrame, database-first with CSV fallback.
        
        Args:
            dtype_spec: Data type specification for CSV reads
            force_csv: Force CSV read (bypass database)
            
        Returns:
            DataFrame with data from database or CSV
        """
        self.usage_stats['total_operations'] += 1
        
        if force_csv:
            db_logger.info("🔄 Forced CSV read requested")
            self.usage_stats['csv_fallbacks'] += 1
            return self.csv_manager.read(dtype_spec)
        
        try:
            # Try database first
            db_logger.debug("🗄️ Attempting database-primary DataFrame read")
            records = self.db_reader.get_records()
            
            if records:
                df = pd.DataFrame(records)
                self.usage_stats['database_reads'] += 1
                db_logger.debug(f"✅ Database read successful: {len(df)} records")
                
                # Validate against CSV if enabled
                if self.enable_validation and Path(self.csv_path).exists():
                    try:
                        csv_df = self.csv_manager.read(dtype_spec)
                        self._validate_dataframe_consistency(df, csv_df)
                    except Exception as validation_error:
                        db_logger.warning(f"⚠️ DataFrame validation failed: {validation_error}")
                
                return df
            else:
                db_logger.warning("🔍 No records found in database, falling back to CSV")
                raise ValueError("No records in database")
                
        except Exception as db_error:
            # Fallback to CSV
            db_logger.warning(f"🔄 FALLBACK TO CSV: DataFrame read")
            db_logger.warning(f"   Reason: {db_error}")
            db_logger.warning(f"   🚨 DATABASE OPERATION FAILED - USING CSV BACKUP")
            
            self.usage_stats['csv_fallbacks'] += 1
            return self.csv_manager.read(dtype_spec)
    
    def get_record_by_id(self, row_id: int) -> Optional[Dict[str, Any]]:
        """
        Get single record by row_id, database-first with CSV fallback.
        
        Args:
            row_id: ID of record to retrieve
            
        Returns:
            Record dictionary or None if not found
        """
        self.usage_stats['total_operations'] += 1
        
        try:
            # Try database first
            record = self.db_reader.get_record(row_id, validate_consistency=self.enable_validation)
            
            if record:
                self.usage_stats['database_reads'] += 1
                return record
            else:
                db_logger.debug(f"🔍 Record {row_id} not found in database, checking CSV")
                raise ValueError(f"Record {row_id} not found in database")
                
        except Exception as db_error:
            # Fallback to CSV
            db_logger.warning(f"🔄 FALLBACK TO CSV: Record {row_id}")
            db_logger.warning(f"   Reason: {db_error}")
            db_logger.warning(f"   🚨 DATABASE OPERATION FAILED - USING CSV BACKUP")
            
            self.usage_stats['csv_fallbacks'] += 1
            
            try:
                df = self.csv_manager.read()
                matches = df[df['row_id'] == row_id]
                if not matches.empty:
                    return matches.iloc[0].to_dict()
                else:
                    return None
            except Exception as csv_error:
                db_logger.error(f"❌ CSV fallback also failed for record {row_id}: {csv_error}")
                return None
    
    def get_records_by_criteria(self, **criteria) -> List[Dict[str, Any]]:
        """
        Get records matching criteria, database-first with CSV fallback.
        
        Args:
            **criteria: Field-value pairs for filtering
            
        Returns:
            List of matching record dictionaries
        """
        self.usage_stats['total_operations'] += 1
        
        try:
            # For database, we need to construct WHERE clause
            # This is a simplified implementation - could be enhanced
            if len(criteria) == 1 and 'processed' in criteria:
                # Common case: filter by processed status
                where_clause = "processed = %s"
                params = [criteria['processed']]
                
                records = self.db_reader.get_records(where_clause=where_clause, params=params)
                
                if records:
                    self.usage_stats['database_reads'] += 1
                    db_logger.debug(f"✅ Database criteria read: {len(records)} records")
                    return records
                else:
                    raise ValueError("No matching records in database")
            else:
                # Complex criteria - fall back to full read and filter
                raise ValueError("Complex criteria require full dataset")
                
        except Exception as db_error:
            # Fallback to CSV
            db_logger.warning(f"🔄 FALLBACK TO CSV: Criteria search")
            db_logger.warning(f"   Reason: {db_error}")
            db_logger.warning(f"   🚨 DATABASE OPERATION FAILED - USING CSV BACKUP")
            
            self.usage_stats['csv_fallbacks'] += 1
            
            try:
                df = self.csv_manager.read()
                
                # Apply criteria filtering
                for field, value in criteria.items():
                    if field in df.columns:
                        df = df[df[field] == value]
                
                return df.to_dict('records')
                
            except Exception as csv_error:
                db_logger.error(f"❌ CSV fallback also failed for criteria search: {csv_error}")
                return []
    
    def count_records(self, **criteria) -> int:
        """
        Count records, database-first with CSV fallback.
        
        Args:
            **criteria: Optional filtering criteria
            
        Returns:
            Number of records
        """
        self.usage_stats['total_operations'] += 1
        
        try:
            if not criteria:
                # Simple count
                count = self.db_reader.count_records()
                self.usage_stats['database_reads'] += 1
                return count
            else:
                # Count with criteria - use get_records_by_criteria and count results
                records = self.get_records_by_criteria(**criteria)
                return len(records)
                
        except Exception as db_error:
            # Fallback to CSV
            db_logger.warning(f"🔄 FALLBACK TO CSV: Count operation")
            db_logger.warning(f"   Reason: {db_error}")
            db_logger.warning(f"   🚨 DATABASE OPERATION FAILED - USING CSV BACKUP")
            
            self.usage_stats['csv_fallbacks'] += 1
            
            try:
                df = self.csv_manager.read()
                
                # Apply criteria filtering
                for field, value in criteria.items():
                    if field in df.columns:
                        df = df[df[field] == value]
                
                return len(df)
                
            except Exception as csv_error:
                db_logger.error(f"❌ CSV fallback also failed for count: {csv_error}")
                return 0
    
    def _validate_dataframe_consistency(self, db_df: pd.DataFrame, csv_df: pd.DataFrame):
        """Validate that database and CSV DataFrames are consistent."""
        
        if len(db_df) != len(csv_df):
            raise ValueError(f"Record count mismatch: DB={len(db_df)}, CSV={len(csv_df)}")
        
        # Check for missing records
        db_ids = set(db_df['row_id'].tolist())
        csv_ids = set(csv_df['row_id'].tolist())
        
        missing_in_csv = db_ids - csv_ids
        missing_in_db = csv_ids - db_ids
        
        if missing_in_csv:
            raise ValueError(f"Records missing in CSV: {list(missing_in_csv)[:5]}...")
        
        if missing_in_db:
            raise ValueError(f"Records missing in DB: {list(missing_in_db)[:5]}...")
        
        db_logger.debug(f"✅ DataFrame consistency validated: {len(db_df)} records match")
    
    def get_usage_statistics(self) -> Dict[str, Any]:
        """Get usage statistics for database vs CSV operations."""
        
        uptime = (datetime.now() - self.usage_stats['start_time']).total_seconds()
        total_ops = self.usage_stats['total_operations']
        
        stats = {
            'total_operations': total_ops,
            'database_reads': self.usage_stats['database_reads'],
            'csv_fallbacks': self.usage_stats['csv_fallbacks'],
            'database_success_rate': (self.usage_stats['database_reads'] / total_ops * 100) if total_ops > 0 else 0,
            'fallback_rate': (self.usage_stats['csv_fallbacks'] / total_ops * 100) if total_ops > 0 else 0,
            'uptime_seconds': uptime
        }
        
        # Add database reader metrics
        db_metrics = self.db_reader.get_current_metrics()
        stats.update({'db_reader_metrics': db_metrics})
        
        return stats
    
    def generate_migration_report(self) -> Dict[str, Any]:
        """Generate comprehensive migration status report."""
        
        return {
            'migration_phase': 'Database-Primary Reads (Phase 2)',
            'usage_statistics': self.get_usage_statistics(),
            'fallback_report': self.db_reader.generate_fallback_report(),
            'recommendations': self._generate_recommendations()
        }
    
    def _generate_recommendations(self) -> List[str]:
        """Generate recommendations based on usage patterns."""
        
        stats = self.get_usage_statistics()
        recommendations = []
        
        if stats['fallback_rate'] > 10:
            recommendations.append(f"High fallback rate ({stats['fallback_rate']:.1f}%) - investigate database reliability")
        
        if stats['database_success_rate'] < 90:
            recommendations.append(f"Low database success rate ({stats['database_success_rate']:.1f}%) - check database health")
        
        if stats['total_operations'] < 10:
            recommendations.append("Low operation count - consider running more comprehensive tests")
        
        if not recommendations:
            recommendations.append("Database-primary reads operating within acceptable parameters")
        
        return recommendations

# Backward compatibility functions
def read_csv_safe(csv_path: str = "outputs/output.csv", dtype_spec: str = 'tracking', 
                  use_database_primary: bool = True) -> pd.DataFrame:
    """
    Enhanced CSV read with optional database-primary operation.
    
    Args:
        csv_path: Path to CSV file
        dtype_spec: Data type specification
        use_database_primary: Whether to use database-primary reads
        
    Returns:
        DataFrame from database or CSV
    """
    if use_database_primary:
        manager = DatabasePrimaryManager(csv_path)
        return manager.read_dataframe(dtype_spec)
    else:
        # Traditional CSV read
        csv_manager = CSVManager(csv_path)
        return csv_manager.read(dtype_spec)

def get_record_database_primary(row_id: int, csv_path: str = "outputs/output.csv") -> Optional[Dict[str, Any]]:
    """Get single record using database-primary operation."""
    manager = DatabasePrimaryManager(csv_path)
    return manager.get_record_by_id(row_id)

def count_records_database_primary(csv_path: str = "outputs/output.csv", **criteria) -> int:
    """Count records using database-primary operation."""
    manager = DatabasePrimaryManager(csv_path)
    return manager.count_records(**criteria)

# Test functions
if __name__ == "__main__":
    print("🗄️ Testing Database-Primary Manager")
    print("=" * 40)
    
    manager = DatabasePrimaryManager()
    
    try:
        # Test DataFrame read
        print("\n📊 Testing DataFrame read...")
        df = manager.read_dataframe()
        print(f"✅ DataFrame read successful: {len(df)} records")
        
        # Test single record read
        print("\n📖 Testing single record read...")
        record = manager.get_record_by_id(1)
        if record:
            print(f"✅ Record found: {record['name']}")
        else:
            print("🔍 Record not found")
        
        # Test count
        print("\n🔢 Testing record count...")
        count = manager.count_records()
        print(f"✅ Total records: {count}")
        
        # Test criteria search
        print("\n🔍 Testing criteria search...")
        processed_records = manager.get_records_by_criteria(processed=True)
        print(f"✅ Processed records: {len(processed_records)}")
        
        # Generate usage statistics
        print("\n📊 Usage Statistics:")
        stats = manager.get_usage_statistics()
        print(f"  Database reads: {stats['database_reads']}")
        print(f"  CSV fallbacks: {stats['csv_fallbacks']}")
        print(f"  Database success rate: {stats['database_success_rate']:.1f}%")
        print(f"  Fallback rate: {stats['fallback_rate']:.1f}%")
        
        print("\n✅ Database-primary manager working correctly!")
        
    except Exception as e:
        print(f"\n❌ Test failed: {e}")
        db_logger.error(f"Database-primary manager test failed: {e}")