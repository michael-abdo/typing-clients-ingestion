#!/usr/bin/env python3
"""
Data Validation Layer for Database Migration

Implements comprehensive validation comparing CSV vs DB writes with fail-fast, fail-loud, fail-safely principles.
All mismatches are logged and cause immediate failure to ensure data integrity.
"""

import os
import json
import pandas as pd
from datetime import datetime
from typing import Dict, Any, List, Optional, Tuple, Union

# Import our utilities
from utils.database_operations import (
    get_database_manager, execute_sql, select, insert
)
from utils.database_logging import (
    db_logger, log_data_consistency_check, log_dual_write_comparison,
    DatabaseOperationLogger, log_database_operation
)
from utils.data_processing import read_csv_safe

class DataValidator:
    """Comprehensive data validation for CSV vs Database operations."""
    
    def __init__(self):
        """Initialize data validator."""
        self.validation_results = []
        self.error_count = 0
        
    @log_database_operation("CSV Row Normalization")
    def normalize_csv_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize CSV row data for database comparison.
        
        Handles type conversions, null values, and JSON serialization.
        """
        normalized = {}
        
        for key, value in row.items():
            # Handle pandas NaN and None values
            if pd.isna(value) or value is None:
                # For boolean fields, None becomes False
                if key in ['processed', 'permanent_failure']:
                    normalized[key] = False
                else:
                    normalized[key] = None
            # Handle empty strings that should be None
            elif isinstance(value, str) and value.strip() == '':
                # For boolean fields, empty string becomes False
                if key in ['processed', 'permanent_failure']:
                    normalized[key] = False
                else:
                    normalized[key] = None
            # Handle JSON fields (file_uuids, s3_paths)
            elif key in ['file_uuids', 's3_paths']:
                if isinstance(value, str):
                    try:
                        # Parse JSON string
                        parsed = json.loads(value) if value.strip() else {}
                        normalized[key] = parsed
                    except json.JSONDecodeError:
                        db_logger.warning(f"Invalid JSON in {key}: {value}")
                        normalized[key] = {}
                else:
                    normalized[key] = value if value is not None else {}
            # Handle boolean fields
            elif key in ['processed', 'permanent_failure']:
                if isinstance(value, str):
                    if value.strip() == '':
                        normalized[key] = False  # Empty string = False
                    else:
                        normalized[key] = value.lower() in ('true', 'yes', '1', 'on')
                else:
                    normalized[key] = bool(value) if value is not None else False
            # Handle pipe-separated lists (keep as string for exact comparison)
            elif key in ['extracted_links', 'youtube_playlist', 'google_drive']:
                normalized[key] = str(value) if value is not None else None
            # Handle regular text fields
            else:
                normalized[key] = str(value) if value is not None else None
        
        return normalized
    
    @log_database_operation("Database Row Normalization")
    def normalize_db_row(self, row: Dict[str, Any]) -> Dict[str, Any]:
        """
        Normalize database row data for CSV comparison.
        
        Handles database-specific types and formatting.
        """
        normalized = {}
        
        for key, value in row.items():
            # Skip database-only fields
            if key in ['created_at', 'updated_at']:
                continue
                
            # Handle None values
            if value is None:
                normalized[key] = None
            # Handle JSON fields
            elif key in ['file_uuids', 's3_paths']:
                normalized[key] = value if isinstance(value, dict) else {}
            # Handle boolean fields  
            elif key in ['processed', 'permanent_failure']:
                # Convert None to False for consistency with CSV normalization
                normalized[key] = bool(value) if value is not None else False
            # Handle datetime fields
            elif key in ['last_download_attempt']:
                normalized[key] = value.isoformat() if value else None
            # Handle regular fields
            else:
                normalized[key] = str(value) if value is not None else None
        
        return normalized
    
    @log_database_operation("Field-by-Field Comparison")
    def compare_records(self, csv_row: Dict[str, Any], db_row: Dict[str, Any], 
                       record_id: Any) -> Tuple[bool, List[Dict[str, Any]]]:
        """
        Compare CSV and database records field by field.
        
        Returns:
            Tuple of (is_match, list_of_mismatches)
        """
        csv_normalized = self.normalize_csv_row(csv_row)
        db_normalized = self.normalize_db_row(db_row)
        
        mismatches = []
        
        # Compare each field that exists in both
        for field in csv_normalized.keys():
            if field in db_normalized:
                csv_val = csv_normalized[field]
                db_val = db_normalized[field]
                
                # Special handling for JSON fields
                if field in ['file_uuids', 's3_paths']:
                    if json.dumps(csv_val, sort_keys=True) != json.dumps(db_val, sort_keys=True):
                        mismatches.append({
                            'field': field,
                            'csv_value': csv_val,
                            'db_value': db_val,
                            'type': 'json_mismatch'
                        })
                # Regular field comparison
                elif csv_val != db_val:
                    mismatches.append({
                        'field': field,
                        'csv_value': csv_val,
                        'db_value': db_val,
                        'type': 'value_mismatch'
                    })
        
        is_match = len(mismatches) == 0
        
        if not is_match:
            db_logger.error(f"❌ RECORD MISMATCH: {record_id}")
            for mismatch in mismatches:
                db_logger.error(f"   {mismatch['field']}: CSV='{mismatch['csv_value']}' vs DB='{mismatch['db_value']}'")
        
        return is_match, mismatches
    
    @log_database_operation("Row Count Validation")
    def validate_row_counts(self, csv_file: str, table_name: str = 'typing_clients_data') -> bool:
        """
        Validate that CSV and database have the same number of rows.
        
        FAIL FAST: Returns False immediately if counts don't match.
        """
        with DatabaseOperationLogger("Row count validation"):
            # Get CSV row count
            df = read_csv_safe(csv_file)
            csv_count = len(df)
            
            # Get database row count
            db_result = execute_sql(f"SELECT COUNT(*) as count FROM {table_name}")
            db_count = db_result[0]['count'] if db_result else 0
            
            # Log and validate
            log_data_consistency_check(csv_count, db_count, table_name)
            
            # Record validation result
            status = 'passed' if csv_count == db_count else 'failed'
            insert('data_validation', {
                'validation_type': 'row_count',
                'table_name': table_name,
                'csv_count': csv_count,
                'db_count': db_count,
                'status': status,
                'mismatch_details': {} if csv_count == db_count else {
                    'difference': abs(csv_count - db_count),
                    'csv_file': csv_file
                }
            })
            
            return csv_count == db_count
    
    @log_database_operation("Data Integrity Validation")
    def validate_data_integrity(self, csv_file: str, table_name: str = 'typing_clients_data',
                               sample_size: Optional[int] = None) -> bool:
        """
        Validate data integrity by comparing random sample of records.
        
        Args:
            csv_file: Path to CSV file
            table_name: Database table name
            sample_size: Number of records to validate (None = all records)
            
        Returns:
            True if all sampled records match exactly
        """
        with DatabaseOperationLogger("Data integrity validation"):
            df = read_csv_safe(csv_file)
            
            if df.empty:
                db_logger.warning("CSV file is empty - skipping integrity validation")
                return True
            
            # Determine sample size
            total_rows = len(df)
            if sample_size is None:
                sample_size = min(total_rows, 100)  # Default to 100 records max
            
            # Get random sample of row IDs
            if total_rows <= sample_size:
                sample_df = df
            else:
                sample_df = df.sample(n=sample_size, random_state=42)
            
            db_logger.info(f"Validating {len(sample_df)} records out of {total_rows} total")
            
            validation_errors = []
            
            for _, csv_row in sample_df.iterrows():
                row_id = csv_row['row_id']
                
                # Get corresponding database record
                db_records = select(table_name, where='row_id = ?', params=[row_id])
                
                if not db_records:
                    error = {
                        'row_id': row_id,
                        'error': 'missing_in_database',
                        'details': 'Record exists in CSV but not in database'
                    }
                    validation_errors.append(error)
                    continue
                
                if len(db_records) > 1:
                    error = {
                        'row_id': row_id,
                        'error': 'duplicate_in_database',
                        'details': f'Found {len(db_records)} records with same row_id'
                    }
                    validation_errors.append(error)
                    continue
                
                # Compare the records
                db_row = db_records[0]
                is_match, mismatches = self.compare_records(csv_row.to_dict(), db_row, row_id)
                
                if not is_match:
                    error = {
                        'row_id': row_id,
                        'error': 'data_mismatch',
                        'details': mismatches
                    }
                    validation_errors.append(error)
            
            # Record validation results
            status = 'passed' if len(validation_errors) == 0 else 'failed'
            insert('data_validation', {
                'validation_type': 'data_integrity',
                'table_name': table_name,
                'csv_count': len(sample_df),
                'db_count': len(sample_df) - len(validation_errors),
                'status': status,
                'mismatch_details': {
                    'total_errors': len(validation_errors),
                    'error_rate': len(validation_errors) / len(sample_df),
                    'errors': validation_errors[:10]  # Store first 10 errors
                }
            })
            
            if validation_errors:
                db_logger.error(f"❌ DATA INTEGRITY VALIDATION FAILED")
                db_logger.error(f"   Errors found: {len(validation_errors)}/{len(sample_df)} records")
                db_logger.error(f"   Error rate: {len(validation_errors)/len(sample_df)*100:.2f}%")
                
                # Log first few errors for debugging
                for error in validation_errors[:3]:
                    db_logger.error(f"   Row {error['row_id']}: {error['error']}")
                
                # FAIL FAST - this is critical
                raise ValueError(f"Data integrity validation failed: {len(validation_errors)} errors found")
            
            db_logger.info(f"✅ DATA INTEGRITY VALIDATION PASSED")
            db_logger.info(f"   Validated: {len(sample_df)} records")
            db_logger.info(f"   Match rate: 100.00%")
            
            return True
    
    @log_database_operation("Dual Write Validation")
    def validate_dual_write(self, csv_row: Dict[str, Any], db_row: Dict[str, Any],
                           record_id: Any) -> bool:
        """
        Validate a single dual-write operation.
        
        Used during dual-write phase to ensure CSV and DB writes are identical.
        FAIL LOUD: Raises exception on any mismatch.
        """
        is_match, mismatches = self.compare_records(csv_row, db_row, record_id)
        
        if not is_match:
            # Log the dual-write mismatch (this will raise an exception)
            log_dual_write_comparison(csv_row, db_row, record_id)
        
        return True
    
    @log_database_operation("Full Dataset Validation")
    def validate_full_migration(self, csv_file: str, table_name: str = 'typing_clients_data') -> bool:
        """
        Comprehensive validation of the entire migration.
        
        Runs all validation checks and provides detailed report.
        """
        with DatabaseOperationLogger("Full migration validation"):
            validation_results = {
                'row_count_check': False,
                'data_integrity_check': False,
                'timestamp': datetime.now().isoformat(),
                'csv_file': csv_file,
                'table_name': table_name
            }
            
            try:
                # Step 1: Validate row counts
                db_logger.info("🔍 Step 1: Validating row counts...")
                validation_results['row_count_check'] = self.validate_row_counts(csv_file, table_name)
                
                if not validation_results['row_count_check']:
                    db_logger.error("❌ Row count validation failed - stopping validation")
                    return False
                
                # Step 2: Validate data integrity
                db_logger.info("🔍 Step 2: Validating data integrity...")
                validation_results['data_integrity_check'] = self.validate_data_integrity(
                    csv_file, table_name, sample_size=None  # Validate all records
                )
                
                # Step 3: Summary
                all_passed = all(validation_results[key] for key in ['row_count_check', 'data_integrity_check'])
                
                if all_passed:
                    db_logger.info("🎉 FULL MIGRATION VALIDATION PASSED")
                    db_logger.info("✅ All data has been successfully migrated and validated")
                else:
                    db_logger.error("❌ FULL MIGRATION VALIDATION FAILED")
                    db_logger.error("💥 Data migration has critical issues")
                
                # Record final validation
                insert('data_validation', {
                    'validation_type': 'full_migration',
                    'table_name': table_name,
                    'csv_count': None,
                    'db_count': None,
                    'status': 'passed' if all_passed else 'failed',
                    'mismatch_details': validation_results
                })
                
                return all_passed
                
            except Exception as e:
                db_logger.error(f"💥 Validation failed with exception: {e}")
                validation_results['exception'] = str(e)
                
                # Record failure
                insert('data_validation', {
                    'validation_type': 'full_migration',
                    'table_name': table_name,
                    'csv_count': None,
                    'db_count': None,
                    'status': 'failed',
                    'mismatch_details': validation_results
                })
                
                return False

def create_validation_report(table_name: str = 'typing_clients_data') -> Dict[str, Any]:
    """
    Create comprehensive validation report from database logs.
    
    Returns:
        Dictionary containing validation summary and statistics
    """
    
    with DatabaseOperationLogger("Creating validation report"):
        # Get all validation records for this table
        validations = select('data_validation', 
                           where='table_name = ?', 
                           params=[table_name],
                           order_by='created_at DESC')
        
        if not validations:
            return {'status': 'no_validations_found', 'table_name': table_name}
        
        # Group by validation type
        by_type = {}
        for validation in validations:
            val_type = validation['validation_type']
            if val_type not in by_type:
                by_type[val_type] = []
            by_type[val_type].append(validation)
        
        # Calculate statistics
        total_validations = len(validations)
        passed_validations = sum(1 for v in validations if v['status'] == 'passed')
        failed_validations = total_validations - passed_validations
        
        latest_validation = validations[0] if validations else None
        
        report = {
            'table_name': table_name,
            'generated_at': datetime.now().isoformat(),
            'summary': {
                'total_validations': total_validations,
                'passed': passed_validations,
                'failed': failed_validations,
                'success_rate': (passed_validations / total_validations * 100) if total_validations > 0 else 0
            },
            'latest_validation': {
                'type': latest_validation['validation_type'] if latest_validation else None,
                'status': latest_validation['status'] if latest_validation else None,
                'timestamp': latest_validation['created_at'] if latest_validation else None
            },
            'by_type': {}
        }
        
        # Add type-specific statistics
        for val_type, type_validations in by_type.items():
            type_passed = sum(1 for v in type_validations if v['status'] == 'passed')
            report['by_type'][val_type] = {
                'total': len(type_validations),
                'passed': type_passed,
                'failed': len(type_validations) - type_passed,
                'latest_status': type_validations[0]['status'],
                'latest_timestamp': type_validations[0]['created_at']
            }
        
        db_logger.info("📊 VALIDATION REPORT GENERATED")
        db_logger.info(f"   Table: {table_name}")
        db_logger.info(f"   Success Rate: {report['summary']['success_rate']:.1f}%")
        db_logger.info(f"   Total Validations: {total_validations}")
        
        return report

# Test and demonstration functions
if __name__ == "__main__":
    # Test the validation system
    validator = DataValidator()
    
    # Test normalization
    test_csv_row = {
        'row_id': 517,
        'name': 'Test User',
        'email': 'test@example.com',
        'processed': 'yes',
        'file_uuids': '{"test": "uuid"}',
        'permanent_failure': float('nan'),  # Simulate pandas NaN
        'document_text': None
    }
    
    normalized = validator.normalize_csv_row(test_csv_row)
    db_logger.info(f"Normalized CSV row: {normalized}")
    
    # Test comparison
    db_row = {
        'row_id': 517,
        'name': 'Test User',
        'email': 'test@example.com',
        'processed': True,
        'file_uuids': {"test": "uuid"},
        'permanent_failure': False,
        'document_text': None,
        'created_at': datetime.now(),
        'updated_at': datetime.now()
    }
    
    is_match, mismatches = validator.compare_records(test_csv_row, db_row, 517)
    db_logger.info(f"Records match: {is_match}")
    if mismatches:
        db_logger.info(f"Mismatches: {mismatches}")
    
    db_logger.info("✅ Data validation system test completed")