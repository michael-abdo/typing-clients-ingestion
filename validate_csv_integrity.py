#!/usr/bin/env python3
"""
CSV Data Integrity Validation Script
Validates the CSV file before database migration
"""

import csv
import json
import sys
from pathlib import Path

def validate_csv_integrity(csv_path: str) -> bool:
    """
    Validate CSV data integrity before migration.
    
    Returns:
        bool: True if validation passes, False otherwise
    """
    errors = []
    warnings = []
    total_rows = 0
    valid_json_rows = 0
    
    print(f"🔍 Validating CSV integrity: {csv_path}")
    
    try:
        with open(csv_path, 'r', encoding='utf-8') as file:
            reader = csv.DictReader(file)
            
            # Check header
            expected_columns = [
                'row_id', 'name', 'email', 'type', 'link', 'extracted_links',
                'youtube_playlist', 'google_drive', 'processed', 'document_text',
                'youtube_status', 'youtube_files', 'youtube_media_id', 'drive_status',
                'drive_files', 'drive_media_id', 'last_download_attempt',
                'download_errors', 'permanent_failure', 'file_uuids', 's3_paths'
            ]
            
            missing_columns = set(expected_columns) - set(reader.fieldnames)
            if missing_columns:
                errors.append(f"Missing columns: {missing_columns}")
            
            extra_columns = set(reader.fieldnames) - set(expected_columns)
            if extra_columns:
                warnings.append(f"Extra columns: {extra_columns}")
            
            print(f"✓ Column validation - Expected: {len(expected_columns)}, Found: {len(reader.fieldnames)}")
            
            # Validate each row
            for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
                total_rows += 1
                
                # Check required fields
                if not row.get('row_id'):
                    errors.append(f"Row {row_num}: Missing row_id")
                
                if not row.get('name'):
                    errors.append(f"Row {row_num}: Missing name")
                
                if not row.get('email'):
                    errors.append(f"Row {row_num}: Missing email")
                
                # Validate JSON fields
                json_fields = ['file_uuids', 's3_paths']
                row_has_valid_json = True
                
                for field in json_fields:
                    json_data = row.get(field, '{}').strip()
                    
                    # Skip empty or default values
                    if not json_data or json_data in ['{}', '""', "''", 'null', '']:
                        continue
                    
                    try:
                        parsed = json.loads(json_data)
                        if not isinstance(parsed, dict):
                            errors.append(f"Row {row_num}: {field} is not a JSON object")
                            row_has_valid_json = False
                    except json.JSONDecodeError as e:
                        errors.append(f"Row {row_num}: Invalid JSON in {field} - {e}")
                        row_has_valid_json = False
                
                if row_has_valid_json and (row.get('file_uuids', '{}') != '{}' or row.get('s3_paths', '{}') != '{}'):
                    valid_json_rows += 1
                
                # Check data consistency between file_uuids and s3_paths
                try:
                    file_uuids_data = row.get('file_uuids', '{}').strip()
                    s3_paths_data = row.get('s3_paths', '{}').strip()
                    
                    # Only validate if both fields have actual JSON data
                    if file_uuids_data and s3_paths_data and file_uuids_data not in ['{}', ''] and s3_paths_data not in ['{}', '']:
                        file_uuids = json.loads(file_uuids_data)
                        s3_paths = json.loads(s3_paths_data)
                        
                        # Validate UUID consistency
                        file_uuid_values = set(file_uuids.values())
                        s3_path_keys = set(s3_paths.keys())
                        
                        if file_uuid_values and s3_path_keys:
                            missing_s3_paths = file_uuid_values - s3_path_keys
                            if missing_s3_paths:
                                warnings.append(f"Row {row_num}: UUIDs in file_uuids missing from s3_paths: {missing_s3_paths}")
                            
                            extra_s3_paths = s3_path_keys - file_uuid_values
                            if extra_s3_paths:
                                warnings.append(f"Row {row_num}: UUIDs in s3_paths not in file_uuids: {extra_s3_paths}")
                
                except Exception as e:
                    errors.append(f"Row {row_num}: Error validating UUID consistency - {e}")
                
                # Progress indicator
                if total_rows % 100 == 0:
                    print(f"  Processed {total_rows} rows...")
    
    except Exception as e:
        errors.append(f"Failed to read CSV file: {e}")
        return False
    
    # Report results
    print(f"\n📊 Validation Results:")
    print(f"  Total rows: {total_rows}")
    print(f"  Rows with JSON data: {valid_json_rows}")
    print(f"  Errors: {len(errors)}")
    print(f"  Warnings: {len(warnings)}")
    
    if errors:
        print(f"\n❌ ERRORS:")
        for error in errors[:10]:  # Show first 10 errors
            print(f"  - {error}")
        if len(errors) > 10:
            print(f"  ... and {len(errors) - 10} more errors")
    
    if warnings:
        print(f"\n⚠️  WARNINGS:")
        for warning in warnings[:10]:  # Show first 10 warnings
            print(f"  - {warning}")
        if len(warnings) > 10:
            print(f"  ... and {len(warnings) - 10} more warnings")
    
    if not errors:
        print(f"\n✅ CSV validation PASSED - Ready for migration")
        return True
    else:
        print(f"\n❌ CSV validation FAILED - {len(errors)} errors found")
        return False

def main():
    csv_path = "outputs/output.csv"
    
    if not Path(csv_path).exists():
        print(f"❌ CSV file not found: {csv_path}")
        sys.exit(1)
    
    success = validate_csv_integrity(csv_path)
    sys.exit(0 if success else 1)

if __name__ == "__main__":
    main()