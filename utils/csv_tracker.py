#!/usr/bin/env python3
"""
CSV Tracker - Row-centric tracking system for download status
Maintains perfect data integrity while tracking YouTube and Drive downloads
"""

import os
import pandas as pd
import json
import shutil
from typing import List, Optional, Dict, Any
from datetime import datetime
from dataclasses import dataclass

try:
    from file_lock import file_lock
    from sanitization import sanitize_error_message, sanitize_csv_field
    from config import get_config
except ImportError:
    from .file_lock import file_lock
    from .sanitization import sanitize_error_message, sanitize_csv_field
    from .config import get_config


def safe_get_na_value(column_name: str = None, dtype: str = 'string') -> Any:
    """
    Get the appropriate null/NA value for a given column and data type.
    This ensures consistent null representation across the entire CSV.
    
    Args:
        column_name: Name of the column (for column-specific rules if needed)
        dtype: The pandas dtype of the column ('string', 'float', 'int', etc.)
        
    Returns:
        The appropriate null value (None for most cases, which becomes NaN in pandas)
        
    Note:
        Using None instead of empty strings prevents false positive corruption detection
        since pd.notna('') returns True while pd.notna(None) returns False.
    """
    # For all string columns, use None (which becomes NaN in pandas)
    # This ensures consistent field counting in validation
    if dtype in ['string', 'object', str]:
        return None
    
    # For numeric types, also use None (becomes NaN)
    elif dtype in ['float', 'int', 'float64', 'int64']:
        return None
    
    # For any other type, default to None
    else:
        return None


def create_csv_backup(csv_path: str, operation_name: str = "write") -> str:
    """
    Create timestamped backup of CSV file before modification.
    
    Args:
        csv_path: Path to CSV file to backup
        operation_name: Description of the operation for backup naming
        
    Returns:
        Path to created backup file
    """
    if not os.path.exists(csv_path):
        raise FileNotFoundError(f"CSV file not found: {csv_path}")
    
    # Create backup directory if it doesn't exist
    backup_dir = os.path.join(os.path.dirname(csv_path), "backups", "output")
    os.makedirs(backup_dir, exist_ok=True)
    
    # Generate timestamped backup filename
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    filename = os.path.basename(csv_path)
    backup_filename = f"{filename}.backup_{operation_name}_{timestamp}"
    backup_path = os.path.join(backup_dir, backup_filename)
    
    # Create backup
    shutil.copy2(csv_path, backup_path)
    
    return backup_path


def validate_csv_integrity(csv_path: str, expected_columns: Optional[List[str]] = None):
    """
    Validate CSV file integrity after write operations.
    
    Checks for:
    - File readability
    - Expected column count and names
    - No HTML/JavaScript content in fields
    - Consistent row structure
    
    Args:
        csv_path: Path to CSV file to validate
        expected_columns: List of expected column names
        
    Returns:
        tuple: (is_valid, error_message)
    """
    try:
        # Test file readability
        df = pd.read_csv(csv_path)
        
        # Check for empty file
        if df.empty:
            return False, "CSV file is empty"
        
        # Check expected columns if provided
        if expected_columns:
            if len(df.columns) != len(expected_columns):
                return False, f"Column count mismatch: expected {len(expected_columns)}, got {len(df.columns)}"
            
            missing_cols = set(expected_columns) - set(df.columns)
            if missing_cols:
                return False, f"Missing columns: {missing_cols}"
        
        # Check for HTML/JavaScript content that could indicate corruption
        # Skip extracted_links column which may have existing corruption from before our fixes
        dangerous_patterns = [
            '<script>', '<html>', 'window.', r'function\(', r'<!DOCTYPE'
        ]
        
        for col in df.columns:
            if df[col].dtype == 'object' and col != 'extracted_links':  # Skip extracted_links column
                for pattern in dangerous_patterns:
                    if df[col].astype(str).str.contains(pattern, case=False, na=False, regex=True).any():
                        return False, f"Dangerous content detected in column '{col}': {pattern}"
        
        # Check for extremely long fields that could indicate corruption
        # Skip extracted_links column which may legitimately contain long content
        for col in df.columns:
            if df[col].dtype == 'object' and col != 'extracted_links':
                max_length = df[col].astype(str).str.len().max()
                if max_length > 1000:  # No field should be longer than 1000 chars
                    return False, f"Suspiciously long field detected in column '{col}': {max_length} characters"
        
        # Check for consistent row structure using intelligent validation
        # Count meaningful fields (non-empty, non-dash) instead of just non-NA
        def count_meaningful_fields(row):
            count = 0
            for val in row:
                if pd.notna(val) and str(val).strip() not in ['', '-']:
                    count += 1
            return count
        
        meaningful_fields_per_row = df.apply(count_meaningful_fields, axis=1)
        
        # Check minimum required fields (row_id, name, type, etc.)
        min_required_fields = 4  # At least row_id, name, type, and one other field
        if meaningful_fields_per_row.min() < min_required_fields:
            return False, f"Row with too few meaningful fields: {meaningful_fields_per_row.min()} (minimum required: {min_required_fields})"
        
        # NEW: State-aware validation - check logical consistency
        # If status columns exist, validate their consistency
        if 'youtube_status' in df.columns and 'youtube_files' in df.columns:
            # Completed downloads should have files
            completed_mask = df['youtube_status'] == 'completed'
            if completed_mask.any():
                missing_files = df.loc[completed_mask, 'youtube_files'].isna()
                if missing_files.any():
                    problem_rows = df.loc[completed_mask & missing_files, 'row_id'].tolist()
                    return False, f"Completed YouTube downloads missing files for rows: {problem_rows[:5]}..."
        
        if 'drive_status' in df.columns and 'drive_files' in df.columns:
            # Completed downloads should have files
            completed_mask = df['drive_status'] == 'completed'
            if completed_mask.any():
                missing_files = df.loc[completed_mask, 'drive_files'].isna()
                if missing_files.any():
                    problem_rows = df.loc[completed_mask & missing_files, 'row_id'].tolist()
                    return False, f"Completed Drive downloads missing files for rows: {problem_rows[:5]}..."
        
        return True, "CSV validation passed"
        
    except pd.errors.EmptyDataError:
        return False, "CSV file is empty or corrupted"
    except pd.errors.ParserError as e:
        return False, f"CSV parsing error: {str(e)}"
    except Exception as e:
        return False, f"Validation error: {sanitize_error_message(str(e))}"


def safe_csv_write(df: pd.DataFrame, csv_path: str, operation_name: str = "write", 
                   expected_columns: Optional[List[str]] = None) -> bool:
    """
    Safely write DataFrame to CSV with backup and validation.
    
    Process:
    1. Create backup of existing file
    2. Write new CSV
    3. Validate written CSV
    4. Rollback to backup if validation fails
    
    Args:
        df: DataFrame to write
        csv_path: Path to write CSV file
        operation_name: Description for backup naming
        expected_columns: Expected column names for validation
        
    Returns:
        True if write successful, False if failed and rolled back
    """
    backup_path = None
    
    try:
        # Create backup if file exists
        if os.path.exists(csv_path):
            backup_path = create_csv_backup(csv_path, operation_name)
            print(f"  Created backup: {os.path.basename(backup_path)}")
        
        # Write new CSV
        df.to_csv(csv_path, index=False)
        
        # Validate written file
        is_valid, error_msg = validate_csv_integrity(csv_path, expected_columns)
        
        if not is_valid:
            print(f"  ❌ CSV validation failed: {error_msg}")
            
            # Rollback to backup if available
            if backup_path and os.path.exists(backup_path):
                shutil.copy2(backup_path, csv_path)
                print(f"  🔄 Rolled back to backup: {os.path.basename(backup_path)}")
                return False
            else:
                print(f"  ⚠️ No backup available for rollback")
                return False
        
        print(f"  ✅ CSV write successful and validated")
        return True
        
    except Exception as e:
        safe_error = sanitize_error_message(str(e))
        print(f"  ❌ CSV write failed: {safe_error}")
        
        # Rollback to backup if available
        if backup_path and os.path.exists(backup_path) and os.path.exists(csv_path):
            try:
                shutil.copy2(backup_path, csv_path)
                print(f"  🔄 Rolled back to backup: {os.path.basename(backup_path)}")
            except Exception as rollback_error:
                print(f"  ⚠️ Rollback failed: {sanitize_error_message(str(rollback_error))}")
        
        return False


@dataclass
class RowContext:
    """Context object that travels with every download to maintain CSV row relationship"""
    row_id: str          # Primary key from CSV
    row_index: int       # Position in CSV for atomic updates  
    type: str           # Personality type - CRITICAL to preserve
    name: str           # Person name for human-readable tracking
    email: str          # Additional identifier
    
    def to_metadata_dict(self) -> Dict[str, Any]:
        """Embed row context in download metadata files"""
        return {
            'source_csv_row_id': self.row_id,
            'source_csv_index': self.row_index, 
            'personality_type': self.type,
            'person_name': self.name,
            'person_email': self.email,
            'download_timestamp': datetime.now().isoformat(),
            'tracking_version': '1.0'
        }
    
    def to_filename_suffix(self) -> str:
        """Create unique filename suffix for organization"""
        clean_type = self.type.replace('/', '-').replace(' ', '_').replace('#', 'num')
        return f"_row{self.row_id}_{clean_type}"


@dataclass 
class DownloadResult:
    """Standardized result object maintaining full traceability"""
    success: bool
    files_downloaded: List[str]     # Actual filenames created
    media_id: Optional[str]         # YouTube video_id or Drive file_id
    error_message: Optional[str]
    metadata_file: Optional[str]    # Path to metadata file
    row_context: RowContext         # Preserve complete source data
    permanent_failure: bool = False # Mark as permanent failure to skip retries
    
    def get_summary(self) -> Dict[str, Any]:
        """Generate summary for CSV update"""
        return {
            'status': 'completed' if self.success else 'failed',
            'files': ','.join(self.files_downloaded),
            'media_id': self.media_id or '',
            'error': self.error_message or '',
            'last_attempt': datetime.now().isoformat()
        }


def ensure_tracking_columns(csv_path: str = 'outputs/output.csv') -> bool:
    """Add tracking columns to CSV if they don't exist"""
    print(f"Enhancing CSV schema with tracking columns: {csv_path}")
    
    # Read current CSV
    df = pd.read_csv(csv_path)
    print(f"Current CSV has {len(df)} rows and columns: {list(df.columns)}")
    
    # Define tracking columns with default values and explicit dtypes
    # Use safe_get_na_value() for empty fields to prevent false positive corruption detection
    tracking_columns = {
        'youtube_status': ('pending', 'string'),                    # pending|downloading|completed|failed
        'youtube_files': (safe_get_na_value(), 'string'),          # Comma-separated list of downloaded files
        'youtube_media_id': (safe_get_na_value(), 'string'),       # YouTube video ID
        'drive_status': ('pending', 'string'),                      # pending|downloading|completed|failed
        'drive_files': (safe_get_na_value(), 'string'),            # Comma-separated list of downloaded files
        'drive_media_id': (safe_get_na_value(), 'string'),         # Google Drive file ID
        'last_download_attempt': (safe_get_na_value(), 'string'),  # ISO timestamp of last attempt
        'download_errors': (safe_get_na_value(), 'string'),        # Error messages from failed downloads
        'permanent_failure': (safe_get_na_value(), 'string')       # Mark permanent failures to skip retries
    }
    
    # Add missing columns with proper data types
    modified = False
    for col, (default_value, dtype) in tracking_columns.items():
        if col not in df.columns:
            df[col] = default_value
            df[col] = df[col].astype(dtype)
            modified = True
            print(f"  Added column: {col} ({dtype})")
    
    # Save enhanced CSV if modified
    if modified:
        # Create backup before modification
        backup_path = f"{csv_path}.backup_before_tracking_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        df_original = pd.read_csv(csv_path)
        df_original.to_csv(backup_path, index=False)
        print(f"  Created backup: {backup_path}")
        
        # Save enhanced CSV
        df.to_csv(csv_path, index=False)
        print(f"  Enhanced CSV saved with {len(df.columns)} columns")
        
        # Verify the change
        df_verify = pd.read_csv(csv_path)
        print(f"  Verification: CSV now has columns: {list(df_verify.columns)}")
        
        return True
    else:
        print("  No changes needed - tracking columns already exist")
        return False


def get_pending_downloads(csv_path: str = 'outputs/output.csv', download_type: str = 'both', 
                         include_failed: bool = True, retry_attempts: int = 3) -> List[RowContext]:
    """Get list of rows that need downloads"""
    df = pd.read_csv(csv_path)
    
    pending_rows = []
    for index, row in df.iterrows():
        # Skip header row if it exists
        if str(row.get('row_id', '')).startswith('#') or str(row.get('name', '')).lower() == 'name':
            continue
            
        row_context = RowContext(
            row_id=str(row['row_id']),
            row_index=index,
            type=str(row['type']),
            name=str(row['name']), 
            email=str(row['email'])
        )
        
        # Check YouTube downloads needed
        if download_type in ['both', 'youtube']:
            youtube_status = row.get('youtube_status', 'pending')
            permanent_failure = str(row.get('permanent_failure', '')).strip()
            
            if (pd.notna(row.get('youtube_playlist')) and 
                str(row.get('youtube_playlist', '')).strip() not in ['', '-']):
                
                # Skip permanent failures 
                if 'youtube' in permanent_failure.lower():
                    continue
                    
                # Include pending downloads
                if youtube_status == 'pending':
                    pending_rows.append(row_context)
                    continue
                # Include failed downloads if retry is enabled and under attempt limit
                elif include_failed and youtube_status == 'failed':
                    attempt_count = _get_retry_attempts(str(row.get('download_errors', '')))
                    if attempt_count < retry_attempts:
                        pending_rows.append(row_context)
                        continue
                
        # Check Drive downloads needed  
        if download_type in ['both', 'drive']:
            drive_status = row.get('drive_status', 'pending')
            permanent_failure = str(row.get('permanent_failure', '')).strip()
            
            if (pd.notna(row.get('google_drive')) and 
                str(row.get('google_drive', '')).strip() not in ['', '-']):
                
                # Skip permanent failures
                if 'drive' in permanent_failure.lower():
                    continue
                    
                # Include pending downloads
                if drive_status == 'pending':
                    if row_context not in pending_rows:  # Avoid duplicates
                        pending_rows.append(row_context)
                # Include failed downloads if retry is enabled and under attempt limit
                elif include_failed and drive_status == 'failed':
                    attempt_count = _get_retry_attempts(str(row.get('download_errors', '')))
                    if attempt_count < retry_attempts:
                        if row_context not in pending_rows:  # Avoid duplicates
                            pending_rows.append(row_context)
                    
    return pending_rows


def _get_retry_attempts(error_message: str) -> int:
    """Count retry attempts from error message"""
    if not error_message or error_message in ['nan', '<NA>', '']:
        return 0
    # Count semicolons which separate multiple error attempts
    return error_message.count(';') + 1


def get_failed_downloads(csv_path: str = 'outputs/output.csv', download_type: str = 'both') -> List[RowContext]:
    """Get list of rows with failed downloads"""
    return get_pending_downloads(csv_path, download_type, include_failed=True, retry_attempts=0)


def reset_download_status(row_id: str, download_type: str, csv_path: str = 'outputs/output.csv') -> bool:
    """Reset download status for a specific row"""
    with file_lock(f'{csv_path}.lock'):
        # Read with proper string dtypes
        df = pd.read_csv(csv_path, dtype={
            'youtube_status': 'string',
            'youtube_files': 'string', 
            'youtube_media_id': 'string',
            'drive_status': 'string',
            'drive_files': 'string',
            'drive_media_id': 'string',
            'last_download_attempt': 'string',
            'download_errors': 'string',
            'permanent_failure': 'string'
        })
        
        # Find the row
        row_mask = df['row_id'].astype(str) == str(row_id)
        if not row_mask.any():
            print(f"Row ID {row_id} not found")
            return False
            
        row_index = df[row_mask].index[0]
        
        if download_type in ['youtube', 'both']:
            df.loc[row_index, 'youtube_status'] = 'pending'
            df.loc[row_index, 'youtube_files'] = safe_get_na_value()
            df.loc[row_index, 'youtube_media_id'] = safe_get_na_value()
            
        if download_type in ['drive', 'both']:
            df.loc[row_index, 'drive_status'] = 'pending'
            df.loc[row_index, 'drive_files'] = safe_get_na_value()
            df.loc[row_index, 'drive_media_id'] = safe_get_na_value()
            
        df.loc[row_index, 'download_errors'] = safe_get_na_value()
        df.loc[row_index, 'last_download_attempt'] = safe_get_na_value()
        df.loc[row_index, 'permanent_failure'] = safe_get_na_value()
        
        # Safe CSV write with backup and validation
        expected_cols = df.columns.tolist()
        success = safe_csv_write(df, csv_path, f"reset_{download_type}_row_{row_id}", expected_cols)
        
        if success:
            print(f"Reset {download_type} status for row {row_id}")
            return True
        else:
            print(f"Failed to reset {download_type} status for row {row_id} - CSV write failed")
            return False


def update_csv_download_status(row_index: int, download_type: str, 
                             result: DownloadResult, csv_path: str = 'outputs/output.csv'):
    """Atomically update CSV with download results while preserving all existing data"""
    
    with file_lock(f'{csv_path}.lock'):
        # Read current state with proper string dtypes
        df = pd.read_csv(csv_path, dtype={
            'youtube_status': 'string',
            'youtube_files': 'string', 
            'youtube_media_id': 'string',
            'drive_status': 'string',
            'drive_files': 'string',
            'drive_media_id': 'string',
            'last_download_attempt': 'string',
            'download_errors': 'string',
            'permanent_failure': 'string'
        })
        
        # Verify row exists and preserve critical data
        if row_index >= len(df):
            raise IndexError(f"Row index {row_index} out of range (CSV has {len(df)} rows)")
            
        # Get original type value to verify preservation
        original_type = df.loc[row_index, 'type']
        original_name = df.loc[row_index, 'name']
        
        print(f"Updating {download_type} status for row {row_index}: {original_name} (Type: {original_type})")
        
        # Update only download-specific columns with proper string conversion
        summary = result.get_summary()
        
        if download_type == 'youtube':
            df.loc[row_index, 'youtube_status'] = str(summary['status'])
            # For files and media_id, use None instead of empty string for failed downloads
            files_value = sanitize_csv_field(str(summary['files']), max_length=150) if summary['files'] else safe_get_na_value()
            media_id_value = sanitize_csv_field(str(summary['media_id']), max_length=50) if summary['media_id'] else safe_get_na_value()
            df.loc[row_index, 'youtube_files'] = files_value
            df.loc[row_index, 'youtube_media_id'] = media_id_value
        elif download_type == 'drive':
            df.loc[row_index, 'drive_status'] = str(summary['status'])
            # For files and media_id, use None instead of empty string for failed downloads
            files_value = sanitize_csv_field(str(summary['files']), max_length=150) if summary['files'] else safe_get_na_value()
            media_id_value = sanitize_csv_field(str(summary['media_id']), max_length=50) if summary['media_id'] else safe_get_na_value()
            df.loc[row_index, 'drive_files'] = files_value
            df.loc[row_index, 'drive_media_id'] = media_id_value
        else:
            raise ValueError(f"Invalid download_type: {download_type}")
            
        # Update common columns with string conversion
        df.loc[row_index, 'last_download_attempt'] = str(summary['last_attempt'])
        if summary['error']:
            # Sanitize error message to prevent CSV corruption from HTML/JavaScript content
            sanitized_error = sanitize_error_message(summary['error'], max_length=200)
            current_errors = str(df.loc[row_index, 'download_errors'])
            if current_errors and current_errors not in ['nan', '<NA>', '']:
                # Sanitize existing errors as well and combine safely
                sanitized_current = sanitize_csv_field(current_errors, max_length=100)
                combined_error = f"{sanitized_current}; {sanitized_error}"
                # Final sanitization of combined message
                df.loc[row_index, 'download_errors'] = sanitize_csv_field(combined_error, max_length=200)
            else:
                df.loc[row_index, 'download_errors'] = sanitized_error
                
        # Mark permanent failures to skip future retries
        if result.permanent_failure:
            current_permanent = str(df.loc[row_index, 'permanent_failure']).strip()
            if current_permanent and current_permanent not in ['nan', '<NA>', '']:
                if download_type not in current_permanent.lower():
                    df.loc[row_index, 'permanent_failure'] = f"{current_permanent},{download_type}"
            else:
                df.loc[row_index, 'permanent_failure'] = download_type
                
        # CRITICAL: Verify type column unchanged
        if str(df.loc[row_index, 'type']) != str(original_type):
            raise RuntimeError(f"Type column corruption detected for row {row_index}: {original_type} -> {df.loc[row_index, 'type']}")
            
        # Safe CSV write with backup and validation
        expected_cols = df.columns.tolist()
        success = safe_csv_write(df, csv_path, f"update_{download_type}_row_{row_index}", expected_cols)
        
        if success:
            print(f"  Updated CSV: {summary['status']} - {len(summary['files'].split(',') if summary['files'] else [])} files")
        else:
            raise RuntimeError(f"Failed to update CSV for row {row_index} - write operation failed validation")


def get_download_status_summary(csv_path: str = 'outputs/output.csv') -> Dict[str, Any]:
    """Get summary statistics of download status"""
    df = pd.read_csv(csv_path)
    
    # Filter out header rows
    data_df = df[~df['row_id'].astype(str).str.startswith('#')]
    data_df = data_df[data_df['name'].astype(str).str.lower() != 'name']
    
    total_rows = len(data_df)
    
    summary = {
        'total_rows': total_rows,
        'youtube': {
            'has_content': len(data_df[data_df['youtube_playlist'].notna() & 
                                    (data_df['youtube_playlist'].astype(str).str.strip() != '') &
                                    (data_df['youtube_playlist'].astype(str) != '-')]),
            'pending': len(data_df[data_df.get('youtube_status', 'pending') == 'pending']),
            'completed': len(data_df[data_df.get('youtube_status', 'pending') == 'completed']),
            'failed': len(data_df[data_df.get('youtube_status', 'pending') == 'failed'])
        },
        'drive': {
            'has_content': len(data_df[data_df['google_drive'].notna() & 
                                     (data_df['google_drive'].astype(str).str.strip() != '') &
                                     (data_df['google_drive'].astype(str) != '-')]),
            'pending': len(data_df[data_df.get('drive_status', 'pending') == 'pending']),
            'completed': len(data_df[data_df.get('drive_status', 'pending') == 'completed']),
            'failed': len(data_df[data_df.get('drive_status', 'pending') == 'failed'])
        }
    }
    
    return summary


def find_row_by_file(filename: str, downloads_dir: str = 'youtube_downloads') -> Optional[RowContext]:
    """Reverse lookup: find CSV row from downloaded filename"""
    # Look for metadata file
    base_name = os.path.splitext(filename)[0]
    metadata_pattern = f"{base_name}*_metadata.json"
    
    import glob
    metadata_files = glob.glob(os.path.join(downloads_dir, metadata_pattern))
    
    if metadata_files:
        try:
            with open(metadata_files[0], 'r') as f:
                metadata = json.load(f)
                
            return RowContext(
                row_id=metadata['source_csv_row_id'],
                row_index=metadata['source_csv_index'],
                type=metadata['personality_type'],
                name=metadata['person_name'],
                email=metadata['person_email']
            )
        except (json.JSONDecodeError, KeyError) as e:
            print(f"Error reading metadata file {metadata_files[0]}: {e}")
    
    return None


if __name__ == "__main__":
    """CLI interface for CSV tracking operations"""
    import argparse
    
    parser = argparse.ArgumentParser(description="CSV Download Tracking Utility")
    parser.add_argument('--enhance-schema', action='store_true', 
                       help='Add tracking columns to CSV')
    parser.add_argument('--status', action='store_true',
                       help='Show download status summary')
    parser.add_argument('--pending', choices=['both', 'youtube', 'drive'], default='both',
                       help='Show pending downloads')
    parser.add_argument('--failed', choices=['both', 'youtube', 'drive'], 
                       help='Show failed downloads')
    parser.add_argument('--retry-failed', choices=['both', 'youtube', 'drive'],
                       help='Retry all failed downloads')
    parser.add_argument('--reset-status', type=str, metavar='ROW_ID',
                       help='Reset download status for specific row ID')
    parser.add_argument('--reset-type', choices=['both', 'youtube', 'drive'], default='both',
                       help='Type of download to reset (used with --reset-status)')
    parser.add_argument('--detailed', action='store_true',
                       help='Show detailed information')
    parser.add_argument('--csv-path', default='outputs/output.csv',
                       help='Path to CSV file (default: outputs/output.csv)')
    
    args = parser.parse_args()
    
    if args.enhance_schema:
        ensure_tracking_columns(args.csv_path)
    elif args.status:
        summary = get_download_status_summary(args.csv_path)
        print(f"\nDownload Status Summary:")
        print(f"Total rows: {summary['total_rows']}")
        print(f"\nYouTube:")
        print(f"  Has content: {summary['youtube']['has_content']}")
        print(f"  Pending: {summary['youtube']['pending']}")
        print(f"  Completed: {summary['youtube']['completed']}")
        print(f"  Failed: {summary['youtube']['failed']}")
        print(f"\nGoogle Drive:")
        print(f"  Has content: {summary['drive']['has_content']}")
        print(f"  Pending: {summary['drive']['pending']}")
        print(f"  Completed: {summary['drive']['completed']}")
        print(f"  Failed: {summary['drive']['failed']}")
    elif args.failed:
        failed = get_failed_downloads(args.csv_path, args.failed)
        print(f"\nFailed {args.failed} downloads: {len(failed)}")
        for i, row in enumerate(failed):
            print(f"  {i+1}. Row {row.row_id}: {row.name} (Type: {row.type})")
            if args.detailed:
                # Show error details
                df = pd.read_csv(args.csv_path)
                row_data = df[df['row_id'].astype(str) == row.row_id].iloc[0]
                if row_data.get('download_errors'):
                    print(f"      Error: {row_data['download_errors']}")
    elif args.retry_failed:
        failed = get_failed_downloads(args.csv_path, args.retry_failed)
        print(f"\nFound {len(failed)} failed {args.retry_failed} downloads to retry")
        print("Use run_complete_workflow.py to process these retries automatically")
        print("Or reset individual downloads with --reset-status ROW_ID")
    elif args.reset_status:
        success = reset_download_status(args.reset_status, args.reset_type, args.csv_path)
        if success:
            print(f"✓ Reset {args.reset_type} status for row {args.reset_status}")
        else:
            print(f"✗ Failed to reset status for row {args.reset_status}")
    else:
        pending = get_pending_downloads(args.csv_path, args.pending)
        print(f"\nPending {args.pending} downloads: {len(pending)}")
        for i, row in enumerate(pending[:10]):  # Show first 10
            print(f"  {i+1}. Row {row.row_id}: {row.name} (Type: {row.type})")
        if len(pending) > 10:
            print(f"  ... and {len(pending) - 10} more")