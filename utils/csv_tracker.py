#!/usr/bin/env python3
"""
CSV Tracker - Row-centric tracking system for download status
Maintains perfect data integrity while tracking YouTube and Drive downloads
"""

import os
import pandas as pd
import json
from typing import List, Optional, Dict, Any
from datetime import datetime
from dataclasses import dataclass

try:
    from file_lock import file_lock
except ImportError:
    from .file_lock import file_lock


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
    
    def get_summary(self) -> Dict[str, Any]:
        """Generate summary for CSV update"""
        return {
            'status': 'completed' if self.success else 'failed',
            'files': ','.join(self.files_downloaded),
            'media_id': self.media_id or '',
            'error': self.error_message or '',
            'last_attempt': datetime.now().isoformat()
        }


def ensure_tracking_columns(csv_path: str = 'output.csv') -> bool:
    """Add tracking columns to CSV if they don't exist"""
    print(f"Enhancing CSV schema with tracking columns: {csv_path}")
    
    # Read current CSV
    df = pd.read_csv(csv_path)
    print(f"Current CSV has {len(df)} rows and columns: {list(df.columns)}")
    
    # Define tracking columns with default values
    tracking_columns = {
        'youtube_status': 'pending',      # pending|downloading|completed|failed
        'youtube_files': '',              # Comma-separated list of downloaded files
        'youtube_media_id': '',           # YouTube video ID
        'drive_status': 'pending',        # pending|downloading|completed|failed
        'drive_files': '',                # Comma-separated list of downloaded files
        'drive_media_id': '',             # Google Drive file ID
        'last_download_attempt': '',      # ISO timestamp of last attempt
        'download_errors': ''             # Error messages from failed downloads
    }
    
    # Add missing columns
    modified = False
    for col, default_value in tracking_columns.items():
        if col not in df.columns:
            df[col] = default_value
            modified = True
            print(f"  Added column: {col}")
    
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


def get_pending_downloads(csv_path: str = 'output.csv', download_type: str = 'both') -> List[RowContext]:
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
            if (pd.notna(row.get('youtube_playlist')) and 
                str(row.get('youtube_playlist', '')).strip() not in ['', '-'] and
                row.get('youtube_status', 'pending') in ['pending', 'failed']):
                pending_rows.append(row_context)
                continue
                
        # Check Drive downloads needed  
        if download_type in ['both', 'drive']:
            if (pd.notna(row.get('google_drive')) and 
                str(row.get('google_drive', '')).strip() not in ['', '-'] and
                row.get('drive_status', 'pending') in ['pending', 'failed']):
                if row_context not in pending_rows:  # Avoid duplicates
                    pending_rows.append(row_context)
                    
    return pending_rows


def update_csv_download_status(row_index: int, download_type: str, 
                             result: DownloadResult, csv_path: str = 'output.csv'):
    """Atomically update CSV with download results while preserving all existing data"""
    
    with file_lock(f'{csv_path}.lock'):
        # Read current state
        df = pd.read_csv(csv_path)
        
        # Verify row exists and preserve critical data
        if row_index >= len(df):
            raise IndexError(f"Row index {row_index} out of range (CSV has {len(df)} rows)")
            
        # Get original type value to verify preservation
        original_type = df.loc[row_index, 'type']
        original_name = df.loc[row_index, 'name']
        
        print(f"Updating {download_type} status for row {row_index}: {original_name} (Type: {original_type})")
        
        # Update only download-specific columns
        summary = result.get_summary()
        
        if download_type == 'youtube':
            df.loc[row_index, 'youtube_status'] = summary['status']
            df.loc[row_index, 'youtube_files'] = summary['files']
            df.loc[row_index, 'youtube_media_id'] = summary['media_id']
        elif download_type == 'drive':
            df.loc[row_index, 'drive_status'] = summary['status']
            df.loc[row_index, 'drive_files'] = summary['files']  
            df.loc[row_index, 'drive_media_id'] = summary['media_id']
        else:
            raise ValueError(f"Invalid download_type: {download_type}")
            
        # Update common columns
        df.loc[row_index, 'last_download_attempt'] = summary['last_attempt']
        if summary['error']:
            current_errors = str(df.loc[row_index, 'download_errors'])
            if current_errors and current_errors != 'nan':
                df.loc[row_index, 'download_errors'] = f"{current_errors}; {summary['error']}"
            else:
                df.loc[row_index, 'download_errors'] = summary['error']
                
        # CRITICAL: Verify type column unchanged
        if str(df.loc[row_index, 'type']) != str(original_type):
            raise RuntimeError(f"Type column corruption detected for row {row_index}: {original_type} -> {df.loc[row_index, 'type']}")
            
        # Atomic write
        df.to_csv(csv_path, index=False)
        print(f"  Updated CSV: {summary['status']} - {len(summary['files'].split(',') if summary['files'] else [])} files")


def get_download_status_summary(csv_path: str = 'output.csv') -> Dict[str, Any]:
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
    parser.add_argument('--csv-path', default='output.csv',
                       help='Path to CSV file (default: output.csv)')
    
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
    else:
        pending = get_pending_downloads(args.csv_path, args.pending)
        print(f"\nPending {args.pending} downloads: {len(pending)}")
        for i, row in enumerate(pending[:10]):  # Show first 10
            print(f"  {i+1}. Row {row.row_id}: {row.name} (Type: {row.type})")
        if len(pending) > 10:
            print(f"  ... and {len(pending) - 10} more")