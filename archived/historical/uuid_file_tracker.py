#!/usr/bin/env python3
"""
UUID File Tracker - Integrates UUID tracking into file upload/download operations
"""

import json
import uuid
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import pandas as pd

from .csv_manager import CSVManager
from .logging_config import get_logger

logger = get_logger(__name__)

class UUIDFileTracker:
    """Manages UUID tracking for uploaded files"""
    
    def __init__(self, csv_path: str = "outputs/output.csv"):
        self.csv_path = csv_path
        self.csv_manager = CSVManager(csv_path=csv_path)
    
    def generate_file_uuid(self) -> str:
        """Generate a new UUID for a file"""
        return str(uuid.uuid4())
    
    def update_file_tracking(self, row_id: int, file_info: Dict[str, str]) -> bool:
        """
        Update CSV with file UUID information
        
        Args:
            row_id: Client row ID
            file_info: Dict containing:
                - filename: Original filename
                - uuid: File UUID
                - s3_path: S3 path/key
                - file_type: 'youtube' or 'drive'
        
        Returns:
            True if successful, False otherwise
        """
        try:
            # Read current CSV
            df = self.csv_manager.read()
            
            # Find the row
            row_mask = df['row_id'] == row_id
            if not row_mask.any():
                logger.error(f"Row ID {row_id} not found in CSV")
                return False
            
            row_idx = df[row_mask].index[0]
            
            # Get current UUID data
            file_uuids = json.loads(df.loc[row_idx, 'file_uuids'] if 'file_uuids' in df.columns and pd.notna(df.loc[row_idx, 'file_uuids']) else '{}')
            youtube_uuids = json.loads(df.loc[row_idx, 'youtube_uuids'] if 'youtube_uuids' in df.columns and pd.notna(df.loc[row_idx, 'youtube_uuids']) else '[]')
            drive_uuids = json.loads(df.loc[row_idx, 'drive_uuids'] if 'drive_uuids' in df.columns and pd.notna(df.loc[row_idx, 'drive_uuids']) else '[]')
            s3_paths = json.loads(df.loc[row_idx, 's3_paths'] if 's3_paths' in df.columns and pd.notna(df.loc[row_idx, 's3_paths']) else '{}')
            
            # Update with new file info
            file_uuid = file_info['uuid']
            filename = file_info['filename']
            s3_path = file_info['s3_path']
            file_type = file_info.get('file_type', '').lower()
            
            # Update mappings
            file_uuids[filename] = file_uuid
            s3_paths[file_uuid] = s3_path
            
            # Add to appropriate type list
            if 'youtube' in file_type or 'youtube' in filename.lower():
                if file_uuid not in youtube_uuids:
                    youtube_uuids.append(file_uuid)
            elif 'drive' in file_type or 'drive' in filename.lower():
                if file_uuid not in drive_uuids:
                    drive_uuids.append(file_uuid)
            
            # Ensure columns exist
            if 'file_uuids' not in df.columns:
                df['file_uuids'] = '{}'
            if 'youtube_uuids' not in df.columns:
                df['youtube_uuids'] = '[]'
            if 'drive_uuids' not in df.columns:
                df['drive_uuids'] = '[]'
            if 's3_paths' not in df.columns:
                df['s3_paths'] = '{}'
            
            # Update DataFrame
            df.loc[row_idx, 'file_uuids'] = json.dumps(file_uuids)
            df.loc[row_idx, 'youtube_uuids'] = json.dumps(youtube_uuids)
            df.loc[row_idx, 'drive_uuids'] = json.dumps(drive_uuids)
            df.loc[row_idx, 's3_paths'] = json.dumps(s3_paths)
            
            # Also update file lists (for backward compatibility)
            if 'youtube' in file_type and 'youtube_files' in df.columns:
                current_files = df.loc[row_idx, 'youtube_files']
                if pd.isna(current_files) or current_files == '':
                    df.loc[row_idx, 'youtube_files'] = filename
                else:
                    files_list = current_files.split('|')
                    if filename not in files_list:
                        files_list.append(filename)
                    df.loc[row_idx, 'youtube_files'] = '|'.join(files_list)
            
            elif 'drive' in file_type and 'drive_files' in df.columns:
                current_files = df.loc[row_idx, 'drive_files']
                if pd.isna(current_files) or current_files == '':
                    df.loc[row_idx, 'drive_files'] = filename
                else:
                    files_list = current_files.split('|')
                    if filename not in files_list:
                        files_list.append(filename)
                    df.loc[row_idx, 'drive_files'] = '|'.join(files_list)
            
            # Save updated CSV
            success = self.csv_manager.safe_csv_write(df, operation_name="update_uuid_tracking")
            
            if success:
                logger.info(f"✅ Updated UUID tracking for {filename} (UUID: {file_uuid})")
            
            return success
            
        except Exception as e:
            logger.error(f"Failed to update UUID tracking: {str(e)}")
            return False
    
    def get_file_by_uuid(self, file_uuid: str) -> Optional[Dict]:
        """Look up file information by UUID"""
        try:
            df = self.csv_manager.read()
            
            for _, row in df.iterrows():
                # Check file_uuids
                if 'file_uuids' in df.columns and pd.notna(row.get('file_uuids')):
                    file_uuids = json.loads(row['file_uuids'])
                    for filename, uuid_val in file_uuids.items():
                        if uuid_val == file_uuid:
                            s3_paths = json.loads(row.get('s3_paths', '{}'))
                            return {
                                'row_id': row['row_id'],
                                'client_name': row['name'],
                                'client_type': row['type'],
                                'filename': filename,
                                's3_path': s3_paths.get(file_uuid, ''),
                                'uuid': file_uuid
                            }
            
            return None
            
        except Exception as e:
            logger.error(f"Failed to lookup UUID: {str(e)}")
            return None
    
    def get_client_files_with_uuids(self, row_id: int) -> Dict[str, List[Dict]]:
        """Get all files for a client with their UUIDs"""
        try:
            df = self.csv_manager.read()
            
            # Find the row
            row = df[df['row_id'] == row_id]
            if row.empty:
                return {'youtube': [], 'drive': []}
            
            row = row.iloc[0]
            
            # Parse UUID data
            file_uuids = json.loads(row.get('file_uuids', '{}')) if 'file_uuids' in df.columns else {}
            s3_paths = json.loads(row.get('s3_paths', '{}')) if 's3_paths' in df.columns else {}
            youtube_uuids = json.loads(row.get('youtube_uuids', '[]')) if 'youtube_uuids' in df.columns else []
            drive_uuids = json.loads(row.get('drive_uuids', '[]')) if 'drive_uuids' in df.columns else []
            
            # Build file lists
            youtube_files = []
            drive_files = []
            
            for filename, file_uuid in file_uuids.items():
                file_info = {
                    'filename': filename,
                    'uuid': file_uuid,
                    's3_path': s3_paths.get(file_uuid, '')
                }
                
                if file_uuid in youtube_uuids:
                    youtube_files.append(file_info)
                elif file_uuid in drive_uuids:
                    drive_files.append(file_info)
            
            return {
                'youtube': youtube_files,
                'drive': drive_files
            }
            
        except Exception as e:
            logger.error(f"Failed to get client files: {str(e)}")
            return {'youtube': [], 'drive': []}


# Singleton instance
_uuid_tracker = None

def get_uuid_tracker(csv_path: Optional[str] = None) -> UUIDFileTracker:
    """Get or create singleton UUID tracker instance"""
    global _uuid_tracker
    if _uuid_tracker is None or csv_path is not None:
        _uuid_tracker = UUIDFileTracker(csv_path or "outputs/output.csv")
    return _uuid_tracker