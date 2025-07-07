#!/usr/bin/env python3
"""
Row Context - Core data structures for row-centric download tracking
Maintains perfect traceability between downloaded files and CSV rows

DRY: Enhanced with consolidated download result patterns (Phase 3G)
"""

import os
import json
from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any, Tuple
from datetime import datetime
from enum import Enum
from utils.path_setup import ensure_directory_exists
from pathlib import Path
import threading


# DRY: Consolidated download status enum
class DownloadStatus(Enum):
    """Standardized download status values across all download types"""
    PENDING = 'pending'
    IN_PROGRESS = 'in_progress'
    COMPLETED = 'completed'
    FAILED = 'failed'
    SKIPPED = 'skipped'
    RETRYING = 'retrying'


# DRY: Consolidated error categories
class ErrorCategory(Enum):
    """Categorize errors for better retry logic and reporting"""
    TEMPORARY = 'temporary'      # Network issues, timeouts - should retry
    PERMANENT = 'permanent'      # Video deleted, access denied - don't retry
    NETWORK = 'network'          # Connection errors - retry with backoff
    PERMISSION = 'permission'    # Authentication/authorization - may need user action
    NOT_FOUND = 'not_found'      # Resource doesn't exist - permanent
    RATE_LIMIT = 'rate_limit'    # API rate limiting - retry after delay
    UNKNOWN = 'unknown'          # Uncategorized errors


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
        # DRY: Use consolidated filename cleaning from utils/config.py
        from .import_utils import setup_project_path, safe_import
        setup_project_path()
        clean_filename = safe_import('utils.config', ['clean_filename'])
        
        clean_type = clean_filename(self.type, replacement='_')
        return f"_row{self.row_id}_{clean_type}"
    
    def to_safe_name_prefix(self) -> str:
        """Create safe filename prefix from person name"""
        # DRY: Use consolidated identifier creation from utils/config.py  
        from .import_utils import setup_project_path, safe_import
        setup_project_path()
        create_safe_identifier = safe_import('utils.config', ['create_safe_identifier'])
        
        return create_safe_identifier(self.name, max_length=20)


@dataclass 
class DownloadResult:
    """Standardized result object maintaining full traceability (DRY: enhanced)"""
    success: bool
    files_downloaded: List[str]     # Actual filenames created
    media_id: Optional[str]         # YouTube video_id or Drive file_id
    error_message: Optional[str]
    metadata_file: Optional[str]    # Path to metadata file
    row_context: RowContext         # Preserve complete source data
    download_type: str              # 'youtube' or 'drive'
    permanent_failure: bool = False # Mark as permanent failure to skip retries
    
    # Enhanced fields (DRY: consolidated from various download modules)
    status: DownloadStatus = DownloadStatus.PENDING
    error_category: ErrorCategory = ErrorCategory.UNKNOWN
    attempt_count: int = 0
    file_sizes: List[int] = field(default_factory=list)  # File sizes in bytes
    duration_seconds: Optional[float] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    additional_metadata: Dict[str, Any] = field(default_factory=dict)
    
    def get_summary(self) -> Dict[str, Any]:
        """Generate summary for CSV update"""
        return {
            'status': self.status.value,
            'files': ','.join(self.files_downloaded),
            'media_id': self.media_id or '',
            'error': self.error_message or '',
            'last_attempt': datetime.now().isoformat(),
            'attempt_count': self.attempt_count,
            'total_size_mb': sum(self.file_sizes) / (1024 * 1024) if self.file_sizes else 0
        }
    
    def save_metadata(self, downloads_dir: str) -> str:
        """Save metadata file with complete context"""
        if not self.metadata_file:
            return ""
            
        metadata_path = os.path.join(downloads_dir, self.metadata_file)
        ensure_directory_exists(Path(downloads_dir))
        
        metadata = {
            'download_result': {
                'success': self.success,
                'status': self.status.value,
                'files_downloaded': self.files_downloaded,
                'media_id': self.media_id,
                'error_message': self.error_message,
                'error_category': self.error_category.value,
                'download_type': self.download_type,
                'attempt_count': self.attempt_count,
                'file_sizes': self.file_sizes,
                'total_size_mb': sum(self.file_sizes) / (1024 * 1024) if self.file_sizes else 0,
                'duration_seconds': self.duration_seconds,
                'start_time': self.start_time.isoformat() if self.start_time else None,
                'end_time': self.end_time.isoformat() if self.end_time else None,
                **self.additional_metadata
            },
            **self.row_context.to_metadata_dict()
        }
        
        try:
            # DRY: Use consolidated JSON save from utils/config.py
            from .import_utils import setup_project_path, safe_import
            setup_project_path()
            safe_json_save = safe_import('utils.config', ['safe_json_save'])
            
            safe_json_save(metadata, metadata_path)
            return metadata_path
        except Exception as e:
            print(f"Warning: Could not save metadata file {metadata_path}: {e}")
            return ""
    
    # DRY: Helper methods for common operations
    def is_retryable(self) -> bool:
        """Check if download error is temporary and should be retried"""
        if self.permanent_failure:
            return False
        return self.error_category in [
            ErrorCategory.TEMPORARY, 
            ErrorCategory.NETWORK, 
            ErrorCategory.RATE_LIMIT
        ]
    
    def should_skip(self) -> bool:
        """Check if download should be skipped (permanent failure)"""
        return self.permanent_failure or self.error_category in [
            ErrorCategory.PERMANENT,
            ErrorCategory.NOT_FOUND
        ]
    
    def mark_complete(self, files: List[str], sizes: Optional[List[int]] = None):
        """Mark download as completed with file info"""
        self.success = True
        self.status = DownloadStatus.COMPLETED
        self.files_downloaded = files
        if sizes:
            self.file_sizes = sizes
        self.end_time = datetime.now()
        if self.start_time:
            self.duration_seconds = (self.end_time - self.start_time).total_seconds()
    
    def mark_failed(self, error: str, category: ErrorCategory = ErrorCategory.UNKNOWN):
        """Mark download as failed with error info"""
        self.success = False
        self.status = DownloadStatus.FAILED
        self.error_message = error
        self.error_category = category
        self.permanent_failure = category in [ErrorCategory.PERMANENT, ErrorCategory.NOT_FOUND]
        self.end_time = datetime.now()
        if self.start_time:
            self.duration_seconds = (self.end_time - self.start_time).total_seconds()
    
    def to_csv_fields(self) -> Dict[str, Any]:
        """Convert to CSV-compatible fields for atomic updates"""
        prefix = f"{self.download_type}_"
        return {
            f"{prefix}status": self.status.value,
            f"{prefix}files": ','.join(self.files_downloaded),
            f"{prefix}media_id": self.media_id or '',
            f"{prefix}error": self.error_message or '',
            f"{prefix}last_attempt": datetime.now().isoformat(),
            f"{prefix}attempt_count": self.attempt_count,
            f"{prefix}size_mb": sum(self.file_sizes) / (1024 * 1024) if self.file_sizes else 0
        }


# DRY: Consolidated download statistics aggregator
@dataclass
class DownloadStats:
    """Aggregate download statistics across all download types"""
    # YouTube stats
    youtube_pending: int = 0
    youtube_in_progress: int = 0
    youtube_completed: int = 0
    youtube_failed: int = 0
    youtube_skipped: int = 0
    youtube_success_rate: float = 0.0
    
    # Drive stats
    drive_pending: int = 0
    drive_in_progress: int = 0
    drive_completed: int = 0
    drive_failed: int = 0
    drive_skipped: int = 0
    drive_success_rate: float = 0.0
    
    # Overall stats
    total_files_downloaded: int = 0
    total_size_mb: float = 0.0
    total_duration_seconds: float = 0.0
    error_categories: Dict[str, int] = field(default_factory=dict)
    
    # Thread-safe tracking
    _lock: threading.Lock = field(default_factory=threading.Lock, init=False, repr=False)
    
    def update_from_result(self, result: DownloadResult):
        """Update stats from a download result (thread-safe)"""
        with self._lock:
            # Update type-specific counters
            type_prefix = result.download_type
            status_field = f"{type_prefix}_{result.status.value}"
            
            if hasattr(self, status_field):
                setattr(self, status_field, getattr(self, status_field) + 1)
            
            # Update overall stats
            if result.success:
                self.total_files_downloaded += len(result.files_downloaded)
                if result.file_sizes:
                    self.total_size_mb += sum(result.file_sizes) / (1024 * 1024)
                if result.duration_seconds:
                    self.total_duration_seconds += result.duration_seconds
            
            # Track error categories
            if result.error_message:
                category = result.error_category.value
                self.error_categories[category] = self.error_categories.get(category, 0) + 1
            
            # Recalculate success rates
            self._recalculate_success_rates()
    
    def _recalculate_success_rates(self):
        """Recalculate success rates for each download type"""
        # YouTube success rate
        youtube_total = self.youtube_completed + self.youtube_failed
        if youtube_total > 0:
            self.youtube_success_rate = (self.youtube_completed / youtube_total) * 100
        
        # Drive success rate
        drive_total = self.drive_completed + self.drive_failed
        if drive_total > 0:
            self.drive_success_rate = (self.drive_completed / drive_total) * 100
    
    def get_summary(self) -> Dict[str, Any]:
        """Get summary statistics"""
        with self._lock:
            return {
                'youtube': {
                    'pending': self.youtube_pending,
                    'in_progress': self.youtube_in_progress,
                    'completed': self.youtube_completed,
                    'failed': self.youtube_failed,
                    'skipped': self.youtube_skipped,
                    'success_rate': f"{self.youtube_success_rate:.1f}%"
                },
                'drive': {
                    'pending': self.drive_pending,
                    'in_progress': self.drive_in_progress,
                    'completed': self.drive_completed,
                    'failed': self.drive_failed,
                    'skipped': self.drive_skipped,
                    'success_rate': f"{self.drive_success_rate:.1f}%"
                },
                'overall': {
                    'total_files': self.total_files_downloaded,
                    'total_size_mb': round(self.total_size_mb, 2),
                    'total_duration_minutes': round(self.total_duration_seconds / 60, 1),
                    'error_categories': dict(self.error_categories)
                }
            }


# DRY: Error categorization helper
def categorize_error(error_message: str) -> ErrorCategory:
    """Categorize an error message for better retry logic"""
    if not error_message:
        return ErrorCategory.UNKNOWN
    
    error_lower = error_message.lower()
    
    # Permanent failures
    permanent_phrases = [
        'video unavailable', 'removed by uploader', 'deleted',
        'private video', 'video not available', 'this video has been removed',
        'video is private', 'copyright claim', 'copyright grounds',
        'account associated with this video has been terminated',
        'no longer available', 'file not found', '404', 'access denied',
        'permission denied', 'forbidden'
    ]
    if any(phrase in error_lower for phrase in permanent_phrases):
        return ErrorCategory.PERMANENT
    
    # Not found errors
    if any(phrase in error_lower for phrase in ['not found', '404', 'does not exist']):
        return ErrorCategory.NOT_FOUND
    
    # Permission errors
    if any(phrase in error_lower for phrase in ['permission', 'unauthorized', 'forbidden', 'access denied']):
        return ErrorCategory.PERMISSION
    
    # Network errors
    network_phrases = [
        'connection', 'timeout', 'timed out', 'network', 'socket',
        'dns', 'resolve', 'urlopen error', 'connection reset',
        'connection refused', 'temporary failure'
    ]
    if any(phrase in error_lower for phrase in network_phrases):
        return ErrorCategory.NETWORK
    
    # Rate limit errors
    if any(phrase in error_lower for phrase in ['rate limit', 'too many requests', '429']):
        return ErrorCategory.RATE_LIMIT
    
    # Default to temporary for other errors
    return ErrorCategory.TEMPORARY


def create_download_result(
    download_type: str,
    row_context: RowContext,
    success: bool = False,
    error: Optional[str] = None
) -> DownloadResult:
    """Factory method to create a properly initialized DownloadResult"""
    result = DownloadResult(
        success=success,
        files_downloaded=[],
        media_id=None,
        error_message=error,
        metadata_file=None,
        row_context=row_context,
        download_type=download_type,
        status=DownloadStatus.PENDING,
        start_time=datetime.now()
    )
    
    if error:
        result.error_category = categorize_error(error)
        result.permanent_failure = result.error_category in [
            ErrorCategory.PERMANENT, 
            ErrorCategory.NOT_FOUND
        ]
    
    return result


def create_row_context_from_csv_row(row, row_index: int) -> RowContext:
    """Create RowContext from pandas DataFrame row"""
    return RowContext(
        row_id=str(row['row_id']),
        row_index=row_index,
        type=str(row['type']),
        name=str(row['name']),
        email=str(row['email'])
    )


def load_row_context_from_metadata(metadata_file_path: str) -> Optional[RowContext]:
    """Load RowContext from metadata file"""
    try:
        # DRY: Use consolidated JSON load from utils/config.py
        from .import_utils import setup_project_path, safe_import
        setup_project_path()
        safe_json_load = safe_import('utils.config', ['safe_json_load'])
        
        metadata = safe_json_load(metadata_file_path)
        if not metadata:
            return None
            
        return RowContext(
            row_id=metadata['source_csv_row_id'],
            row_index=metadata['source_csv_index'],
            type=metadata['personality_type'],
            name=metadata['person_name'],
            email=metadata['person_email']
        )
    except (KeyError) as e:
        print(f"Error loading row context from {metadata_file_path}: {e}")
        return None


def find_metadata_files(downloads_dir: str, pattern: str = "*_metadata.json") -> List[str]:
    """Find all metadata files in downloads directory"""
    import glob
    return glob.glob(os.path.join(downloads_dir, pattern))


def verify_type_preservation(original_type: str, metadata_file_path: str) -> bool:
    """Verify that type data was preserved in metadata"""
    try:
        # DRY: Use consolidated JSON load from utils/config.py
        from .import_utils import setup_project_path, safe_import
        setup_project_path()
        safe_json_load = safe_import('utils.config', ['safe_json_load'])
        
        metadata = safe_json_load(metadata_file_path)
        if not metadata:
            return False
        return metadata.get('personality_type') == original_type
    except:
        return False


if __name__ == "__main__":
    """CLI interface for row context operations"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Row Context Utility")
    parser.add_argument('--test-context', action='store_true',
                       help='Test RowContext creation and methods')
    parser.add_argument('--find-metadata', type=str,
                       help='Find metadata files in directory')
    parser.add_argument('--verify-preservation', nargs=2, metavar=('TYPE', 'METADATA_FILE'),
                       help='Verify type preservation in metadata file')
    
    args = parser.parse_args()
    
    if args.test_context:
        # Test RowContext functionality
        test_context = RowContext(
            row_id="487",
            row_index=2,
            type="FF-Fi/Se-CP/B(S) #4",
            name="Olivia Tomlinson",
            email="oliviatomlinson8@gmail.com"
        )
        
        print(f"Test RowContext:")
        print(f"  Name: {test_context.name}")
        print(f"  Type: {test_context.type}")
        print(f"  Filename suffix: {test_context.to_filename_suffix()}")
        print(f"  Safe name prefix: {test_context.to_safe_name_prefix()}")
        
        metadata = test_context.to_metadata_dict()
        print(f"  Metadata keys: {list(metadata.keys())}")
        
    elif args.find_metadata:
        files = find_metadata_files(args.find_metadata)
        print(f"Found {len(files)} metadata files in {args.find_metadata}:")
        for f in files[:10]:  # Show first 10
            print(f"  {os.path.basename(f)}")
        if len(files) > 10:
            print(f"  ... and {len(files) - 10} more")
            
    elif args.verify_preservation:
        original_type, metadata_file = args.verify_preservation
        preserved = verify_type_preservation(original_type, metadata_file)
        print(f"Type preservation check: {'✓' if preserved else '✗'}")
        if preserved:
            print(f"  Original type '{original_type}' correctly preserved")
        else:
            print(f"  ERROR: Type '{original_type}' not preserved correctly")
    else:
        print("Use --help to see available options")