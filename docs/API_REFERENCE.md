# API Reference - Row-Centric Download Tracking System

## Core Data Classes

### RowContext
**File**: `utils/row_context.py`

Maintains CSV row relationships throughout the download pipeline.

```python
@dataclass
class RowContext:
    row_id: str          # Primary key from CSV
    row_index: int       # Position in CSV for atomic updates  
    type: str           # Personality type - CRITICAL to preserve
    name: str           # Person name for human-readable tracking
    email: str          # Additional identifier
```

**Methods**:
- `to_metadata_dict() -> Dict[str, Any]`: Embed row context in download metadata files
- `to_filename_suffix() -> str`: Create unique filename suffix for organization

### DownloadResult
**File**: `utils/row_context.py`

Standardized result object maintaining full traceability.

```python
@dataclass 
class DownloadResult:
    success: bool
    files_downloaded: List[str]     # Actual filenames created
    media_id: Optional[str]         # YouTube video_id or Drive file_id
    error_message: Optional[str]
    metadata_file: Optional[str]    # Path to metadata file
    row_context: RowContext         # Preserve complete source data
```

**Methods**:
- `get_summary() -> Dict[str, Any]`: Generate summary for CSV update

## CSV Tracking Functions

### Core Functions
**File**: `utils/csv_tracker.py`

#### `ensure_tracking_columns(csv_path: str = 'output.csv') -> bool`
Add tracking columns to CSV if they don't exist.

**Tracking Columns Added**:
- `youtube_status`: pending|downloading|completed|failed
- `youtube_files`: Comma-separated list of downloaded files
- `youtube_media_id`: YouTube video ID
- `drive_status`: pending|downloading|completed|failed  
- `drive_files`: Comma-separated list of downloaded files
- `drive_media_id`: Google Drive file ID
- `last_download_attempt`: ISO timestamp of last attempt
- `download_errors`: Error messages from failed downloads

#### `get_pending_downloads(csv_path: str, download_type: str = 'both', include_failed: bool = True, retry_attempts: int = 3) -> List[RowContext]`
Get list of rows that need downloads.

**Parameters**:
- `download_type`: 'both', 'youtube', or 'drive'
- `include_failed`: Include failed downloads for retry
- `retry_attempts`: Maximum retry attempts before giving up

**Returns**: List of `RowContext` objects for pending downloads

#### `update_csv_download_status(row_index: int, download_type: str, result: DownloadResult, csv_path: str = 'output.csv')`
Atomically update CSV with download results while preserving all existing data.

**Critical Features**:
- Uses file locking for concurrent safety
- Preserves personality type data with corruption detection
- Updates only download-specific columns
- Tracks error history with attempt counting

#### `reset_download_status(row_id: str, download_type: str, csv_path: str = 'output.csv') -> bool`
Reset download status for a specific row.

**Use Cases**:
- Retry failed downloads
- Clear corrupted state
- Force re-download

### Status and Summary Functions

#### `get_download_status_summary(csv_path: str = 'output.csv') -> Dict[str, Any]`
Get summary statistics of download status.

**Returns**:
```python
{
    'total_rows': int,
    'youtube': {
        'has_content': int,     # Rows with YouTube URLs
        'pending': int,
        'completed': int, 
        'failed': int
    },
    'drive': {
        'has_content': int,     # Rows with Drive URLs
        'pending': int,
        'completed': int,
        'failed': int
    }
}
```

#### `find_row_by_file(filename: str, downloads_dir: str = 'youtube_downloads') -> Optional[RowContext]`
Reverse lookup: find CSV row from downloaded filename.

**Implementation**: Searches for metadata files with embedded row context.

## Download Functions

### YouTube Downloads
**File**: `utils/download_youtube.py`

#### `download_youtube_with_context(url: str, row_context: RowContext, transcript_only=False, resolution=None, output_format=None) -> DownloadResult`
Download YouTube content with complete row tracking.

**Features**:
- Playlist support with individual video processing
- Transcript and video downloads
- Embedded metadata with row context
- Filename organization with row suffixes

### Google Drive Downloads  
**File**: `utils/download_drive.py`

#### `download_drive_with_context(url: str, row_context: RowContext, filename=None, metadata=True) -> DownloadResult`
Download Google Drive content with complete row tracking.

**Features**:
- Large file support (>100MB with virus scan warnings)
- Direct download URL handling
- Progress tracking with speed and ETA
- Embedded metadata with row context

## Error Handling

### Error Classes
**File**: `utils/error_handling.py`

#### `ErrorSeverity(Enum)`
- `INFO`: Informational messages
- `WARNING`: Non-critical issues
- `ERROR`: Significant problems requiring attention
- `CRITICAL`: System-threatening issues

#### `ErrorCategory(Enum)`
- `NETWORK`: Connection, timeout, DNS, SSL issues
- `FILE_IO`: File system operations
- `VALIDATION`: Data validation errors
- `PERMISSION`: Access denied, authorization issues
- `QUOTA`: Rate limits, quota exceeded
- `RATE_LIMIT`: Too many requests
- `SYSTEM`: System-level errors
- `DATA_CORRUPTION`: Data integrity issues

### ErrorContext
Complete error context for tracking and debugging.

```python
@dataclass
class ErrorContext:
    error_id: str
    severity: ErrorSeverity
    category: ErrorCategory
    message: str
    details: Optional[str] = None
    row_id: Optional[str] = None
    download_type: Optional[str] = None
    url: Optional[str] = None
    file_path: Optional[str] = None
    timestamp: str = None
    stack_trace: Optional[str] = None
```

### ErrorHandler
Production-ready error handling with categorization and recovery.

#### `handle_error(error: Exception, context: Dict[str, Any] = None) -> ErrorContext`
Handle and categorize errors with full context.

#### `should_retry(error_context: ErrorContext, attempt_count: int, max_attempts: int = 3) -> bool`
Determine if operation should be retried based on error type.

**Retry Logic**:
- ✅ Retry: Network errors, rate limits
- ❌ Don't retry: Validation errors, permission errors, critical system errors

#### `get_retry_delay(error_context: ErrorContext, attempt_count: int) -> float`
Calculate retry delay based on error type.

**Delay Strategies**:
- Rate limits: Exponential backoff up to 60 seconds
- Network errors: Progressive delay up to 30 seconds
- Other errors: Linear progression

### Validation Functions

#### `validate_csv_integrity(csv_path: str) -> List[ErrorContext]`
Validate CSV file integrity.

**Checks**:
- File existence and readability
- Required columns present
- No duplicate row IDs
- No empty critical fields

#### `validate_download_environment() -> List[ErrorContext]`
Validate download environment and dependencies.

**Checks**:
- Required directories exist
- Sufficient disk space (>1GB)
- yt-dlp availability and functionality

## Monitoring System

### System Metrics
**File**: `utils/monitoring.py`

#### `SystemMetrics`
```python
@dataclass
class SystemMetrics:
    timestamp: str
    total_rows: int
    pending_downloads: int
    completed_downloads: int
    failed_downloads: int
    success_rate: float
    avg_download_time: Optional[float]
    disk_usage_gb: float
    error_rate: float
    active_processes: int
```

#### `DownloadStats`
```python
@dataclass
class DownloadStats:
    youtube_pending: int = 0
    youtube_completed: int = 0
    youtube_failed: int = 0
    youtube_success_rate: float = 0.0
    drive_pending: int = 0
    drive_completed: int = 0
    drive_failed: int = 0
    drive_success_rate: float = 0.0
    total_files_downloaded: int = 0
    total_size_mb: float = 0.0
```

### DownloadMonitor
Production monitoring system for download tracking.

#### `collect_metrics() -> SystemMetrics`
Collect comprehensive system metrics.

#### `get_download_stats() -> DownloadStats`
Get detailed download statistics.

#### `check_alerts(metrics: SystemMetrics) -> List[Dict[str, Any]]`
Check alert conditions and return triggered alerts.

**Default Alert Conditions**:
- High failure rate: >20%
- Low disk space: <1GB
- Stuck downloads: >100 pending
- Critical errors: ≥1

#### `generate_report(include_details: bool = False) -> Dict[str, Any]`
Generate comprehensive status report.

**Report Contents**:
- Current system metrics
- Download statistics
- Historical trends (24-hour)
- Active alerts
- Actionable recommendations
- Failed download details (if requested)

## CLI Interfaces

### CSV Tracker CLI
```bash
python utils/csv_tracker.py --help
```

**Commands**:
- `--enhance-schema`: Add tracking columns
- `--status`: Show download status summary
- `--pending [both|youtube|drive]`: Show pending downloads
- `--failed [both|youtube|drive]`: Show failed downloads
- `--reset-status ROW_ID --reset-type [both|youtube|drive]`: Reset download status

### Monitoring CLI
```bash
python utils/monitoring.py --help
```

**Commands**:
- `--status`: Show current system status
- `--report`: Generate full status report  
- `--metrics`: Show current metrics
- `--alerts`: Check alert conditions
- `--detailed`: Include detailed information

### Error Handling CLI
```bash
python utils/error_handling.py --help
```

**Commands**:
- `--validate-csv CSV_PATH`: Validate CSV file integrity
- `--validate-environment`: Validate download environment
- `--test-error-handling`: Test error handling system

## Production Workflow Integration

### Main Workflow
**File**: `run_complete_workflow.py`

The production workflow integrates all components:

1. **Schema Enhancement**: Calls `ensure_tracking_columns()`
2. **Intelligent Processing**: Uses `get_pending_downloads()` to find work
3. **Row-Centric Downloads**: Passes `RowContext` to download functions
4. **Atomic Updates**: Uses `update_csv_download_status()` after each download
5. **Error Handling**: Integrates `ErrorHandler` for comprehensive error management
6. **Monitoring**: Generates run summaries with metrics and recommendations

### Usage Examples

```python
# Get pending downloads
pending = get_pending_downloads('output.csv', 'both', include_failed=True)

# Process each pending download
for row_context in pending:
    if 'youtube_playlist' in row_data and row_data.youtube_status == 'pending':
        result = download_youtube_with_context(url, row_context)
        update_csv_download_status(row_context.row_index, 'youtube', result)

# Monitor system health
monitor = DownloadMonitor()
metrics = monitor.collect_metrics()
alerts = monitor.check_alerts(metrics)
if alerts:
    print(f"⚠️ {len(alerts)} alerts triggered!")
```

This API provides complete control over the row-centric download tracking system while maintaining data integrity and production reliability.