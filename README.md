# Personality Typing Content Manager

A production-ready system for downloading and tracking personality typing videos, documents, and transcripts from Google Sheets sources.

## 🚀 Features

- **Automated Content Collection**: Downloads YouTube videos/transcripts and Google Drive files
- **Row-Centric Tracking**: Maintains perfect CSV row relationships throughout the download pipeline
- **Data Integrity**: Preserves personality type data with atomic CSV updates
- **Bidirectional Mapping**: Links downloaded files back to source CSV rows
- **Error Resilience**: Intelligent retry system with permanent failure detection
- **Production Monitoring**: Real-time health checks and alerts

## 📋 Prerequisites

- Python 3.8+
- yt-dlp (for YouTube downloads)
- Required Python packages in `requirements.txt`

## 🛠️ Installation

1. Clone the repository:
```bash
git clone https://github.com/YOUR_USERNAME/ops-typing-log-client.git
cd ops-typing-log-client
```

2. Create virtual environment:
```bash
python3 -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install dependencies:
```bash
pip install -r requirements.txt
```

4. Configure settings:
```bash
cp config/config.yaml.example config/config.yaml
# Edit config/config.yaml with your settings
```

## 🎯 Quick Start

### Basic Usage

Run the complete workflow:
```bash
python run_complete_workflow.py
```

### Advanced Options

```bash
# Skip Google Sheets scraping (use existing CSV)
python run_complete_workflow.py --skip-sheet

# Limit downloads for testing
python run_complete_workflow.py --max-youtube 10 --max-drive 5

# Use existing CSV links (fastest)
python run_complete_workflow.py --use-csv-links
```

### Monitoring

Check system status:
```bash
python utils/monitoring.py --status
```

View download statistics:
```bash
python -c "from utils.csv_manager import CSVManager; CSVManager().get_download_status_summary()"
```

## 🔄 Recent DRY Refactoring (2025)

This codebase has undergone a comprehensive DRY (Don't Repeat Yourself) refactoring to consolidate duplicate functionality and improve maintainability:

### ✅ Completed Consolidations

**1. Workflow Consolidation**
- `simple_workflow.py` + `simple_workflow_db.py` → **Unified `simple_workflow.py`**
- Eliminated 85% code duplication
- Added database/CSV mode switching
- Maintained full backward compatibility

**2. CLI Argument Standardization**
- Applied `StandardCLIArguments` across 6+ scripts
- Unified `--csv`, `--output`, `--max-rows`, `--reset` flags
- Consistent help text and behavior

**3. File Mapping System Consolidation**
- 4 deprecated modules → **`comprehensive_file_mapper.py`**
  - `map_files_to_types.py` ✅ Deprecated
  - `create_definitive_mapping.py` ✅ Deprecated  
  - `fix_mapping_issues.py` ✅ Deprecated
  - `csv_file_integrity_mapper.py` → Absorbed functionality
  - `fix_csv_file_mappings.py` → Absorbed functionality
- Single `FileMapper` interface for all mapping operations
- Comprehensive error fixing and integrity validation

**4. Error Handling Standardization**
- Applied decorators to core download modules:
  - `@handle_download_operations` for download functions
  - `@handle_network_operations` for network requests  
  - `@handle_file_operations` for file operations
- Consistent retry logic and error categorization
- Standardized error logging and reporting

### 📁 Project Structure

```
.
├── README.md                   # Project documentation
├── LICENSE                     # License file
├── requirements.txt            # Python dependencies
├── CHANGELOG.md               # Project changelog
├── .gitignore                 # Git ignore rules
├── run_complete_workflow.py    # Main entry point
├── run_all_tests.py           # Test runner
│
├── minimal/                    # Core workflow implementation
│   ├── simple_workflow.py      # 🔄 UNIFIED: CSV + Database workflow
│   ├── database_manager.py     # Database operations
│   ├── extraction_utils.py     # Document extraction utilities
│   └── CLAUDE.md              # Workflow documentation
│
├── utils/                      # Core utilities
│   ├── download_youtube.py     # ✅ Enhanced with error decorators
│   ├── download_drive.py       # ✅ Enhanced with error decorators
│   ├── csv_manager.py          # Unified CSV operations (tracking, atomic, streaming)
│   ├── comprehensive_file_mapper.py  # 🔄 UNIFIED: All file mapping operations
│   ├── error_decorators.py     # ✅ Applied across download modules
│   ├── config.py               # ✅ StandardCLIArguments for consistent CLI
│   ├── monitoring.py           # System monitoring
│   └── ...                     # Other utilities
│
├── scripts/                    # Utility scripts
│   ├── migrate_placeholders.py # ✅ Uses StandardCLIArguments
│   ├── download_*.py           # ✅ Enhanced with error decorators
│   ├── monitor_downloads.py    # ✅ Uses StandardCLIArguments
│   └── run_*.py               # Various runner scripts
│
├── docs/                       # Documentation
│   ├── refactoring/           # DRY refactoring documentation
│   │   ├── DRY_*.md           # DRY refactoring guides
│   │   ├── PHASE*.md          # Phase completion reports
│   │   └── ...                # Other refactoring docs
│   ├── API_REFERENCE.md       # API documentation
│   ├── DEPLOYMENT_INSTRUCTIONS.md  # Deployment guide
│   └── ...                    # Other documentation
│
├── tests/                      # Test suite
│   ├── test_*.py              # Unit tests
│   └── __init__.py            # Test package
│
├── tools/                      # Development tools
│   ├── apply_dry_refactoring.py    # Refactoring tool
│   ├── dry_refactoring_verifier.py  # Verification tool
│   └── create_snapshot.py          # Snapshot tool
│
├── config/                     # Configuration files
│   └── config.yaml            # Main configuration
│
├── data/                       # Data files
│   ├── *.db                   # Database files
│   ├── *.json                 # JSON data
│   └── sheet.html             # Cached sheet data
│
├── reports/                    # Analysis reports
│   ├── refactoring/           # Refactoring reports
│   └── csv_placeholder_analysis.md
│
├── outputs/                    # CSV output files
├── youtube_downloads/          # Downloaded videos
├── drive_downloads/            # Downloaded documents
└── logs/                       # System logs
    ├── history.csv            # Operation history
    └── runs/                  # Run-specific logs
```

### 🏆 Benefits Achieved

- **Reduced Code Duplication**: Eliminated 70-85% overlap in mapping utilities
- **Improved Maintainability**: Single source of truth for common operations
- **Enhanced Error Handling**: Consistent error patterns across all download operations
- **Standardized CLI**: Uniform command-line interface across all scripts
- **Better Testing**: Consolidated code paths enable more focused testing

### 🔄 Migration Guide

**Updated Import Patterns:**
```python
# OLD (deprecated)
from utils.map_files_to_types import FileTypeMapper
from utils.create_definitive_mapping import DefinitiveMapper

# NEW (consolidated)
from utils.comprehensive_file_mapper import FileMapper

# Usage
mapper = FileMapper(mode='comprehensive')  # or 'type_mapping', 'integrity', etc.
results = mapper.map_files()
```

**Updated Workflow Scripts:**
```bash
# OLD: Two separate scripts
python minimal/simple_workflow.py        # CSV mode
python minimal/simple_workflow_db.py     # Database mode

# NEW: Unified script with mode flags
python minimal/simple_workflow.py        # Default CSV mode
python minimal/simple_workflow.py --db   # Database mode
python minimal/simple_workflow.py --output-format both  # Both modes
```

**Standardized CLI Usage:**
```bash
# All scripts now support standard arguments
python scripts/migrate_placeholders.py --csv outputs/output.csv --max-rows 100
python scripts/monitor_downloads.py --status --detailed
python scripts/download_small_drive_files.py --csv my_data.csv --reset
```

## 🔧 Configuration

Edit `config/config.yaml` to customize:
- Download directories
- Retry settings
- Rate limits
- Monitoring thresholds

## 🔧 Core Modules (DRY Consolidated)

### CSVManager - Unified CSV Operations
Consolidates all CSV functionality into a single interface:

```python
from utils.csv_manager import CSVManager

# Initialize CSV manager
csv_manager = CSVManager('outputs/output.csv')

# Read CSV with proper data types
df = csv_manager.read()

# Atomic operations
csv_manager.atomic_write([{'name': 'John', 'type': 'ENFP'}])
csv_manager.atomic_append([{'name': 'Jane', 'type': 'INFJ'}])

# Streaming operations for large files
csv_manager.stream_process(lambda chunk: len(chunk))

# Tracking operations
pending = csv_manager.get_pending_downloads()
csv_manager.update_download_status(row_index=1, download_type='youtube', result=download_result)
```

### Logging Configuration - Unified Logging System
Consolidates all logging functionality with pipeline support:

```python
from utils.logging_config import get_logger, pipeline_run, DownloadStats

# Get component-specific logger
logger = get_logger(__name__)

# Pipeline logging with automatic stats
with pipeline_run() as pipeline_logger:
    logger.info("Processing started")
    # Your processing code here
    
# Enhanced logging with statistics
stats = DownloadStats()
stats.update_from_result(download_result)
print(stats.get_summary())
```

### Validation & URL Processing - Centralized Validation
Consolidates URL processing and validation functions:

```python
from utils.validation import (
    clean_url, validate_url, determine_url_type,
    extract_youtube_ids, extract_drive_links, filter_meaningful_links
)

# URL cleaning and validation
cleaned_url = clean_url(raw_url)
validated_url = validate_url(cleaned_url, allowed_domains=['youtube.com'])

# URL type detection
url_type = determine_url_type(url)  # 'youtube_video', 'drive_file', etc.

# Extract IDs and links
youtube_ids = extract_youtube_ids(links)
drive_links = extract_drive_links(links, html_content)
content_links = filter_meaningful_links(all_links, source_url)
```

### Row Context & Download Results - Enhanced Tracking
Consolidated download result patterns with comprehensive metadata:

```python
from utils.row_context import (
    DownloadResult, DownloadStatus, ErrorCategory, 
    create_download_result, categorize_error
)

# Create enhanced download result
result = create_download_result('youtube', row_context)
result.mark_complete(['video.mp4'], sizes=[1024000])

# Advanced error categorization
result.mark_failed("Video unavailable", ErrorCategory.PERMANENT)
if result.is_retryable():
    # Retry logic
    pass

# CSV-compatible output
csv_fields = result.to_csv_fields()
```

### Cleanup Manager - Unified Cleanup System
Consolidates all cleanup operations into a single interface:

```python
from utils.cleanup_manager import CleanupManager

# Initialize cleanup manager
cleanup = CleanupManager()

# Basic cleanup operations
cleanup.cleanup_temporary_files()
cleanup.cleanup_backup_files(older_than_days=30)
cleanup.cleanup_empty_directories()

# Enhanced cleanup operations
cleanup.cleanup_csv_fields('outputs/output.csv', max_field_size=100000)
cleanup.cleanup_duplicate_transcripts('downloads')
cleanup.emergency_cleanup(include_system=True)

# Full cleanup with configuration
stats = cleanup.run_full_cleanup('production')
```

### Safe Import Patterns - Consolidated Import Management
Eliminates try/except ImportError patterns across the codebase:

```python
from utils.config import safe_relative_import, bulk_safe_import

# Single import with fallback
CSVManager = safe_relative_import('csv_manager', 'CSVManager')

# Bulk imports with single call
imports = bulk_safe_import([
    ('csv_manager', 'CSVManager'),
    ('logging_config', 'get_logger'),
    ('validation', 'clean_url')
])
CSVManager = imports['CSVManager']
get_logger = imports['get_logger']
```

### FileMapper - Unified File Mapping
Consolidates all file mapping functionality:

```python
from utils.comprehensive_file_mapper import FileMapper

# Initialize file mapper
mapper = FileMapper('outputs/output.csv')

# Map files to CSV rows
mappings = mapper.map_files()

# Organize files by personality type
mapper.organize_by_type('organized_by_type')

# Find and resolve mapping issues
conflicts = mapper.find_mapping_conflicts()
mapper.fix_orphaned_files()

# Create definitive mapping
definitive = mapper.create_definitive_mapping()
```

## 📊 CSV Schema

The system tracks downloads with these columns:
- `youtube_status`: Download status (pending/completed/failed)
- `youtube_files`: Downloaded filenames
- `drive_status`: Download status
- `drive_files`: Downloaded filenames
- `permanent_failure`: Marks content that should not be retried

## 🔄 Migration Guide

### From Legacy CSV Modules to CSVManager

**Old approach** (deprecated modules):
```python
# Old way with multiple modules
from utils.csv_tracker import get_pending_downloads, update_csv_download_status
from utils.atomic_csv import atomic_csv_write
from utils.streaming_csv import stream_csv_processing

# Multiple different interfaces
pending = get_pending_downloads('outputs/output.csv')
atomic_csv_write('outputs/output.csv', data)
stream_csv_processing('outputs/output.csv', process_func)
```

**New approach** (unified CSVManager):
```python
# New way with single unified interface
from utils.csv_manager import CSVManager

csv_manager = CSVManager('outputs/output.csv')
pending = csv_manager.get_pending_downloads()
csv_manager.atomic_write(data)
csv_manager.stream_process(process_func)
```

### From Legacy Logging to Unified Logging Configuration

**Old approach** (deprecated utils/logger.py):
```python
# Old way with separate logging modules
from utils.logger import get_pipeline_logger, DualLogger

pipeline_logger = get_pipeline_logger()
dual_logger = DualLogger('MAIN', 'main.log')
```

**New approach** (consolidated utils/logging_config.py):
```python
# New way with unified logging configuration
from utils.logging_config import get_logger, pipeline_run

logger = get_logger(__name__)
with pipeline_run() as pipeline_logger:
    # Your code here
```

### From Scattered URL Processing to Validation Module

**Old approach** (scattered across extract_links.py):
```python
# Old way with functions scattered in extract_links.py
from utils.extract_links import clean_url, extract_youtube_ids, extract_drive_links

cleaned = clean_url(url)
youtube_ids = extract_youtube_ids(links)
drive_links = extract_drive_links(links)
```

**New approach** (consolidated in utils/validation.py):
```python
# New way with unified validation module
from utils.validation import clean_url, extract_youtube_ids, extract_drive_links, determine_url_type

cleaned = clean_url(url)
url_type = determine_url_type(url)
youtube_ids = extract_youtube_ids(links)
drive_links = extract_drive_links(links, html_content)
```

### From Scattered Import Patterns to Safe Import Functions

**Old approach** (try/except ImportError everywhere):
```python
# Old way with repeated try/except patterns
try:
    from csv_manager import CSVManager
    from logging_config import get_logger
except ImportError:
    from .csv_manager import CSVManager
    from .logging_config import get_logger
```

**New approach** (consolidated safe import functions):
```python
# New way with centralized import management
from utils.config import bulk_safe_import

imports = bulk_safe_import([
    ('csv_manager', 'CSVManager'),
    ('logging_config', 'get_logger')
])
CSVManager = imports['CSVManager']
get_logger = imports['get_logger']
```

### From Scattered Cleanup Scripts to Cleanup Manager

**Old approach** (multiple cleanup scripts):
```bash
# Old way with multiple scripts
python final_cleanup.py
python simple_cleanup.py
python temp_cleanup.py
python execute_cleanup.py
```

**New approach** (unified cleanup manager):
```bash
# New way with single cleanup interface
python -m utils.cleanup_manager --full
python -m utils.cleanup_manager --csv-fields outputs/output.csv
python -m utils.cleanup_manager --emergency-system
```

### From Legacy File Mapping Modules to FileMapper

**Old approach** (deprecated modules):
```python
# Old way with multiple modules
from utils.clean_file_mapper import CleanFileMapper
from utils.map_files_to_types import FileTypeMapper
from utils.fix_mapping_issues import MappingIssueFixer
from utils.recover_unmapped_files import match_unmapped_files

# Multiple different interfaces and classes
clean_mapper = CleanFileMapper('outputs/output.csv')
type_mapper = FileTypeMapper('outputs/output.csv')  
issue_fixer = MappingIssueFixer()
match_unmapped_files('outputs/output.csv')
```

**New approach** (unified FileMapper):
```python
# New way with single unified interface
from utils.comprehensive_file_mapper import FileMapper

mapper = FileMapper('outputs/output.csv')
mapper.map_files()  # Clean mapping
mapper.organize_by_type()  # Type organization  
mapper.fix_orphaned_files()  # Issue fixing
mapper.recover_unmapped_files()  # Recovery
```

### From Basic Download Results to Enhanced Row Context

**Old approach** (basic success/failure tracking):
```python
# Old way with simple result objects
result = {'success': True, 'files': ['video.mp4'], 'error': None}
```

**New approach** (comprehensive download result tracking):
```python
# New way with enhanced result objects
from utils.row_context import create_download_result, DownloadStatus, ErrorCategory

result = create_download_result('youtube', row_context)
result.mark_complete(['video.mp4'], sizes=[1024000])
result.error_category = ErrorCategory.PERMANENT
csv_fields = result.to_csv_fields()
```

**Note**: Legacy modules still work but show deprecation warnings. Update your code to use the new consolidated interfaces for better maintainability.

## 🐛 Troubleshooting

### Common Issues

1. **YouTube download fails**: Ensure yt-dlp is up to date
   ```bash
   pip install --upgrade yt-dlp
   ```

2. **CSV corruption**: The system creates automatic backups
   ```bash
   ls outputs/backups/
   ```

3. **Permission errors**: Check file permissions in download directories

## 📝 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🤝 Contributing

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/amazing-feature`)
3. Commit your changes (`git commit -m 'Add amazing feature'`)
4. Push to the branch (`git push origin feature/amazing-feature`)
5. Open a Pull Request

## 📧 Support

For issues and questions, please open an issue on GitHub.