# Personality Typing Content Manager

A production-ready system for downloading and tracking personality typing videos, documents, and transcripts from Google Sheets sources.

## 🚀 Features

- **Automated Content Collection**: Downloads YouTube videos/transcripts and Google Drive files
- **Row-Centric Tracking**: Maintains perfect CSV row relationships throughout the download pipeline
- **Data Integrity**: Preserves personality type data with atomic CSV updates
- **Bidirectional Mapping**: Links downloaded files back to source CSV rows
- **Error Resilience**: Intelligent retry system with permanent failure detection
- **Production Monitoring**: Real-time health checks and alerts
- **DRY Architecture**: Consolidated workflow with unified configuration management

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

## 🏗️ Architecture

This project has been refactored to follow DRY (Don't Repeat Yourself) principles:

### Consolidated Structure
- **Single Workflow**: `simple_workflow.py` is now the unified entry point (previously split across multiple versions)
- **Centralized Configuration**: All settings in `config/config.yaml`, loaded by `utils/config.py`
- **Eliminated Duplicates**: Removed redundant file structure and consolidated functionality

### Key Changes (Latest Refactoring)
1. **File Consolidation**: Merged `minimal/simple_workflow.py` (847 lines, full-featured) into root `simple_workflow.py`, replacing basic prototype (191 lines)
2. **Configuration Management**: Extracted hardcoded URLs and settings to use existing `config/config.yaml`
3. **Import Cleanup**: Updated all test files to use consolidated import structure
4. **Text-Only Processing**: Streamlined to extract raw text without HTML overhead

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

## 📁 Project Structure

```
.
├── run_complete_workflow.py    # Main entry point
├── utils/                      # Core utilities
│   ├── download_youtube.py     # YouTube downloader
│   ├── download_drive.py       # Google Drive downloader
│   ├── csv_manager.py          # Unified CSV operations (tracking, atomic, streaming)
│   ├── comprehensive_file_mapper.py  # Unified file mapping system
│   ├── monitoring.py           # System monitoring
│   └── ...                     # Other utilities
├── config/                     # Configuration files
├── outputs/                    # CSV output files
├── youtube_downloads/          # Downloaded videos
├── drive_downloads/            # Downloaded documents
└── logs/                       # System logs
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

**Note**: Legacy modules still work but show deprecation warnings. Update your code to use the new consolidated interfaces for better maintainability.

## 🔧 New DRY Patterns (Phase 2 Consolidation)

### CLI Parser Factory
Eliminates duplicate argparse patterns across 20+ scripts:

```python
from utils.config import create_standard_parser

# Create parser with standard arguments
parser = create_standard_parser("My Script Description", 
                               csv=True,      # Adds --csv argument
                               debug=True,    # Adds --debug argument
                               output=True)   # Adds --output argument

# Add custom arguments
parser.add_argument('--custom', help='Custom argument')

args = parser.parse_args()
```

**Standard arguments provided**:
- `--csv`: CSV file path (default from config)
- `--debug`: Enable debug logging
- `--output`: Output directory
- `--dry-run`: Simulate operations without changes
- `--verbose`: Increase output verbosity

### Centralized Download Utilities
Consolidated download patterns with progress tracking:

```python
from utils.download_utils import download_file_with_progress

# Download with progress bar and error handling
success = download_file_with_progress(
    response=http_response,
    output_path="/path/to/file.mp4",
    total_size=file_size_bytes,
    logger=logger,
    show_progress=True
)

if success:
    print("Download completed!")
```

**Features**:
- Adaptive chunk sizes based on file size
- Progress bar with speed indicators
- Atomic file operations (temp file → final)
- Centralized error handling
- MB/GB calculations using Constants

### Constants Class
Eliminates magic numbers throughout codebase:

```python
from utils.config import Constants

# File size calculations
file_size_mb = file_size_bytes / Constants.BYTES_PER_MB
file_size_gb = file_size_bytes / Constants.BYTES_PER_GB

# Progress display
progress_bar_width = Constants.DEFAULT_PROGRESS_BAR_WIDTH
update_interval = Constants.PROGRESS_UPDATE_INTERVAL
```

**Available constants**:
- `BYTES_PER_KB`: 1024
- `BYTES_PER_MB`: 1024 * 1024  
- `BYTES_PER_GB`: 1024 * 1024 * 1024
- `DEFAULT_PROGRESS_BAR_WIDTH`: 40
- `PROGRESS_UPDATE_INTERVAL`: 1.0 seconds

### Centralized Configuration Paths
Replace hardcoded paths with config lookups:

```python
from utils.config import get_config

config = get_config()

# Instead of hardcoded paths
output_csv = config.get('paths.output_csv', 'outputs/output.csv')
download_dir = config.get('paths.youtube_downloads', 'youtube_downloads')
```

## 🔄 Migration Guide (Phase 2)

### From Duplicate CLI Parsers to Standard Factory

**Old approach** (duplicate patterns):
```python
# Old way - duplicated in 20+ scripts
import argparse

parser = argparse.ArgumentParser(description='My script')
parser.add_argument('--csv', default='outputs/output.csv', 
                   help='Path to CSV file')
parser.add_argument('--debug', action='store_true', 
                   help='Enable debug logging')
```

**New approach** (centralized factory):
```python
# New way - single source of truth
from utils.config import create_standard_parser

parser = create_standard_parser("My script", csv=True, debug=True)
```

### From Duplicate Download Logic to Centralized Utilities

**Old approach** (duplicate download code):
```python
# Old way - duplicated across download scripts
with open(temp_path, 'wb') as f:
    downloaded = 0
    for chunk in response.iter_content(chunk_size=8192):
        if chunk:
            f.write(chunk)
            downloaded += len(chunk)
            # Progress calculation logic...
```

**New approach** (centralized utility):
```python
# New way - reusable function
from utils.download_utils import download_file_with_progress

success = download_file_with_progress(response, output_path, total_size, logger)
```

### From Magic Numbers to Constants

**Old approach** (scattered magic numbers):
```python
# Old way - magic numbers everywhere
file_size_mb = file_size / 1048576
chunk_size = 1024 * 1024
progress_width = 40
```

**New approach** (centralized constants):
```python
# New way - named constants
from utils.config import Constants

file_size_mb = file_size / Constants.BYTES_PER_MB
chunk_size = Constants.BYTES_PER_MB
progress_width = Constants.DEFAULT_PROGRESS_BAR_WIDTH
```

### From Hardcoded Paths to Configuration

**Old approach** (hardcoded paths):
```python
# Old way - paths scattered throughout code
csv_path = 'outputs/output.csv'
output_dir = 'youtube_downloads'
```

**New approach** (configuration-driven):
```python
# New way - centralized configuration
from utils.config import get_config

config = get_config()
csv_path = config.get('paths.output_csv', 'outputs/output.csv')
output_dir = config.get('paths.youtube_downloads', 'youtube_downloads')
```

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