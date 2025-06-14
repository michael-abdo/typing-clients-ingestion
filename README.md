# Personality Typing Content Manager

A production-ready system for downloading and tracking personality typing videos, documents, and transcripts with complete CSV row integrity and type preservation.

## Features

### Core Download & Tracking System
- **Row-centric download tracking** - Maintains perfect CSV row relationships throughout download pipeline
- **Type column preservation** - Protects critical personality type data (e.g., "FF-Fi/Se-CP/B(S) #4")
- **Atomic CSV updates** - Prevents data corruption during concurrent operations
- **Bidirectional file mapping** - Links downloaded files back to source CSV rows via metadata
- **Intelligent retry system** - Categorizes errors and retries based on failure type
- **Production monitoring** - Real-time health checks with configurable alerts

### Media Downloads
- **YouTube downloads** with playlist support and automatic transcript extraction
- **Google Drive individual files** (including large files >100MB with virus scan warnings)
- **Google Drive folder downloads** (1:many relationship) - Downloads all files from public folders
- **Permanent failure detection** - Marks deleted/private videos to skip future retries
- **Embedded metadata** in all downloads for complete traceability
- **Rate limiting and error handling** for reliable batch processing

### Data Processing
- Extract all links from webpages, including embedded Google Drive links
- Scrape data from published Google Sheets with automatic updates
- Export scraped data to CSV with duplicate prevention
- Link extraction and YouTube playlist generation

## Directory Structure

### Root Directory (Essential Files Only)
- `README.md` - Project documentation and setup instructions
- `CLAUDE.md` - AI assistant instructions and workflows  
- `requirements.txt` - Python dependencies specification
- `run_complete_workflow.py` - Main entry point for complete processing pipeline
- `.gitignore` - Git configuration for ignored files

### Core System Directories
- `/utils/` - Core utility modules for web scraping and data processing
  - `download_youtube.py` - YouTube video and transcript downloader with row tracking
  - `download_drive.py` - Google Drive file downloader with metadata
  - `csv_tracker.py` - CSV row tracking and status management
  - `master_scraper.py` - Google Sheets scraping and link extraction
  - `monitoring.py` - System health monitoring and alerts
  - `config.py` - Configuration management and validation
  - `download_youtube_legacy.py` - Legacy YouTube downloader (moved from root)
- `/config/` - Configuration files
  - `config.yaml` - Main system configuration
- `/outputs/` - CSV output files and processed data
  - `output.csv` - Main data file with tracking columns
- `/scripts/` - Utility scripts and tools
  - `submit_all_jobs.sh` - Batch job submission script (moved from root)
  - `/archive/` - Archived and legacy scripts
    - `run_complete_workflow_old.py` - Previous workflow version
- `/docs/` - Project documentation and analysis files
  - `CLAUDE_INSTRUCTIONS.md` - Detailed AI instructions (moved from root)
  - `API_REFERENCE.md` - System API documentation
  - `PRODUCTION_ARCHITECTURE.md` - Architecture overview

### Data and Processing Directories  
- `/tests/` - Unit tests for validation, rate limiting, and utilities
- `/utilities/` - Helper scripts organized by purpose:
  - `/utilities/scripts/` - General purpose scripts
  - `/utilities/alternative_workflows/` - Alternative processing workflows
  - `/utilities/maintenance/` - Cleanup and maintenance scripts
- `/cache/` - Cached HTML and debug files from web scraping
- `/html_cache/` - Additional HTML cache storage
- `/backups/` - Automated backups of output files
- `/logs/` - Application logs organized by run timestamp (includes moved workflow logs)
- `/youtube_downloads/` - Downloaded YouTube videos and transcripts
- `/drive_downloads/` - Downloaded Google Drive files with metadata
- `/venv/` - Python virtual environment

## Google Sheet Table Structure

The table in the Google Sheet has the following structure:
- **Column 0**: ID/Number value (numeric identifier)
- **Column 1**: Freezebar cell (empty separation column)
- **Column 2**: Name (contains hyperlinked text)
- **Column 3**: Email address
- **Column 4**: Type/Classification (contains hyperlinked text with personality type codes)
- **Column 5**: Status indicator (contains "DONE" with colored formatting)
- **Column 6**: Empty/notes column

**Link Structure**:
- Names in column 2 are hyperlinked to Google documents
- Type information in column 4 is also hyperlinked, often to Google documents
- Links are formatted as Google redirect URLs that need to be extracted
- Real content starts around row 15

## Data Flow

```
                             ┌───────────────┐
                             │               │
                             │master_scraper │
                             │               │
                             └───┬───────┬───┘
                                 │       │
                 ┌───────────────┘       └───────────────┐
                 │                                       │
                 ▼                                       ▼
        ┌──────────────────┐                   ┌──────────────┐
        │                  │                   │              │
        │scrape_google_sheets│◄──┐             │extract_links │
        │                  │     │             │              │
        └────────┬─────────┘     │             └───────┬──────┘
                 │               │                     │
                 │               │                     │
                 ▼               │                     │
        ┌─────────────┐          │                     │
        │             │          │                     │
        │Google Sheets│          │                     │
        │             │          │                     │
        └─────────────┘          │                     │
                                 │                     │
                                 │                     │
                                 │                     │
                         ┌───────┴─────────┐           │
                         │                 │           │
                         │   output.csv    │◄──────────┘
                         │                 │
                         └────────┬────────┘
```

## Requirements

```
requests>=2.28.2
beautifulsoup4>=4.11.2
selenium>=4.8.0
webdriver-manager>=3.8.5
pathlib>=1.0.1
yt-dlp>=2025.4.30
python-dotenv>=1.0.0
pytest>=7.3.1
```

You can install all requirements using:

```bash
pip install -r requirements.txt
```

## Usage

### Extract Links and YouTube Videos

```python
from extract_links import process_url

# Process a webpage to extract links and YouTube videos
links, youtube_playlist = process_url("https://example.com")

# Print all extracted links
print(f"Found {len(links)} links")

# If YouTube videos were found, display the playlist URL
if youtube_playlist:
    print(f"YouTube playlist: {youtube_playlist}")
```

### Scrape Google Sheets Data

```python
from scrape_google_sheets import update_csv

# Update the URL in scrape_google_sheets.py first:
# URL = "https://docs.google.com/spreadsheets/d/your-sheet-id/pubhtml"

# Run the script to fetch data and update the CSV
update_csv()
```

### Master Scraper (Combined Workflow)

```python
from master_scraper import process_links_from_csv

# Run the master scraper to:
# 1. Get latest data from Google Sheets
# 2. Process each link from the CSV
# 3. Add extracted links and YouTube playlists to the CSV
process_links_from_csv()

# Command-line usage with options
# python master_scraper.py --force-download  # Force new download of Google Sheet
# python master_scraper.py --max-rows 10     # Process only 10 rows
# python master_scraper.py --reset           # Reprocess all rows, even if already processed
```

### Complete Workflow Automation

The `run_complete_workflow.py` script provides production-ready automation with row-centric tracking:
1. **Enhanced CSV Schema** - Automatically adds 9 tracking columns for download status
2. **Intelligent Processing** - Only processes pending downloads, skips completed and permanently failed rows
3. **Row Context Preservation** - Maintains personality type data throughout pipeline
4. **Atomic Updates** - Updates CSV after each download with full error tracking
5. **Permanent Failure Tracking** - Marks deleted/private content to prevent retry loops
6. **Google Drive Folder Support** - Downloads all files from public Drive folders (1:many relationship)
7. **Production Logging** - Comprehensive logs with duration, success rates, and error counts

```bash
# Activate virtual environment
source venv/bin/activate

# Run complete workflow with row tracking
python run_complete_workflow.py

# FASTEST: Use extracted links from CSV (skip Google Doc scraping)
python run_complete_workflow.py --use-csv-links

# Production batch processing with limits
python run_complete_workflow.py --max-youtube 10 --max-drive 5

# Skip specific steps while maintaining tracking
python run_complete_workflow.py --skip-sheet    # Skip Google Sheet scraping
python run_complete_workflow.py --skip-drive    # Skip Google Drive downloads
python run_complete_workflow.py --skip-youtube  # Skip YouTube downloads

# Combine CSV links with other flags for maximum efficiency
python run_complete_workflow.py --use-csv-links --skip-youtube  # Only Drive downloads
python run_complete_workflow.py --use-csv-links --max-drive 20  # Limit downloads
```

### Production Monitoring & Management

```bash
# System health monitoring
python utils/monitoring.py --status           # Quick system status
python utils/monitoring.py --report           # Full status report with recommendations
python utils/monitoring.py --alerts           # Check alert conditions
python utils/monitoring.py --metrics --detailed  # Detailed metrics

# Download status management
python utils/csv_tracker.py --status          # Download summary statistics
python utils/csv_tracker.py --failed both     # Show failed downloads
python utils/csv_tracker.py --reset-status 151 --reset-type both  # Reset specific row

# Error handling and validation
python utils/error_handling.py --validate-csv output.csv      # Validate CSV integrity
python utils/error_handling.py --validate-environment         # Check system dependencies
python utils/error_handling.py --test-error-handling         # Test error categorization
```

### CSV Schema Enhancement

The system automatically enhances your CSV with 8 tracking columns:

| Column | Purpose | Values |
|--------|---------|--------|
| `youtube_status` | YouTube download status | pending, downloading, completed, failed |
| `youtube_files` | List of downloaded YouTube files | Comma-separated filenames |
| `youtube_media_id` | YouTube video ID | Video identifier for tracking |
| `drive_status` | Google Drive download status | pending, downloading, completed, failed |
| `drive_files` | List of downloaded Drive files | Comma-separated filenames |
| `drive_media_id` | Google Drive file ID | File identifier for tracking |
| `last_download_attempt` | Timestamp of last attempt | ISO 8601 format |
| `download_errors` | Error messages from failures | Semicolon-separated error history |
| `permanent_failure` | Permanent failure markers | youtube,drive (skip future retries) |

### Download YouTube Videos and Transcripts

```bash
# Command-line usage
python download_youtube.py https://www.youtube.com/watch?v=VIDEO_ID
python download_youtube.py https://www.youtube.com/watch?v=VIDEO_ID --transcript-only
python download_youtube.py https://www.youtube.com/watch?v=VIDEO_ID --resolution 1080
```

### Download Google Drive Files and Folders

```bash
# Individual file download
python download_drive.py "https://drive.google.com/file/d/FILE_ID/view"
python download_drive.py "https://drive.google.com/file/d/FILE_ID/view" --filename custom_name.ext
python download_drive.py "https://drive.google.com/file/d/FILE_ID/view" --metadata

# Folder download (NEW: Downloads all files from public folders)
python download_drive.py "https://drive.google.com/drive/folders/FOLDER_ID"

# Download large files with virus scan warnings
python download_drive.py "https://drive.usercontent.google.com/download?id=FILE_ID&export=download&confirm=t&uuid=UUID"

# The script now automatically handles:
# - **Google Drive folders** (1:many relationship) - Downloads all contained files
# - Large files (>100MB) that show virus scan warnings
# - Direct download URLs from drive.usercontent.google.com
# - Progress tracking with speed and ETA
# - Automatic retry for confirmation pages
# - Folder metadata with individual file tracking
```

## New Features - Enhanced Error Handling & Folder Support

### Permanent Failure Detection (YouTube)

The system now intelligently detects and marks permanent YouTube failures to prevent endless retry loops:

**Detected Conditions:**
- "Video unavailable" / "This video has been removed by the uploader"
- "Private video" / "Video not available" 
- "Deleted" videos

**Behavior:**
- Automatically marks failures as permanent in the `permanent_failure` CSV column
- Skips permanently failed content in future workflow runs
- Preserves detailed error messages for debugging
- Maintains row integrity and personality type data

**CSV Tracking:**
```
permanent_failure: "youtube" (skips YouTube retries for this row)
```

### Google Drive Folder Downloads (1:Many Relationship)

**NEW:** The system now supports downloading entire Google Drive folders, not just individual files.

**Capabilities:**
- **Automatic Detection**: Recognizes folder URLs (`/drive/folders/FOLDER_ID`)
- **Bulk Download**: Downloads all publicly accessible files in the folder
- **Individual Tracking**: Each file gets its own metadata and tracking
- **Combined Metadata**: Folder-level metadata with download statistics
- **Error Resilience**: Continues downloading other files if some fail

**Supported URL Formats:**
```bash
# Individual files (existing)
https://drive.google.com/file/d/FILE_ID/view

# Folders (NEW)
https://drive.google.com/drive/folders/FOLDER_ID
```

**CSV Integration:**
- `drive_media_id`: Contains folder ID for folder downloads
- `drive_files`: Lists all successfully downloaded files from folder
- `download_errors`: Individual file failures within folder
- Row-centric tracking maintains personality type relationships

**Limitations:**
- Only works with publicly accessible folders
- Private folders return "No files found or folder not accessible"
- Uses HTML scraping (production would benefit from Google Drive API)

### Enhanced CSV Schema

The tracking system now includes 9 columns (was 8):

| Column | Purpose | Values |
|--------|---------|--------|
| `permanent_failure` | Skip retry markers | `youtube`, `drive`, `youtube,drive` |

This prevents the system from repeatedly attempting downloads that will never succeed, improving efficiency and reducing log noise.