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
- Download YouTube videos and transcripts with playlist support
- Download Google Drive files (including large files >100MB with virus scan warnings)
- Embedded metadata in all downloads for complete traceability
- Rate limiting and error handling for reliable batch processing

### Data Processing
- Extract all links from webpages, including embedded Google Drive links
- Scrape data from published Google Sheets with automatic updates
- Export scraped data to CSV with duplicate prevention
- Link extraction and YouTube playlist generation

## Directory Structure

- `/` - Main executable scripts and core project files
- `/utils/` - Core utility modules for web scraping and data processing
- `/docs/` - Project documentation and analysis files
- `/data/` - CSV output files and cached data
- `/tests/` - Unit tests for validation, rate limiting, and utilities
- `/utilities/` - Helper scripts organized by purpose:
  - `/utilities/scripts/` - General purpose scripts
  - `/utilities/alternative_workflows/` - Alternative processing workflows
  - `/utilities/maintenance/` - Cleanup and maintenance scripts
- `/cache/` - Cached HTML and debug files from web scraping
- `/html_cache/` - Additional HTML cache storage
- `/backups/` - Automated backups of output files
- `/logs/` - Application logs organized by run timestamp
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
1. **Enhanced CSV Schema** - Automatically adds 8 tracking columns for download status
2. **Intelligent Processing** - Only processes pending downloads, skips completed rows
3. **Row Context Preservation** - Maintains personality type data throughout pipeline
4. **Atomic Updates** - Updates CSV after each download with full error tracking
5. **Production Logging** - Comprehensive logs with duration, success rates, and error counts

```bash
# Activate virtual environment
source venv/bin/activate

# Run complete workflow with row tracking
python run_complete_workflow.py

# Production batch processing with limits
python run_complete_workflow.py --max-youtube 10 --max-drive 5

# Skip specific steps while maintaining tracking
python run_complete_workflow.py --skip-sheet    # Skip Google Sheet scraping
python run_complete_workflow.py --skip-drive    # Skip Google Drive downloads
python run_complete_workflow.py --skip-youtube  # Skip YouTube downloads
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

### Download YouTube Videos and Transcripts

```bash
# Command-line usage
python download_youtube.py https://www.youtube.com/watch?v=VIDEO_ID
python download_youtube.py https://www.youtube.com/watch?v=VIDEO_ID --transcript-only
python download_youtube.py https://www.youtube.com/watch?v=VIDEO_ID --resolution 1080
```

### Download Google Drive Files

```bash
# Command-line usage
python download_drive.py "https://drive.google.com/file/d/FILE_ID/view"
python download_drive.py "https://drive.google.com/file/d/FILE_ID/view" --filename custom_name.ext
python download_drive.py "https://drive.google.com/file/d/FILE_ID/view" --metadata

# Download large files with virus scan warnings
python download_drive.py "https://drive.usercontent.google.com/download?id=FILE_ID&export=download&confirm=t&uuid=UUID"

# The script now automatically handles:
# - Large files (>100MB) that show virus scan warnings
# - Direct download URLs from drive.usercontent.google.com
# - Progress tracking with speed and ETA
# - Automatic retry for confirmation pages
```