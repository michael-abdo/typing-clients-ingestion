# Web Scraping Utilities

A collection of Python utilities for scraping web content, Google Sheets data, and downloading files from Google Drive and YouTube.

## Features

- Extract all links from a webpage, including embedded Google Drive links
- Extract YouTube video IDs from links
- Generate YouTube playlist URLs from extracted video IDs
- Scrape data from published Google Sheets
- Export scraped data to CSV, avoiding duplicates
- Download YouTube videos and transcripts
- Download files from Google Drive

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

The `run_complete_workflow.py` script automates the entire process:
1. Download and scrape a new Google Sheet
2. Extract and process links
3. Download all new Google Drive files
4. Download all new YouTube videos

```bash
# Activate virtual environment
source venv/bin/activate

# Run complete workflow
python run_complete_workflow.py

# Run with specific options
python run_complete_workflow.py --max-rows 10   # Process only 10 rows 
python run_complete_workflow.py --reset         # Reprocess all rows

# Skip specific steps if needed
python run_complete_workflow.py --skip-sheet    # Skip Google Sheet scraping
python run_complete_workflow.py --skip-drive    # Skip Google Drive downloads
python run_complete_workflow.py --skip-youtube  # Skip YouTube downloads
```

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
```