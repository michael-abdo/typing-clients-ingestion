# Complete Workflow Execution Map

**Date**: July 26, 2025  
**Status**: ✅ Complete  
**Purpose**: Visual map of complete file/function call chain for the entire typing clients ingestion pipeline

## Overview

This document provides a comprehensive ASCII visual representation of the complete workflow execution flow, showing every file and function called when running the pipeline from start to finish.

---

## 🎯 COMPLETE WORKFLOW EXECUTION FLOW

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           TYPING CLIENTS INGESTION PIPELINE                      │
│                         Complete File/Function Call Chain                        │
└─────────────────────────────────────────────────────────────────────────────────┘

🚀 ENTRY POINT: python simple_workflow.py [--basic|--text] [options]
│
├── simple_workflow.py::main()
│   ├── parse_arguments() → argparse configuration
│   ├── config loading from utils/config.py::get_config()
│   │   └── config/config.yaml ← Configuration loaded
│   │
│   ┌─────────────────────────────────────────────────────────────────────────────┐
│   │                              STEP 1: DOWNLOAD SHEET                         │
│   └─────────────────────────────────────────────────────────────────────────────┘
│   ├── step1_download_sheet()
│   │   ├── utils/http_pool.py::get() → HTTP request to Google Sheets
│   │   │   └── 🌐 https://docs.google.com/spreadsheets/d/[SHEET_ID]/edit
│   │   └── sheet.html ← Raw HTML cached locally
│   │
│   ┌─────────────────────────────────────────────────────────────────────────────┐
│   │                        STEP 2: EXTRACT PEOPLE & DOCS                       │
│   └─────────────────────────────────────────────────────────────────────────────┘
│   ├── step2_extract_people_and_docs(html_content)
│   │   ├── BeautifulSoup → Parse HTML table structure
│   │   ├── utils/extract_links.py::extract_actual_url() → Clean Google redirect URLs
│   │   ├── Row extraction: [row_id, name, email, type, doc_link]
│   │   ├── Link categorization:
│   │   │   ├── Google Docs: docs.google.com/document/[DOC_ID]
│   │   │   ├── Direct YouTube: youtube.com/watch?v=[VIDEO_ID]
│   │   │   └── Direct Drive: drive.google.com/file/d/[FILE_ID]
│   │   └── Returns: all_people[], people_with_docs[]
│   │
│   ┌─────────────────────────────────────────────────────────────────────────────┐
│   │                         PROCESSING MODE SELECTION                           │
│   └─────────────────────────────────────────────────────────────────────────────┘
│   │
│   ├─── BASIC MODE (--basic flag)
│   │    └── utils/csv_manager.py::create_record(mode='basic')
│   │        └── Minimal CSV: [row_id, name, email, type, link]
│   │
│   ├─── TEXT MODE (--text flag)
│   │    ├── Batch processing with progress tracking
│   │    ├── utils/extract_links.py::extract_text_with_retry()
│   │    │   ├── Selenium WebDriver initialization
│   │    │   ├── utils/patterns.py::get_selenium_driver()
│   │    │   ├── Google Doc text extraction
│   │    │   └── Error handling with retry logic
│   │    ├── Progress state management
│   │    │   ├── utils/config.py::load_json_state()
│   │    │   └── extraction_progress.json ← Progress tracking
│   │    └── utils/csv_manager.py::create_record(mode='text')
│   │
│   └─── FULL MODE (default)
│        │
│        ┌─────────────────────────────────────────────────────────────────────────┐
│        │                        STEP 3: SCRAPE DOC CONTENTS                     │
│        └─────────────────────────────────────────────────────────────────────────┘
│        ├── step3_scrape_doc_contents(doc_url)
│        │   ├── Google Doc Detection: "docs.google.com/document" in url
│        │   ├── utils/extract_links.py::extract_google_doc_text()
│        │   │   ├── utils/patterns.py::get_selenium_driver()
│        │   │   ├── Selenium navigation to document
│        │   │   ├── Wait for content loading
│        │   │   ├── Text extraction from DOM
│        │   │   └── utils/patterns.py::cleanup_selenium_driver()
│        │   ├── utils/http_pool.py::get() → HTML content extraction
│        │   └── Returns: (html_content, doc_text)
│        │
│        ┌─────────────────────────────────────────────────────────────────────────┐
│        │                         STEP 4: EXTRACT LINKS                          │
│        └─────────────────────────────────────────────────────────────────────────┘
│        ├── step4_extract_links(doc_content, doc_text)
│        │   ├── Content preparation & Unicode decoding
│        │   ├── utils/patterns.py::PatternRegistry patterns:
│        │   │   ├── YOUTUBE_VIDEO_FULL → youtube.com/watch?v=[VIDEO_ID]
│        │   │   ├── YOUTUBE_SHORT_FULL → youtu.be/[VIDEO_ID]
│        │   │   ├── YOUTUBE_PLAYLIST_FULL → youtube.com/playlist?list=[LIST_ID]
│        │   │   ├── DRIVE_FILE_FULL → drive.google.com/file/d/[FILE_ID]
│        │   │   ├── DRIVE_OPEN_FULL → drive.google.com/open?id=[FILE_ID]
│        │   │   ├── DRIVE_FOLDER_FULL → drive.google.com/drive/folders/[FOLDER_ID]
│        │   │   └── HTTP_URL → All http(s) links
│        │   ├── filter_meaningful_links() → Remove infrastructure noise
│        │   │   ├── Noise filtering: googleapis.com, gstatic.com, schema.org
│        │   │   ├── Content validation: actual video/file IDs required
│        │   │   └── URL normalization: standardized formats
│        │   └── Returns: {youtube: [], drive_files: [], drive_folders: [], all_links: []}
│        │
│        ┌─────────────────────────────────────────────────────────────────────────┐
│        │                      STEP 5: PROCESS EXTRACTED DATA                    │
│        └─────────────────────────────────────────────────────────────────────────┘
│        ├── step5_process_extracted_data(person, links, doc_text)
│        │   ├── utils/config.py::ensure_directory() → Create output directories
│        │   ├── filter_meaningful_links() → Secondary filtering
│        │   ├── utils/csv_manager.py::create_record(mode='full')
│        │   │   ├── Field mapping: row_id, name, email, type, link
│        │   │   ├── Links processing: youtube_playlist, google_drive JSON
│        │   │   ├── Metadata: document_text, total_links, processing_info
│        │   │   ├── S3 integration: file_uuids, s3_paths JSON columns
│        │   │   └── Timestamps: extracted_at, updated_at
│        │   └── Returns: complete_record_dict
│        │
│        ┌─────────────────────────────────────────────────────────────────────────┐
│        │                           STEP 6: MAP DATA TO CSV                      │
│        └─────────────────────────────────────────────────────────────────────────┘
│        └── step6_map_data(processed_records, basic_mode, text_mode, output_file)
│            ├── Column configuration from utils/config.py
│            │   ├── Basic: ['row_id', 'name', 'email', 'type', 'link']
│            │   ├── Text: Basic + ['document_text', 'processing_info']
│            │   └── Full: All 23 columns including S3 UUID mappings
│            ├── Record filtering & validation
│            ├── pandas.DataFrame creation
│            ├── utils/csv_manager.py::safe_csv_write()
│            │   ├── utils/file_lock.py::file_lock() → Atomic write protection
│            │   ├── CSV backup creation with timestamp
│            │   ├── Data validation & sanitization
│            │   ├── utils/csv_s3_versioning.py → S3 versioning support
│            │   └── outputs/output.csv ← Final CSV output
│            └── Statistics & summary reporting

┌─────────────────────────────────────────────────────────────────────────────────┐
│                              S3 STREAMING PIPELINE                              │
│                        (When media files need processing)                        │
└─────────────────────────────────────────────────────────────────────────────────┘

🔄 STREAMING EXECUTION: python core/stream_folder_contents_direct.py
│
├── core/stream_folder_contents_direct.py::stream_drive_folders_direct()
│   ├── utils/s3_manager.py::UnifiedS3Manager()
│   │   ├── S3Config loading: bucket, credentials, upload mode
│   │   └── boto3 S3 client initialization
│   ├── utils/download_drive.py::list_folder_files()
│   │   ├── Google Drive API authentication
│   │   ├── Folder content listing: file IDs, names, sizes
│   │   └── utils/download_drive.py::extract_file_id()
│   ├── For each file in folder:
│   │   ├── UUID generation: str(uuid.uuid4())
│   │   ├── S3 key creation: f"files/{uuid}{extension}"
│   │   ├── utils/s3_manager.py::stream_drive_to_s3()
│   │   │   ├── 🌐 Google Drive download URL construction
│   │   │   ├── requests.get(stream=True) → Streaming download
│   │   │   ├── BytesIO buffering → In-memory processing
│   │   │   ├── s3_client.upload_fileobj() → Direct S3 upload
│   │   │   └── ✅ Zero local storage usage
│   │   ├── CSV updating via utils/csv_manager.py
│   │   │   ├── JSON field updates: file_uuids, s3_paths
│   │   │   ├── Metadata preservation: original_filename, source
│   │   │   └── UUID → S3 path mapping storage
│   │   └── Progress tracking & error handling
│   └── Returns: complete_s3_upload_report

🔧 EXTENSION FIXING: python core/fix_s3_extensions.py
│
├── core/fix_s3_extensions.py::fix_s3_file_extensions()
│   ├── File mapping definition: UUID → {filename, extension, content_type}
│   ├── For each UUID mapping:
│   │   ├── boto3.s3_client.head_object() → Check file existence
│   │   ├── boto3.s3_client.copy_object() → Copy with new extension
│   │   │   ├── MetadataDirective='REPLACE'
│   │   │   ├── ContentType update: video/mp4, application/docx
│   │   │   └── Metadata preservation: original_filename
│   │   ├── boto3.s3_client.delete_object() → Remove old .bin file
│   │   └── CSV path updates: JSON field corrections
│   └── Returns: extension_fix_summary

🔄 METADATA PROCESSING: python core/process_pending_metadata_downloads.py
│
├── core/process_pending_metadata_downloads.py::main()
│   ├── utils/csv_manager.py::CSVManager.read()
│   ├── Metadata filtering: has_metadata=True, has_media=False
│   ├── For each pending person:
│   │   ├── utils/row_context.py::RowContext creation
│   │   ├── YouTube processing:
│   │   │   ├── _download_youtube_playlist_direct()
│   │   │   ├── yt-dlp command construction
│   │   │   ├── subprocess.run() → Direct YouTube download
│   │   │   └── Local → S3 upload via utils/s3_manager.py
│   │   ├── Google Drive processing:
│   │   │   ├── utils/download_drive.py::download_drive_file()
│   │   │   ├── Direct streaming via utils/s3_manager.py::stream_drive_to_s3()
│   │   │   └── UUID generation & CSV mapping
│   │   └── Progress tracking: metadata_download_progress.json
│   └── Returns: metadata_processing_report

┌─────────────────────────────────────────────────────────────────────────────────┐
│                              CRITICAL UTILITIES                                 │
│                           (Supporting Infrastructure)                            │
└─────────────────────────────────────────────────────────────────────────────────┘

📁 utils/config.py
├── get_config() → config/config.yaml loading
├── ensure_directory() → Path creation with permissions
├── ensure_parent_dir() → Parent directory validation
├── load_json_state() → JSON state file loading
├── save_json_state() → JSON state file saving
└── format_error_message() → Standardized error formatting

📁 utils/patterns.py
├── PatternRegistry class → Centralized regex patterns
├── extract_youtube_id() → Video ID extraction
├── extract_drive_id() → Drive file ID extraction
├── clean_url() → URL sanitization
├── normalize_whitespace() → Text normalization
├── get_selenium_driver() → WebDriver with configuration
└── cleanup_selenium_driver() → Resource cleanup

📁 utils/error_handling.py
├── with_standard_error_handling() → Decorator for error wrapping
├── create_error_context() → Context information generation
├── handle_download_error() → Download-specific error handling
├── handle_extraction_error() → Text extraction error handling
└── format_error_details() → Detailed error formatting

📁 utils/retry_utils.py
├── retry_with_backoff() → Exponential backoff retry logic
├── calculate_delay() → Dynamic delay calculation
├── should_retry() → Retry condition evaluation
└── retry_state_tracking() → Attempt counting & logging

📁 utils/http_pool.py
├── get() → Centralized HTTP requests with pooling
├── connection_pooling() → urllib3 PoolManager
├── request_retry_logic() → Built-in retry mechanisms
└── response_validation() → Status code & content validation

📁 utils/logging_config.py
├── get_logger() → Module-specific logger creation
├── setup_logging() → Global logging configuration
├── print_section_header() → Formatted section output
└── log_performance_metrics() → Execution timing

📁 utils/file_lock.py
├── file_lock() → Cross-process file locking
├── FileLock class → Context manager implementation
├── lock_acquisition() → Timeout & retry logic
└── lock_cleanup() → Resource cleanup on exit

┌─────────────────────────────────────────────────────────────────────────────────┐
│                              DATA FLOW SUMMARY                                  │
└─────────────────────────────────────────────────────────────────────────────────┘

🌐 INPUT SOURCES:
├── Google Sheets: https://docs.google.com/spreadsheets/d/[SHEET_ID]
├── Google Docs: https://docs.google.com/document/d/[DOC_ID]
├── YouTube Videos: https://www.youtube.com/watch?v=[VIDEO_ID]
├── YouTube Playlists: https://www.youtube.com/playlist?list=[LIST_ID]
├── Google Drive Files: https://drive.google.com/file/d/[FILE_ID]
└── Google Drive Folders: https://drive.google.com/drive/folders/[FOLDER_ID]

📊 PROCESSING STAGES:
1. Sheet Download → Raw HTML caching
2. HTML Parsing → People & document extraction
3. Document Scraping → Content & text extraction
4. Link Extraction → Media URL discovery
5. Data Processing → Record standardization
6. CSV Mapping → Structured output generation
7. S3 Streaming → Direct cloud storage (when needed)
8. Extension Fixing → Proper media types (when needed)
9. Metadata Processing → Missing media recovery (when needed)

💾 OUTPUT DESTINATIONS:
├── outputs/output.csv → Main structured dataset
├── s3://typing-clients-uuid-system/files/[UUID] → Media files
├── data/sheet.html → Cached Google Sheet
├── data/extraction_progress.json → Progress tracking
├── logs/runs/[TIMESTAMP]/ → Execution logs
└── Various backup & versioning files

🔗 KEY INTEGRATIONS:
├── Selenium WebDriver → Google Docs text extraction
├── BeautifulSoup → HTML parsing & link extraction
├── pandas → CSV data manipulation
├── boto3 → S3 cloud storage operations
├── requests → HTTP communications
├── yt-dlp → YouTube content downloading
└── Google Drive API → Drive file operations

⚡ PERFORMANCE OPTIMIZATIONS:
├── HTTP connection pooling → Reduced latency
├── Direct streaming → Zero local storage
├── Atomic file operations → Data integrity
├── Progress state management → Resumable operations
├── Retry logic with backoff → Reliability
├── Meaningful link filtering → Noise reduction
└── UUID-based file organization → Scalable storage

🛡️ SECURITY & RELIABILITY:
├── File locking → Concurrent access protection
├── CSV versioning → Data loss prevention
├── Error context preservation → Debugging support
├── Sanitization → Data integrity
├── UUID file names → Non-enumerable security
└── Backup strategies → Recovery capabilities
```

---

## 🎯 EXECUTION MODES

### **Basic Mode** (`--basic`)
- **Purpose**: Extract only essential person data
- **Files Called**: `simple_workflow.py` → `utils/csv_manager.py`
- **Output**: 5-column CSV (row_id, name, email, type, link)
- **Performance**: Fastest execution, no document processing

### **Text Mode** (`--text`)
- **Purpose**: Extract document text for analysis
- **Files Called**: All basic + `utils/extract_links.py` + Selenium
- **Output**: Basic columns + document_text + processing_info
- **Performance**: Medium speed, batch processing with progress tracking

### **Full Mode** (default)
- **Purpose**: Complete pipeline with media link extraction
- **Files Called**: All components in the workflow
- **Output**: Complete 23-column CSV with S3 UUID mappings
- **Performance**: Comprehensive processing, all features enabled

---

## 🔄 STREAMING PIPELINE ACTIVATION

The streaming components activate when:

1. **Missing Media Detected**: `process_pending_metadata_downloads.py`
2. **Google Drive Folders**: `stream_folder_contents_direct.py`
3. **Extension Issues**: `fix_s3_extensions.py`

These operate independently but integrate with the main CSV via UUID mappings.

---

## 📋 FILE DEPENDENCY MATRIX

| Component | Dependencies | Purpose |
|-----------|-------------|---------|
| `simple_workflow.py` | config, csv_manager, extract_links, patterns, http_pool | Main 6-step pipeline |
| `utils/csv_manager.py` | file_lock, sanitization, config, row_context, error_handling | CSV operations & S3 integration |
| `utils/extract_links.py` | http_pool, config, logging_config, patterns, error_handling | Document scraping & link extraction |
| `utils/s3_manager.py` | boto3, config, logging_config | S3 streaming operations |
| `utils/patterns.py` | selenium, logging_config | Regex patterns & WebDriver management |
| `core/stream_folder_contents_direct.py` | s3_manager, download_drive, logging_config | Direct Drive→S3 streaming |
| `core/process_pending_metadata_downloads.py` | s3_manager, downloader, csv_manager, row_context | Metadata processing |
| `core/fix_s3_extensions.py` | boto3, pandas | Extension & content type correction |

---

## 🚀 EXECUTION EXAMPLES

### **Standard Full Pipeline**
```bash
python simple_workflow.py
# Calls: All 6 steps + full CSV generation
# Output: outputs/output.csv with complete data
```

### **Basic Data Extraction Only**
```bash
python simple_workflow.py --basic
# Calls: Steps 1-2 + basic CSV creation
# Output: 5-column CSV with person data only
```

### **Text Extraction with Progress Tracking**
```bash
python simple_workflow.py --text --batch-size 5 --resume
# Calls: All steps + text extraction + progress management
# Output: CSV with document text + progress files
```

### **Stream Missing Media Files**
```bash
python core/process_pending_metadata_downloads.py
# Calls: S3 streaming pipeline for metadata gaps
# Output: Media files in S3 + updated CSV mappings
```

### **Fix File Extensions**
```bash
python core/fix_s3_extensions.py
# Calls: S3 extension correction pipeline
# Output: Properly typed media files in S3
```

---

## 🎯 CRITICAL SUCCESS PATH

For the complete pipeline to succeed, this exact execution order is required:

1. **Configuration Loading** (`utils/config.py::get_config()`)
2. **Sheet Download** (`step1_download_sheet()`)
3. **Data Extraction** (`step2_extract_people_and_docs()`)
4. **Document Processing** (`step3_scrape_doc_contents()` + `step4_extract_links()`)
5. **Record Creation** (`step5_process_extracted_data()`)
6. **CSV Generation** (`step6_map_data()`)
7. **S3 Integration** (streaming components as needed)

Any interruption in this chain will result in incomplete data processing, but the modular design allows for resumable operations through progress tracking.

---

## 🔧 MAINTENANCE & DEBUGGING

### **Key Log Locations**
- `logs/runs/[TIMESTAMP]/main.log` → Primary execution log
- `logs/runs/[TIMESTAMP]/errors.log` → Error details
- `data/extraction_progress.json` → Text extraction progress
- S3 upload reports → Streaming operation results

### **Common Debug Points**
- **Configuration Issues**: Check `config/config.yaml` loading
- **Google Sheets Access**: Verify URL and permissions
- **Selenium Failures**: Check WebDriver setup and cleanup
- **S3 Access**: Validate AWS credentials and bucket permissions
- **CSV Corruption**: Review file locking and backup strategies

This complete execution map provides the foundation for understanding, debugging, and maintaining the entire typing clients ingestion pipeline.