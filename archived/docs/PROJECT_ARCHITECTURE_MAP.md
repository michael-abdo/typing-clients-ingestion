# Project Architecture Map - Typing Clients Ingestion Minimal

## 📊 **File Count Summary**
- **Total Files**: 395 files
- **Python Files**: 82 files  
- **Core Workflow Files**: 6 essential files
- **Utility Modules**: 39 modules
- **Test Files**: 18 test files

## 🏗️ **ASCII Architecture Diagram**

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                    TYPING CLIENTS INGESTION WORKFLOW                           │
│                           (6-Step Process)                                     │
└─────────────────────────────────────────────────────────────────────────────────┘

                               ┌─────────────────┐
                               │  sheet.html     │
                               │   (Raw Data)    │
                               └─────────┬───────┘
                                         │
                                         v
┌─────────────────────────────────────────────────────────────────────────────────┐
│                          STEP 1: DOWNLOAD SHEET                                │
│                                                                                 │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐            │
│  │ simple_workflow │◄──►│ scrape_google_  │◄──►│ extract_links   │            │
│  │     .py         │    │   sheets.py     │    │     .py         │            │
│  │   (Main Entry)  │    │ (Sheet Scraper) │    │ (Link Extractor)│            │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘            │
└─────────────────────────────────────────────────────────────────────────────────┘
                                         │
                                         v
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     STEP 2-3: EXTRACT & SCRAPE DOCS                           │
│                                                                                 │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐            │
│  │   patterns.py   │◄──►│ validation.py   │◄──►│ sanitization.py │            │
│  │ (URL Patterns)  │    │ (URL Validation)│    │ (Name Cleaning) │            │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘            │
└─────────────────────────────────────────────────────────────────────────────────┘
                                         │
                                         v
┌─────────────────────────────────────────────────────────────────────────────────┐
│                     STEP 4-5: DOWNLOAD CONTENT                                 │
│                                                                                 │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐            │
│  │download_all_min │◄──►│download_youtube │◄──►│ download_drive  │            │
│  │   imal.py       │    │     .py         │    │     .py         │            │
│  │ (Main Downloader│    │ (YouTube DL)    │    │ (Drive DL)      │            │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘            │
└─────────────────────────────────────────────────────────────────────────────────┘
                                         │
                                         v
┌─────────────────────────────────────────────────────────────────────────────────┐
│                        STEP 6: MAP TO CSV                                      │
│                                                                                 │
│  ┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐            │
│  │ csv_manager.py  │◄──►│comprehensive_   │◄──►│  output.csv     │            │
│  │ (CSV Operations)│    │file_mapper.py   │    │ (Final Output)  │            │
│  │                 │    │ (File Mapping)  │    │                 │            │
│  └─────────────────┘    └─────────────────┘    └─────────────────┘            │
└─────────────────────────────────────────────────────────────────────────────────┘

                    ┌─────────────────────────────────────────┐
                    │             SUPPORT LAYER                │
                    │                                         │
                    │  ┌─────────────┐  ┌─────────────┐      │
                    │  │ config.py   │  │logging_config│      │
                    │  │(Config Mgmt)│  │   .py       │      │
                    │  └─────────────┘  └─────────────┘      │
                    │                                         │
                    │  ┌─────────────┐  ┌─────────────┐      │
                    │  │error_handling│  │ retry_utils │      │
                    │  │   .py       │  │   .py       │      │
                    │  └─────────────┘  └─────────────┘      │
                    │                                         │
                    │  ┌─────────────┐  ┌─────────────┐      │
                    │  │path_setup.py│  │http_pool.py │      │
                    │  │(Path Utils) │  │(HTTP Client)│      │
                    │  └─────────────┘  └─────────────┘      │
                    └─────────────────────────────────────────┘
```

## 🔧 **Core Workflow Files (6 files)**

### **Primary Entry Point**
- **`simple_workflow.py`** - Main orchestrator for the 6-step process
  - Coordinates all steps
  - Handles CLI arguments
  - Manages workflow state

### **Step Controllers**
- **`download_all_minimal.py`** - Unified download manager
  - Downloads YouTube content
  - Saves Google Drive file info
  - Generates download reports

### **Data Processors**
- **`sheet.html`** - Raw Google Sheet data (HTML format)
- **`outputs/output.csv`** - Final processed CSV output
- **`config/config.yaml`** - Configuration settings

### **Verification Tools**
- **`check_downloads.py`** - Validates downloaded content

## 🛠️ **Utility Modules (39 modules)**

### **Core Processing**
```
patterns.py          ◄─► URL pattern matching & extraction
validation.py        ◄─► URL validation & type detection  
sanitization.py      ◄─► Filename & data sanitization
extract_links.py     ◄─► Google Doc link extraction
csv_manager.py       ◄─► CSV operations & management
```

### **Download & Network**
```
download_youtube.py  ◄─► YouTube-specific downloads
download_drive.py    ◄─► Google Drive downloads
http_pool.py         ◄─► HTTP connection management
rate_limiter.py      ◄─► Request rate limiting
retry_utils.py       ◄─► Retry logic with backoff
```

### **File Management**
```
comprehensive_file_mapper.py ◄─► File discovery & mapping
path_setup.py              ◄─► Path utilities & setup
file_lock.py               ◄─► File locking mechanisms
atomic_csv.py              ◄─► Atomic CSV operations
```

### **Infrastructure**
```
config.py           ◄─► Configuration management
logging_config.py   ◄─► Centralized logging
error_handling.py   ◄─► Error management & decorators
monitoring.py       ◄─► System monitoring & metrics
```

## 📋 **Test Files (18 files)**

### **Validation Tests**
```
validate_dry_refactoring.py  ◄─► DRY refactoring validation
check_circular_deps.py       ◄─► Circular dependency checker
test_validation_*.py         ◄─► Various validation tests
```

### **Component Tests**
```
test_selenium.py            ◄─► Selenium functionality tests
test_all_*.py               ◄─► End-to-end workflow tests
test_e2e_simple_workflow.py ◄─► Complete workflow testing
```

### **Unit Tests**
```
tests/test_*.py             ◄─► Individual component tests
test_new_functions.py       ◄─► New function validation
test_patterns.py            ◄─► Pattern matching tests
```

## 🔄 **Data Flow Connections**

### **Input Flow**
```
sheet.html → simple_workflow.py → extract_links.py → patterns.py → validation.py
```

### **Processing Flow**
```
validation.py → sanitization.py → download_all_minimal.py → download_*.py
```

### **Output Flow**
```
download_*.py → csv_manager.py → comprehensive_file_mapper.py → output.csv
```

### **Support Flow**
```
config.py → logging_config.py → error_handling.py → monitoring.py
```

## 🎯 **DRY Refactoring Connections**

### **Consolidated Functions**
- **`sanitize_filename()`** - Used by download_all_minimal.py
- **`extract_youtube_id()`** - Used by download_all_minimal.py
- **`extract_drive_id()`** - Used by download_all_minimal.py
- **`safe_csv_read()`** - Used by download_all_minimal.py
- **`ensure_directory()`** - Used by download_all_minimal.py
- **`setup_project_path()`** - Used by test_selenium.py

### **Shared Utilities**
- **PatternRegistry** - Centralized regex patterns
- **ValidationError** - Standardized error handling
- **get_logger()** - Unified logging setup
- **get_config()** - Configuration access

## 📈 **File Relationship Summary**

- **Core**: 6 essential files drive the main workflow
- **Utils**: 39 utility modules provide reusable functionality
- **Tests**: 18 test files ensure code quality and validation
- **Data**: 332 supporting files (logs, downloads, configs, documentation)

**Total Architecture**: 395 files working together in a DRY, maintainable structure with clear separation of concerns and centralized utilities.