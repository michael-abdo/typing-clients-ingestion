# CORE WORKFLOW - UNIFIED EDITION

## The Simple 6-Step Process:

1. **Download a local copy** of the Google Sheet
2. **Extract the Google Doc link** from the "name" column (if the link exists)
3. **Scrape the contents** of that Google Doc (if we have access to it)
4. **Extract links** from the scraped contents (if they exist)
5. **Download the links** (YouTube videos, Drive folders, Drive files)
6. **Correctly map** the downloaded content to the Name/Email/Type columns so that the data isn't lost

## CRITICAL PRINCIPLE:
This is a SIMPLE workflow. Everything is consolidated into ONE script with multiple modes. No duplicate files, no temporary scripts, complete DRY implementation.

## 🔥 UNIFIED ARCHITECTURE
- **Single Script**: `simple_workflow.py` handles all modes
- **SQLite Database**: Optional database mode for enhanced features
- **Incremental Processing**: Skip already processed documents
- **Progress Tracking**: Enhanced progress tracking with visual feedback
- **Validation Built-in**: Integrated testing and validation
- **Error Handling**: Comprehensive retry logic and graceful degradation

## Unified Script Usage:

### Basic Mode (Fast - Just 5 columns):
```bash
python3 simple_workflow.py --basic
```
Extracts: `row_id, name, email, type, link` for all 496 people (~2 seconds)

### Text Extraction Mode (Basic + Document Text):
```bash
python3 simple_workflow.py --text
```
Extracts: `row_id, name, email, type, link, document_text, processed, extraction_date` with batch processing (~30-60 minutes)

### Full Mode (Complete processing):
```bash
python3 simple_workflow.py
```
Extracts all data with document processing and link extraction (~1+ hours)

### Database Mode (Enhanced Features):
```bash
# Enable database mode with all features
python3 simple_workflow.py --db --create-tables

# Database mode with custom path
python3 simple_workflow.py --db --db-path my_data.db

# Skip already processed documents
python3 simple_workflow.py --text --skip-processed

# Reprocess all documents
python3 simple_workflow.py --text --reprocess-all

# Retry previously failed extractions
python3 simple_workflow.py --text --retry-failed
```

### Testing and Validation:
```bash
# Run built-in validation tests
python3 simple_workflow.py --test

# Validate against known test cases
python3 simple_workflow.py --validate

# Find specific person by name
python3 simple_workflow.py --find-person "James Kirton"

# Process specific row range
python3 simple_workflow.py --start-row 499 --end-row 499
```

### Batch and Testing Options:
```bash
# Test with limited documents
python3 simple_workflow.py --text --test-limit 5

# Custom batch size
python3 simple_workflow.py --text --batch-size 20

# Custom output file
python3 simple_workflow.py --text --output my_text_data.csv
```

## Database Migration

### 🔄 Migrating from CSV to Database:
```bash
# Migrate existing CSV data to SQLite database
python3 csv_to_sqlite_migration.py --create-tables --test

# Custom database name
python3 csv_to_sqlite_migration.py --database my_data.db --create-tables
```

### 📊 Database Schema:
- **people**: Core person data (row_id, name, email, type)
- **documents**: Document URLs and extracted text
- **extracted_links**: All links found in documents
- **downloads**: Download tracking and status
- **processing_log**: Complete audit trail

### 🏗️ Architecture (DRY Principles Applied):
- **simple_workflow.py**: Single unified script with all modes
- **extraction_utils.py**: Reusable extraction functions with Google Docs improvements
- **database_manager.py**: All database operations centralized
- **utils/config.py**: Centralized configuration management with shared utilities:
  - `parse_file_size_from_html()`: Consolidates file size parsing from Google Drive HTML
  - `setup_csv_environment()`: Standardized CSV field size limit setup
  - `get_standard_components()`: Unified logger and config initialization
- **Person/Document/Link**: Data models for type safety

## Success Criteria:
- Name/Email/Type data is preserved and correctly mapped to downloaded content
- No data loss during the process
- Clean, minimal implementation focusing only on these 6 steps
- DRY principle applied - modular, reusable components
- Database provides scalability and query performance
- Incremental processing handles growing data efficiently