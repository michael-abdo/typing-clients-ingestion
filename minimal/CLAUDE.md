# CORE WORKFLOW - DATABASE EDITION

## The Simple 6-Step Process:

1. **Download a local copy** of the Google Sheet:
   `https://docs.google.com/spreadsheets/u/1/d/e/2PACX-1vRqqjqoaj8sEZBfZRw0Og7g8ms_0yTL2MsegTubcjhhBnXr1s1jFBwIVAsbkyj1xD0TMj06LvGTQIHU/pubhtml?pli=1#`

2. **Extract the Google Doc link** from the "name" column (if the link exists)

3. **Scrape the contents** of that Google Doc (if we have access to it)

4. **Extract links** from the scraped contents (if they exist)

5. **Download the links** (YouTube videos, Drive folders, Drive files)

6. **Correctly map** the downloaded content to the Name/Email/Type columns so that the data isn't lost

## CRITICAL PRINCIPLE:
This is a SIMPLE workflow. We overcomplicated it. Strip everything down to JUST these core minimal parts. Add complexity back only as absolutely necessary.

## 🔥 NEW: DATABASE SUPPORT
- **SQLite Database**: All data is stored in a normalized SQLite database (`xenodex.db`)
- **Incremental Processing**: Skip already processed documents
- **Progress Tracking**: Full audit trail in `processing_log` table  
- **Retry Failed**: Easily retry documents that failed extraction
- **Export Options**: Export to CSV, database, or both
- **DRY Architecture**: Modular code with `DatabaseManager`, `extraction_utils`

## Usage Options:

### 📁 Legacy CSV Script (simple_workflow.py)
Original script - CSV-based processing, still available

### 🔥 Database Script (simple_workflow_db.py) - RECOMMENDED
New database-powered version with enhanced features

## Database Script Usage:

### Basic Mode (Fast - Just 5 columns):
```bash
python3 simple_workflow_db.py --basic
```
Extracts: `row_id, name, email, type, link` for all 496 people (~2 seconds)

### Text Extraction Mode (Basic + Document Text):
```bash
python3 simple_workflow_db.py --text
```
Extracts: `row_id, name, email, type, link, document_text, processed, extraction_date` with batch processing (~30-60 minutes)

**Enhanced Database Features:**
- **Incremental Processing**: Automatically skips already processed documents
- **Transaction Safety**: All operations are atomic with proper rollback
- **Retry Logic**: Built-in retry for failed extractions with progress tracking
- **Audit Trail**: Complete processing history in `processing_log` table
- **Export Flexibility**: Choose CSV, database, or both outputs

### Full Mode (Complete processing):
```bash
python3 simple_workflow_db.py
```
Extracts all data with document processing and link extraction (~1+ hours)

### Database-Specific Options:
```bash
# Use database (default)
python3 simple_workflow_db.py --basic --db

# CSV-only mode (disable database)
python3 simple_workflow_db.py --basic --no-db

# Custom database file
python3 simple_workflow_db.py --basic --db-path my_data.db

# Output format options
python3 simple_workflow_db.py --basic --output-format csv
python3 simple_workflow_db.py --basic --output-format db  
python3 simple_workflow_db.py --basic --output-format both

# Skip already processed documents (default)
python3 simple_workflow_db.py --text --skip-processed

# Reprocess all documents
python3 simple_workflow_db.py --text --reprocess-all

# Retry previously failed extractions
python3 simple_workflow_db.py --text --retry-failed
```

### Testing and Batch Options:
```bash
# Test with limited documents
python3 simple_workflow_db.py --text --test-limit 5

# Custom batch size
python3 simple_workflow_db.py --text --batch-size 20

# Custom output file
python3 simple_workflow_db.py --text --output my_text_data.csv
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
- **database_manager.py**: All database operations centralized
- **extraction_utils.py**: Reusable extraction functions
- **simple_workflow_db.py**: Main workflow orchestration
- **Person/Document/Link**: Data models for type safety

## Success Criteria:
- Name/Email/Type data is preserved and correctly mapped to downloaded content
- No data loss during the process
- Clean, minimal implementation focusing only on these 6 steps
- DRY principle applied - modular, reusable components
- Database provides scalability and query performance
- Incremental processing handles growing data efficiently