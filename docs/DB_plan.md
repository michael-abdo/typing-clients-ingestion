# Database Migration Plan

A comprehensive analysis and plan for migrating from CSV-based asset management to database with cloud storage integration.

## 1. Codebase Scan - Current Architecture

### **Current Asset/File Management Approach**

**File Storage and Organization:**
- **Download Directories**: 
  - `youtube_downloads/` - YouTube videos and transcripts
  - `drive_downloads/` - Google Drive files and folders
  - Files stored with configurable naming schemes from `config/config.yaml`
  
- **File Naming Conventions**:
  - YouTube: `{video_id}.{format}` and `{video_id}_transcript.{format}`
  - Drive: `{file_id}.{extension}` or original filename when available
  - Context-aware naming: `row_{row_id}_{original_name}` for traceability

**Asset-to-People Mapping Strategy:**
The system uses a sophisticated **Row-Centric Design** that maintains perfect traceability:

```python
@dataclass
class RowContext:
    row_id: str          # Primary key from CSV
    row_index: int       # Position in CSV for atomic updates  
    type: str           # Personality type - CRITICAL to preserve
    name: str           # Person name for human-readable tracking
    email: str          # Additional identifier
```

### **Data Flow and Storage Patterns**

**Google Sheets → Processing → Storage Flow:**
1. **Extract**: `simple_workflow.py` scrapes Google Sheets using BeautifulSoup
2. **Process**: Extracts people data with core fields (row_id, name, email, type, link)
3. **Download**: Context-aware downloads maintain row relationships
4. **Store**: Files saved with embedded metadata linking back to source rows

**CSV Structure Evolution:**
The system automatically enhances CSV with 8 tracking columns:
```
Core: row_id, name, email, type, link
Enhanced: youtube_status, youtube_files, youtube_media_id, 
         drive_status, drive_files, drive_media_id,
         last_download_attempt, download_errors
```

## 2. Components Affected by Database Migration

**🔴 HIGH IMPACT (Core Changes Required):**
- `simple_workflow.py` - Step 6 mapping function
- `utils/csv_manager.py` - Complete replacement with DB operations  
- `utils/atomic_csv.py` - Transaction logic moves to DB
- `utils/streaming_csv.py` - Batch operations become DB queries
- `utils/csv_tracker.py` - Progress tracking in DB tables
- `utils/row_context.py` - RowContext becomes DB entity

**🟡 MEDIUM IMPACT (Interface Changes):**
- `utils/download_youtube.py` - Asset records go to DB
- `utils/download_drive.py` - Asset records go to DB  
- `run_complete_workflow.py` - DB initialization calls
- All `scripts/` download scripts - DB connections needed

**🟢 LOW IMPACT (Configuration Only):**
- `config/config.yaml` - Add DB connection settings
- File locking utilities - Less critical with DB transactions

## 3. Project Structure Plan

```
database/
├── schema/
│   ├── init.sql              # Database schema creation
│   ├── migrations/           # Version-controlled schema changes
│   └── indexes.sql           # Performance indexes
├── models/
│   ├── person.py            # Person entity (from RowContext)
│   ├── asset.py             # Downloaded asset entity  
│   ├── download_session.py  # Batch download tracking
│   └── base.py              # Base model with common fields
├── operations/
│   ├── db_manager.py        # Replaces csv_manager.py
│   ├── person_ops.py        # Person CRUD operations
│   ├── asset_ops.py         # Asset CRUD operations
│   └── migration_ops.py     # CSV→DB migration utilities
├── cloud_storage/
│   ├── storage_manager.py   # S3/GCS integration
│   ├── upload_manager.py    # Asset upload coordination
│   └── url_manager.py       # Cloud URL generation/tracking
└── config/
    └── database.yaml        # DB connection settings
```

## 4. Database Schema

**Core Goal:** Migrate CSV-based asset management to database while preserving step 6 Name/Email/Type→content mapping and enabling cloud storage.

```sql
-- Preserve current RowContext as Person entity
CREATE TABLE people (
    row_id VARCHAR PRIMARY KEY,    -- From CSV row_id
    name VARCHAR NOT NULL,         -- Step 6: preserve name mapping
    email VARCHAR,                 -- Step 6: preserve email mapping  
    personality_type VARCHAR,      -- Step 6: preserve type mapping (CRITICAL)
    source_link VARCHAR,           -- Google Doc URL
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Track all downloaded assets with cloud storage URLs
CREATE TABLE assets (
    asset_id UUID PRIMARY KEY,
    person_row_id VARCHAR REFERENCES people(row_id), -- Preserve CSV relationship
    asset_type VARCHAR,            -- 'youtube_video', 'youtube_transcript', 'drive_file'
    original_url VARCHAR,          -- Source URL
    local_file_path VARCHAR,       -- Current local storage
    cloud_storage_url VARCHAR,     -- S3/GCS URL after upload
    media_id VARCHAR,              -- YouTube video_id or Drive file_id
    filename VARCHAR,
    file_size_bytes BIGINT,
    download_status VARCHAR,       -- 'pending', 'downloaded', 'uploaded', 'failed'
    metadata JSONB,                -- RowContext + download metadata
    created_at TIMESTAMP,
    updated_at TIMESTAMP
);

-- Track download sessions for batch operations  
CREATE TABLE download_sessions (
    session_id UUID PRIMARY KEY,
    session_type VARCHAR,          -- 'youtube', 'drive', 'mixed'
    total_assets INTEGER,
    completed_assets INTEGER,
    failed_assets INTEGER,
    status VARCHAR,
    created_at TIMESTAMP,
    completed_at TIMESTAMP
);
```

## 5. Migration Strategy

**🎯 Migration Strategy: Gradual replacement**
1. **Database setup** - Create schema alongside existing CSV
2. **Dual-write phase** - Write to both CSV and DB during transition  
3. **Read migration** - Switch reads from CSV to DB
4. **CSV deprecation** - Remove CSV dependencies
5. **Cloud integration** - Add cloud storage URLs to existing DB

## 6. Implementation Phases

**Phase 1: Database Foundation (Week 1)**
1. Choose database (PostgreSQL recommended for JSONB support)
2. Create schema in `database/schema/init.sql`
3. Build `database/models/` with SQLAlchemy/similar ORM
4. Create `database/operations/db_manager.py` (replaces csv_manager.py)
5. Add database config to `config/database.yaml`

**Phase 2: Dual-Write Migration (Week 2)** 
1. Modify `simple_workflow.py` step6 to write both CSV and DB
2. Update `utils/download_youtube.py` and `utils/download_drive.py` for dual writes
3. Create migration script in `database/operations/migration_ops.py`
4. Migrate existing CSV data to database
5. Verify data integrity between CSV and DB

**Phase 3: Read Migration (Week 3)**
1. Switch `simple_workflow.py` to read from DB instead of CSV
2. Update all download scripts to query DB for people/assets
3. Create `database/operations/person_ops.py` and `asset_ops.py` 
4. Test all three workflow modes (--basic, --text, full) with DB backend
5. Performance testing and optimization

**Phase 4: Cloud Storage Integration (Week 4)**
1. Build `database/cloud_storage/storage_manager.py` for S3/GCS
2. Add `cloud_storage_url` tracking to assets table
3. Create upload workflows in `database/cloud_storage/upload_manager.py`
4. Implement cloud URL generation in `database/cloud_storage/url_manager.py`
5. Test end-to-end: download→upload→cloud URL tracking

**Phase 5: CSV Deprecation (Week 5)**
1. Remove CSV write operations from workflow
2. Archive/backup existing CSV files  
3. Remove CSV dependencies from download utilities
4. Update documentation and README
5. Performance validation and monitoring setup

## Success Criteria
- ✅ **Step 6 preservation**: Name/Email/Type→downloaded content mapping intact
- ✅ **Cloud storage**: All assets tracked with cloud URLs
- ✅ **Performance**: DB operations faster than CSV file operations
- ✅ **Data integrity**: Zero data loss during migration
- ✅ **Rollback capability**: Can revert to CSV if needed