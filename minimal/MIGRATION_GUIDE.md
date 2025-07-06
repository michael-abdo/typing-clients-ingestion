# Migration Guide: CSV to Database Edition

## Overview
This guide helps you migrate from the CSV-based `simple_workflow.py` to the new database-powered `simple_workflow_db.py`.

## What Changed?

### 🔄 Architecture Changes
- **Old**: Single script with CSV files for data storage
- **New**: Modular architecture with SQLite database

### 📁 New Files Created
- `simple_workflow_db.py` - Main database-powered workflow
- `database_manager.py` - All database operations (DRY principle)
- `extraction_utils.py` - Reusable extraction functions (DRY principle)
- `database_schema_sqlite.sql` - Database schema definition
- `csv_to_sqlite_migration.py` - Migration script

### 🚀 Benefits of Upgrading
- **Performance**: Database queries vs CSV scanning
- **Incremental Processing**: Skip already processed documents
- **Transaction Safety**: Atomic operations with rollback
- **Audit Trail**: Complete processing history
- **Scalability**: Handle millions of records
- **Concurrent Access**: Multiple processes can read/write
- **Data Integrity**: Foreign keys, constraints, normalization

## Step-by-Step Migration

### 1. Backup Your Existing Data
```bash
# Backup your CSV files
cp simple_output.csv simple_output_backup.csv
```

### 2. Migrate CSV Data to Database
```bash
# Run the migration script
python3 csv_to_sqlite_migration.py --create-tables --test

# This creates xenodex.db with all your CSV data
```

### 3. Verify Migration
```bash
# Check database statistics
sqlite3 xenodx.db "SELECT COUNT(*) as people FROM people;"
sqlite3 xenodx.db "SELECT COUNT(*) as documents FROM documents;"
sqlite3 xenodx.db "SELECT COUNT(*) as links FROM extracted_links;"
```

### 4. Update Your Workflows

#### Old Commands → New Commands

**Basic Mode:**
```bash
# Old
python3 simple_workflow.py --basic

# New  
python3 simple_workflow_db.py --basic
```

**Text Mode:**
```bash
# Old
python3 simple_workflow.py --text --batch-size 10

# New
python3 simple_workflow_db.py --text --batch-size 10
```

**Full Mode:**
```bash
# Old
python3 simple_workflow.py

# New
python3 simple_workflow_db.py
```

### 5. New Capabilities Available

#### Incremental Processing
```bash
# Skip already processed documents (default)
python3 simple_workflow_db.py --text --skip-processed

# Or reprocess everything
python3 simple_workflow_db.py --text --reprocess-all
```

#### Retry Failed Extractions
```bash
# Retry only documents that failed previously
python3 simple_workflow_db.py --text --retry-failed
```

#### Custom Database
```bash
# Use a different database file
python3 simple_workflow_db.py --basic --db-path my_project.db
```

#### Output Format Control
```bash
# Database only
python3 simple_workflow_db.py --basic --output-format db

# CSV only (like old behavior)
python3 simple_workflow_db.py --basic --output-format csv

# Both (default)
python3 simple_workflow_db.py --basic --output-format both
```

## Comparison Table

| Feature | CSV Version | Database Version |
|---------|-------------|------------------|
| **Data Storage** | CSV files | SQLite database |
| **Processing** | Always reprocess all | Incremental processing |
| **Resume Support** | JSON progress files | Database transaction log |
| **Retry Failed** | Manual JSON editing | `--retry-failed` flag |
| **Performance** | Linear scan | Indexed queries |
| **Concurrent Access** | File locking issues | Database transactions |
| **Data Integrity** | No constraints | Foreign keys, constraints |
| **Query Capability** | Pandas/manual | SQL queries |
| **Scalability** | Memory limited | Database limited (much higher) |

## Compatibility

### Backward Compatibility
- CSV output format is **identical** to the original
- All CLI flags from old script are supported
- Original `simple_workflow.py` still works

### Forward Migration
- New database provides superset of CSV functionality
- Can export to CSV anytime for compatibility
- No data loss during migration

## Troubleshooting

### Migration Issues
```bash
# If migration fails, check:
ls -la simple_output.csv  # File exists?
head simple_output.csv    # Valid CSV format?

# Run with verbose output
python3 csv_to_sqlite_migration.py --create-tables --test
```

### Database Issues
```bash
# Check database file
ls -la xenodx.db

# Verify schema
sqlite3 xenodx.db ".schema people"

# Check data
sqlite3 xenodx.db "SELECT COUNT(*) FROM people;"
```

### Performance Issues
```bash
# If Selenium fails (disk space)
./emergency_cleanup.sh

# Use CSV-only mode as fallback
python3 simple_workflow_db.py --basic --no-db --output-format csv
```

## Rollback Plan

If you need to revert to CSV-only processing:

### Option 1: Use CSV-Only Mode
```bash
# Disable database, use CSV only
python3 simple_workflow_db.py --basic --no-db --output-format csv
```

### Option 2: Use Original Script
```bash
# Keep using the original script
python3 simple_workflow.py --basic
```

### Option 3: Export Database to CSV
```bash
# Export current database state to CSV
python3 simple_workflow_db.py --basic --output-format csv
```

## Next Steps

1. **Start with basic mode** to verify migration
2. **Use incremental processing** for efficiency
3. **Set up regular exports** if you need CSV compatibility
4. **Explore SQL queries** for advanced analysis
5. **Monitor disk space** for Selenium operations

## Getting Help

- Check the updated `CLAUDE.md` for latest documentation
- All original functionality is preserved
- Database provides additional capabilities without losing CSV compatibility