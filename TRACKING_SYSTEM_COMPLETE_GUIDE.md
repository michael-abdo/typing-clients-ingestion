# Complete Guide: How Tracking Works in the Typing Clients System

**Date**: August 09, 2025  
**Purpose**: Comprehensive explanation of all tracking mechanisms  

## Executive Summary

The system tracks client data processing through a combination of:
- **JSON files** for workflow progress (which documents have been processed)
- **CSV files** for client data and download status
- **S3 versioning** for historical backups
- **No active database** (schema exists but unused)

## Quick Answer: Does it detect previously processed clients?

**No**, the system does NOT prevent processing the same client multiple times. Instead:
- It tracks which **documents** have been extracted (not which clients)
- Every run creates a fresh CSV with all clients from the Google Sheet
- The `--resume` flag only skips document extraction, not client processing

## Detailed Tracking Mechanisms

### 1. JSON Progress Files

#### extraction_progress.json
```json
{
  "completed": ["https://docs.google.com/document/d/12vUN..."],
  "failed": [],
  "last_batch": 10,
  "total_processed": 1
}
```
- **Purpose**: Track expensive document extractions
- **Scope**: Document URLs only (not clients)
- **Used by**: `--resume` flag to skip completed documents

#### failed_extractions.json
```json
[]
```
- **Purpose**: List of failed document URLs for retry
- **Used by**: `--retry-failed` flag

### 2. CSV Data Tracking

#### outputs/output.csv
```csv
row_id,name,email,type,link
515,Holiday Solly,Holidaysolly@protonmail.com,FF-Fi/Se-CP/B(S) #1,https://docs.google.com/...
514,Alex Mueller,alex.j.mueller@pm.me,FF-Te/Se-PC/S(B) #3,
```

**Key tracking columns** (in full mode):
- `processed`: yes/no
- `youtube_status`: pending/completed/failed/streamed
- `drive_status`: pending/completed/failed/streamed  
- `file_uuids`: JSON mapping descriptions to UUIDs
- `s3_paths`: JSON mapping UUIDs to S3 locations
- `download_errors`: Error messages
- `permanent_failure`: Permanent failure flag

**Important**: This file is **completely overwritten** on each run, not appended to.

### 3. S3 Backup System

- **Automatic**: Every CSV write triggers S3 upload
- **Location**: `typing-clients-uuid-system` bucket
- **Path**: `csv-versions/2025/08/output_20250809_143022.csv`
- **Purpose**: Historical record and recovery

### 4. File Locking

- **output.csv.lock**: Prevents concurrent access
- **Timeout**: 30 seconds default
- **Purpose**: Data integrity during updates

## How Different Scenarios Work

### Scenario 1: Fresh Run (no flags)
```bash
python simple_workflow.py
```
1. Downloads Google Sheet (all 510 people)
2. Ignores any previous progress files
3. Extracts text from ALL documents
4. Creates brand new output.csv with all 510 rows
5. Updates extraction_progress.json

### Scenario 2: Resume Run
```bash
python simple_workflow.py --resume
```
1. Downloads Google Sheet (all 510 people)
2. Loads extraction_progress.json
3. Skips documents listed in 'completed'
4. Extracts only new/remaining documents
5. **Still creates new output.csv with all 510 rows**

### Scenario 3: Client Updates in Google Sheet
If Holiday Solly's email changes in the Sheet:
1. Fresh run: New email appears in output.csv
2. Resume run: New email appears (client data always refreshed)
3. Document re-extracted only if URL changed

### Scenario 4: Duplicate Clients
If Google Sheet has two "Holiday Solly" entries:
- Both appear in output.csv with different row_ids
- System preserves Sheet structure exactly
- No deduplication performed

## Why It Works This Way

### Design Principles

1. **Google Sheet is Truth**
   - Always reflects latest client list
   - System mirrors Sheet structure exactly
   - No local deduplication or filtering

2. **Documents are Expensive**
   - API rate limits on Google Docs
   - Network bandwidth for large files
   - Progress tracking prevents re-extraction

3. **CSV is Disposable**
   - Can regenerate from Sheet + documents
   - Overwrites ensure consistency
   - S3 versioning preserves history

4. **Simplicity Over Complexity**
   - Full overwrites vs complex merging
   - Document-level vs client-level tracking
   - File-based vs database complexity

## Common Misconceptions

### "It tracks which clients we've processed"
**Reality**: It tracks which documents were extracted, not which clients were processed. Every run processes all clients from the Sheet.

### "Resume means it won't duplicate clients"  
**Reality**: Resume only skips document extraction. All clients still appear in the new CSV.

### "The CSV appends new clients"
**Reality**: The CSV is completely overwritten each run with all current Sheet data.

### "It uses a database for tracking"
**Reality**: All tracking is file-based (JSON + CSV). Database schema exists but isn't used.

## Architecture Diagram

```
Google Sheet (Source of Truth)
     ↓
simple_workflow.py
     ├── extraction_progress.json (document URLs)
     ├── output.csv (complete overwrite)
     └── S3 versioning (automatic backup)
           
Download Scripts
     ├── Read output.csv rows
     ├── Update status columns
     └── Stream files to S3 with UUIDs
```

## Best Practices

1. **For fresh data**: Run without flags
2. **For interrupted runs**: Use `--resume`
3. **For failed documents**: Use `--retry-failed`
4. **For history**: Check S3 csv-versions
5. **For concurrency**: System handles via file locks

## Summary

The tracking system is optimized for:
- **Reliability**: Resume from interruptions
- **Efficiency**: Avoid redundant document extraction  
- **Simplicity**: Straightforward overwrite model
- **Auditability**: Complete history in S3

It is NOT designed for:
- Client-level deduplication
- Incremental updates
- Complex merge scenarios
- Real-time processing

Understanding this helps explain why "Processing person 8/510" appears even on subsequent runs - the system always processes all clients, but may skip their document extraction if previously completed.