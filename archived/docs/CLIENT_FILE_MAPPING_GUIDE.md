# Client-File Mapping Guide

## Understanding the File Organization System

### Current Structure (No UUIDs)

Despite the bucket name "typing-clients-uuid-system", the current implementation **does not use UUIDs**. Instead, files are organized using a hierarchical structure based on client identifiers from the CSV.

## File Organization in S3

```
typing-clients-uuid-system/
├── {row_id}/                     # Client's row ID from CSV
│   └── {person_name}/           # Client's name (spaces replaced with underscores)
│       ├── youtube_{id}.mp4     # YouTube videos
│       ├── drive_file_{id}_{name} # Google Drive files
│       └── {platform}_metadata.json # Metadata files
└── csv-versions/                # CSV version history
    └── YYYY/MM/
        └── output_YYYY-MM-DD_HHMMSS.csv
```

## Key Identifiers

### Primary Client Identifier: `row_id`
- **What it is**: The unique row number from the CSV (column 1)
- **Example**: 502, 503, 504
- **Usage**: Primary folder in S3 path

### Secondary Identifier: Client Name
- **What it is**: Person's name from the CSV (column 2)
- **Example**: "Sam Torode", "Augusto Evangelista"
- **Usage**: Subfolder in S3 path (spaces converted to underscores)

### Client Type
- **What it is**: Personality type classification (column 4)
- **Example**: "FF-Fi/Se-CS/P(B) #4"
- **Usage**: Stored in CSV, viewable in mapping reports

## Mapping Tools

### 1. Generate Complete Mapping Report
```bash
python3 generate_client_file_mapping.py
```

This creates:
- **Summary report** (printed to console)
- **JSON report**: `client_file_mapping_YYYYMMDD_HHMMSS.json`
- **CSV report**: `client_file_mapping_YYYYMMDD_HHMMSS.csv`

### 2. Look Up Specific Client Files
```bash
# By row ID
python3 lookup_client_files.py 502

# By name (partial match supported)
python3 lookup_client_files.py "Sam"

# List all clients with files
python3 lookup_client_files.py --list
```

### 3. View Mapping Reports

#### CSV Report Format
| row_id | name | type | email | filename | s3_key | file_type | size_mb | last_modified |
|--------|------|------|-------|----------|--------|-----------|---------|---------------|
| 502 | Sam Torode | FF-Fi/Se-CS/P(B) #4 | sam.torode@gmail.com | youtube_7cufMri1c5o.mp4 | 502/Sam_Torode/youtube_7cufMri1c5o.mp4 | YouTube | 135.66 | 2025-07-21T06:31:52+00:00 |

## File Naming Conventions

### YouTube Files
- **Pattern**: `youtube_{video_id}.{ext}`
- **Example**: `youtube_7cufMri1c5o.mp4`
- **Video ID**: The 11-character YouTube video identifier

### Google Drive Files
- **Pattern**: `drive_file_{file_id}_{original_name}`
- **Example**: `drive_file_1q2w3e4r5t6y_document.pdf`
- **File ID**: Google Drive's file identifier

## Understanding File Mappings

### From File to Client
Given a file in S3, you can determine the client by:
1. **S3 Path**: `502/Sam_Torode/youtube_7cufMri1c5o.mp4`
   - Row ID: 502
   - Client Name: Sam Torode

### From Client to Files
Given a client, you can find their files by:
1. **Row ID**: Look in S3 under `{row_id}/` prefix
2. **CSV Lookup**: Check `youtube_files` and `drive_files` columns
3. **Use Tools**: Run `lookup_client_files.py {row_id}`

## Example Queries

### "Which client owns this YouTube video?"
```bash
# If you have: youtube_7cufMri1c5o.mp4
# The S3 path shows: 502/Sam_Torode/youtube_7cufMri1c5o.mp4
# Answer: Row ID 502, Sam Torode
```

### "What type is the client who owns these files?"
```bash
# Look up by row ID
python3 lookup_client_files.py 502
# Output shows: Type: FF-Fi/Se-CS/P(B) #4
```

### "Show all files for personality type X"
```bash
# Generate full mapping
python3 generate_client_file_mapping.py
# Check the JSON or CSV report, filter by type
```

## Database Integration

The system includes a database schema with proper UUID support, but it's not yet implemented. The current system relies on:
- CSV file as the source of truth
- S3 path structure for file organization
- Row ID as the primary identifier

## Future UUID Implementation

The bucket name suggests a planned UUID system that would:
- Assign unique UUIDs to each file
- Create a mapping table: UUID ↔ Client ↔ File
- Enable more robust file tracking

Currently, the "UUID" in the bucket name is aspirational - the actual implementation uses row IDs.

## Quick Reference

| Need to Know | Use This |
|--------------|----------|
| All files for a client | `python3 lookup_client_files.py {row_id}` |
| Client type from file | Check S3 path for row_id, look up in CSV |
| All clients with files | `python3 lookup_client_files.py --list` |
| Complete mapping report | `python3 generate_client_file_mapping.py` |
| Client info from row_id | Check row in `outputs/output.csv` |

## Storage Statistics

Current storage usage (as of last run):
- **Total Clients with Files**: 4
- **Total Files**: 39
- **Total Storage**: ~16 GB
- **File Types**: Mostly YouTube MP4s, some Google Drive files

## Tips

1. **Row ID is Key**: The row_id is the most reliable identifier
2. **CSV is Truth**: The CSV file contains all client metadata
3. **S3 Path Pattern**: Always `{row_id}/{name}/{filename}`
4. **No Actual UUIDs**: Despite the bucket name, no UUID system exists yet