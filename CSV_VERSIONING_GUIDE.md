# CSV Versioning Guide

## Overview

Automatic CSV versioning is now integrated into the pipeline. Every time a CSV file is saved when S3 storage mode is enabled, a timestamped version is automatically uploaded to S3 for version history.

## How It Works

1. **Automatic Versioning**: When `storage_mode: "s3"` is set in config, all CSV writes automatically create a version in S3
2. **Timestamp-based Naming**: Files are named with pattern: `filename_YYYY-MM-DD_HHMMSS.csv`
3. **Organized Structure**: Versions are stored in S3 as: `csv-versions/YYYY/MM/filename_YYYY-MM-DD_HHMMSS.csv`
4. **Zero Configuration**: No additional setup required - it works automatically

## File Organization in S3

```
typing-clients-uuid-system/
└── csv-versions/
    ├── 2025/
    │   ├── 01/
    │   │   ├── output_2025-01-15_143052.csv
    │   │   ├── output_2025-01-15_151230.csv
    │   │   └── output_2025-01-20_093015.csv
    │   └── 02/
    │       ├── output_2025-02-01_080000.csv
    │       └── output_2025-02-05_141500.csv
    └── ...
```

## Usage Examples

### 1. Run Pipeline (Automatic Versioning)
```bash
# When you run the pipeline, CSV versions are created automatically
python3 simple_workflow.py

# Or with enhanced downloads
python3 download_all_enhanced.py
```

### 2. List CSV Versions
```bash
# List all CSV versions
python3 list_csv_versions.py

# Filter by filename prefix
python3 list_csv_versions.py --prefix output

# Limit results
python3 list_csv_versions.py --limit 10
```

### 3. Download a Specific Version
```bash
# Download a specific version
python3 list_csv_versions.py --download csv-versions/2025/01/output_2025-01-15_143052.csv

# Download with custom output name
python3 list_csv_versions.py --download csv-versions/2025/01/output_2025-01-15_143052.csv --output old_version.csv
```

### 4. Test Versioning
```bash
# Run the test script to see versioning in action
python3 test_csv_versioning.py
```

## Implementation Details

### Integrated Components

1. **utils/csv_s3_versioning.py**
   - Core versioning functionality
   - Handles S3 uploads and version management
   - Provides listing and retrieval functions

2. **utils/csv_manager.py**
   - Modified `safe_csv_write()` to automatically upload versions
   - Only uploads when `storage_mode: "s3"` in config
   - Non-blocking - CSV write succeeds even if S3 upload fails

3. **Configuration**
   - Uses existing S3 configuration from `config/config.yaml`
   - No additional configuration needed

### Metadata

Each uploaded CSV version includes metadata:
- `original_filename`: The original CSV filename
- `upload_timestamp`: When the version was created
- `file_size`: Size of the CSV file
- `operation`: The operation that triggered the save (e.g., "workflow_output")
- `row_count`: Number of rows in the CSV
- `column_count`: Number of columns in the CSV

## Benefits

1. **Version History**: Never lose previous states of your data
2. **Audit Trail**: Track when and how data changed
3. **Easy Recovery**: Retrieve any previous version when needed
4. **Automatic**: No manual intervention required
5. **Organized**: Chronological organization by year/month
6. **Searchable**: Easy to find versions by date or filename

## Monitoring

To monitor CSV versioning:

1. Check logs for versioning messages:
   ```
   CSV version uploaded to S3: output_2025-01-21_143052.csv
   ```

2. Use `list_csv_versions.py` to view all versions

3. Check S3 directly:
   ```bash
   aws s3 ls s3://typing-clients-uuid-system/csv-versions/ --recursive
   ```

## Troubleshooting

If versioning isn't working:

1. **Check S3 Mode**: Ensure `storage_mode: "s3"` in `config/config.yaml`
2. **Check Permissions**: Verify AWS credentials have S3 write access
3. **Check Logs**: Look for warning messages about S3 upload failures
4. **Test Manually**: Run `test_csv_versioning.py` to verify setup

## Cost Considerations

- Each CSV version uses S3 storage
- Typical CSV files are small (< 1MB)
- S3 storage costs ~$0.023/GB/month
- Consider lifecycle policies for old versions if needed

## Future Enhancements

Possible future improvements:
- Compression for older versions
- Automatic cleanup of versions older than X days
- Diff visualization between versions
- Version comparison tools