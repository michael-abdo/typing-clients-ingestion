# Consolidated Modules Usage Guide

This guide provides comprehensive examples for using the newly consolidated CSV and File Mapping modules.

## 🎯 Overview

The DRY (Don't Repeat Yourself) consolidation has unified 10 separate modules into 2 powerful classes:

- **CSVManager**: Unified CSV operations (from 5 modules)
- **FileMapper**: Unified file mapping (from 5 modules)

## 📊 CSVManager - Complete Usage Guide

### Basic Operations

```python
from utils.csv_manager import CSVManager

# Initialize with default CSV path
csv_manager = CSVManager()

# Initialize with custom path and options
csv_manager = CSVManager(
    csv_path='custom/path.csv',
    chunk_size=2000,
    use_file_lock=True,
    auto_backup=True,
    timeout=60.0
)

# Read CSV data
df = csv_manager.read()
print(f"Loaded {len(df)} rows")

# Static method for one-off reads
df_static = CSVManager.safe_csv_read('path/to/file.csv')
```

### Atomic Operations (Thread-Safe)

```python
# Atomic write - completely replace CSV
new_data = [
    {'name': 'John Doe', 'type': 'ENFP', 'email': 'john@example.com'},
    {'name': 'Jane Smith', 'type': 'INFJ', 'email': 'jane@example.com'}
]
success = csv_manager.atomic_write(new_data, fieldnames=['name', 'type', 'email'])

# Atomic append - add new rows safely
new_rows = [
    {'name': 'Bob Wilson', 'type': 'ENTP', 'email': 'bob@example.com'}
]
success = csv_manager.atomic_append(new_rows)

# Atomic update with custom function
def update_function(df):
    # Your update logic here
    df.loc[df['type'] == 'ENFP', 'category'] = 'Enthusiast'
    return df

success = csv_manager.atomic_update(update_function)
```

### Streaming Operations (Memory Efficient)

```python
# Stream processing for large files
def count_personality_types(chunk):
    return chunk['type'].value_counts().to_dict()

# Process in chunks
result = csv_manager.stream_process(
    process_func=count_personality_types,
    output_path='processed_output.csv',
    chunk_size=1000,
    include_headers=True
)

# Stream filtering
def filter_enfp(chunk):
    return chunk[chunk['type'] == 'ENFP']

filtered_count = csv_manager.stream_filter(
    filter_func=filter_enfp,
    output_path='enfp_only.csv'
)
```

### Tracking Operations (Download Management)

```python
# Ensure CSV has all required tracking columns
csv_manager.ensure_tracking_columns()

# Get pending downloads
pending_youtube = csv_manager.get_pending_downloads(
    download_type='youtube',
    include_failed=False
)

pending_drive = csv_manager.get_pending_downloads(
    download_type='drive',
    include_failed=True
)

pending_all = csv_manager.get_pending_downloads(
    download_type='both'
)

# Update download status
from utils.row_context import DownloadResult

download_result = DownloadResult(
    success=True,
    files=['video1.mp4', 'video2.mp4'],
    errors=[]
)

success = csv_manager.update_download_status(
    row_index=5,
    download_type='youtube',
    result=download_result
)

# Reset failed downloads for retry
csv_manager.reset_download_status('123', 'youtube')

# Get comprehensive status summary
summary = csv_manager.get_download_status_summary()
print(f"Total pending: {summary['total_pending']}")
print(f"Success rate: {summary['success_rate']}")
```

### Integrity and Backup Operations

```python
# Analyze file integrity
integrity_report = csv_manager.analyze_file_integrity()
print(f"Integrity score: {integrity_report['integrity_score']}")

# Create manual backup
backup_path = csv_manager.create_backup('manual_backup')
print(f"Backup created: {backup_path}")

# Write with automatic backup
df_modified = df.copy()
df_modified['new_column'] = 'new_value'
csv_manager.safe_csv_write(df_modified, operation_name='add_new_column')
```

## 🗂️ FileMapper - Complete Usage Guide

### Basic File Mapping

```python
from utils.comprehensive_file_mapper import FileMapper

# Initialize file mapper
mapper = FileMapper('outputs/output.csv')

# Perform comprehensive file mapping
mapping_results = mapper.map_files()
print(f"Mapped {len(mapping_results)} files")

# Get all file mappings
all_mappings = mapper.get_file_mappings()

# Get files for specific row
row_files = mapper.get_row_files('123')
print(f"Row 123 has {len(row_files)} files")
```

### Type Organization

```python
# Organize files by personality type
organization_result = mapper.organize_by_type(
    output_dir='organized_by_type',
    copy_files=True  # False to move instead of copy
)

# Add type information to filenames
rename_result = mapper.add_type_to_filenames(
    dry_run=True  # Set False to actually rename
)
print(f"Would rename {rename_result['renamed_count']} files")

# Get type distribution
type_dist = mapper.get_type_distribution()
for personality_type, count in type_dist.items():
    print(f"{personality_type}: {count} files")
```

### Issue Resolution

```python
# Find mapping conflicts
conflicts = mapper.find_mapping_conflicts()
for conflict in conflicts:
    print(f"Conflict: {conflict['file']} mapped to multiple rows")

# Fix orphaned files
fix_result = mapper.fix_orphaned_files()
print(f"Fixed {fix_result['fixed_count']} orphaned files")

# Clean duplicate files
duplicate_result = mapper.clean_duplicate_files(
    action='move'  # 'move' or 'delete'
)
print(f"Cleaned {duplicate_result['removed_count']} duplicates")

# Validate all mappings
validation_result = mapper.validate_mappings()
print(f"Validation passed: {validation_result['is_valid']}")
```

### File Recovery

```python
# Recover unmapped files using various strategies
recovery_result = mapper.recover_unmapped_files(
    recovery_strategy='id_matching'  # 'id_matching', 'name_matching', 'content_analysis'
)
print(f"Recovered {len(recovery_result['matched'])} files")

# Extract IDs from filenames
video_id = mapper.extract_video_id('watch_video,abc123def456.mp4')
drive_id = mapper.extract_drive_id('document_1BxYz2C3D4E5F6G7H8I9J0K.pdf')
```

### Definitive Mapping Creation

```python
# Create definitive CSV-to-file mapping
definitive_mapping = mapper.create_definitive_mapping()
print(f"Created definitive mapping with {len(definitive_mapping)} entries")

# Create corrected metadata files
metadata_count = mapper.create_corrected_metadata()
print(f"Created {metadata_count} corrected metadata files")

# Generate comprehensive report
report = mapper.generate_comprehensive_report()
print(f"Mapping success rate: {report['success_rate']}")
print(f"Total conflicts: {report['conflicts']}")
print(f"Unmapped files: {report['unmapped_count']}")
```

### Export and Import

```python
# Export mappings in various formats
csv_export = mapper.export_mappings(
    format='csv',
    output_path='file_mappings.csv'
)

json_export = mapper.export_mappings(
    format='json',
    output_path='file_mappings.json'
)

excel_export = mapper.export_mappings(
    format='excel',
    output_path='file_mappings.xlsx'
)

print(f"Exported mappings to: {csv_export}")
```

## 🔧 Advanced Usage Patterns

### Combined Operations

```python
# Use both modules together for complete workflow
from utils.csv_manager import CSVManager
from utils.comprehensive_file_mapper import FileMapper

# Initialize both
csv_manager = CSVManager('outputs/output.csv')
file_mapper = FileMapper('outputs/output.csv')

# Ensure CSV is ready
csv_manager.ensure_tracking_columns()

# Map all files
file_mapper.map_files()

# Get pending downloads from CSV
pending = csv_manager.get_pending_downloads()

# Organize existing files by type
file_mapper.organize_by_type('organized_files')

# Update CSV with mapping results
def update_with_mappings(df):
    # Your logic to update CSV with mapping results
    return df

csv_manager.atomic_update(update_with_mappings)
```

### Error Handling

```python
try:
    # CSV operations with error handling
    csv_manager = CSVManager('outputs/output.csv')
    df = csv_manager.read()
    
    # File operations with error handling
    mapper = FileMapper('outputs/output.csv')
    mappings = mapper.map_files()
    
except FileNotFoundError:
    print("CSV file not found")
except PermissionError:
    print("Permission denied accessing files")
except Exception as e:
    print(f"Unexpected error: {e}")
```

### Performance Optimization

```python
# Optimize for large datasets
csv_manager = CSVManager(
    csv_path='large_dataset.csv',
    chunk_size=5000,  # Larger chunks for better performance
    use_file_lock=False,  # Disable if single-threaded
    auto_backup=False  # Disable for frequent operations
)

# Use streaming for memory efficiency
csv_manager.stream_process(
    process_func=your_processing_function,
    chunk_size=10000
)
```

## 📋 Best Practices

1. **Always use CSVManager for CSV operations** instead of direct pandas operations
2. **Use FileMapper.map_files()** before other mapping operations
3. **Enable auto_backup** for important CSV modifications
4. **Use atomic operations** for multi-threaded environments
5. **Test with dry_run=True** before bulk file operations
6. **Check validation results** before applying fixes
7. **Export mappings** before making major changes

## 🚨 Migration Notes

- Legacy modules still work but show deprecation warnings
- Update import statements to use consolidated modules
- Replace multiple class instances with single manager instances
- Adapt method calls to new unified interface
- Test thoroughly after migration

## 📞 Support

For issues or questions about the consolidated modules:
1. Check this usage guide first
2. Review the design documents (CSV_CONSOLIDATION_DESIGN.md, FILE_MAPPING_CONSOLIDATION_DESIGN.md)
3. Run with error handling to get detailed error messages
4. Open an issue on GitHub with specific error details