# CSV-File Integrity Report

## Executive Summary

We've completed a comprehensive analysis of the relationship between CSV rows and downloaded files. The goal was to ensure each downloaded file is correctly mapped to its CSV row, and each row with downloadable content has its files.

## Key Findings

### Overall Statistics
- **Total CSV rows**: 494
- **Rows with actual files**: 49 (out of 69 rows marked as having completed downloads)
- **Files correctly mapped**: 71
- **Success rate**: 74.7% of expected files found

### Major Issues Discovered

1. **Mapping Conflicts**: 80 files
   - Files are currently associated with wrong CSV rows
   - Example: Row 494 (John Williams) files are incorrectly stored with Row 469
   - Most conflicts involve rows 469 and 462 having files that belong to other rows

2. **Missing Files**: 24 files
   - Listed in CSV as downloaded but not found on disk
   - May have been deleted or download failed without proper status update

3. **Unmapped Files**: 187 files
   - Exist on disk but not listed in any CSV row
   - Includes many metadata files and possibly test downloads

## Specific Examples

### Mismatched Mappings
- **Row 496 (James Kirton)**: 7 files incorrectly stored with Row 469
- **Row 492 (Kiko)**: 2 files incorrectly stored with Row 462
- **Row 494 (John Williams)**: Files listed but stored with Row 469

### Files Created
1. **definitive_csv_file_mapping.csv** - The authoritative mapping of 81 files to their correct rows
2. **mapping_conflicts.csv** - 80 files that need to be reassigned
3. **definitive_missing_files.csv** - 24 files that should exist but don't
4. **definitive_unmapped_files.csv** - 187 orphaned files

## Root Causes

1. **Metadata Corruption**: Download process sometimes assigned files to wrong row IDs
2. **Concurrent Downloads**: Multiple downloads may have interfered with each other
3. **CSV Updates**: Files may have been downloaded before CSV was properly updated

## Recommendations

### Immediate Actions
1. **Use definitive mapping**: The `definitive_csv_file_mapping.csv` should be treated as the authoritative source
2. **Fix conflicts**: Reassign the 80 conflicted files to their correct rows
3. **Investigate missing**: Determine if the 24 missing files can be re-downloaded

### Long-term Improvements
1. **Atomic operations**: Ensure file downloads and CSV updates happen atomically
2. **Validation checks**: Add integrity checks after each download
3. **Unique identifiers**: Use row IDs in filenames to prevent confusion

## Technical Details

### Mapping Process
1. Parsed CSV for all rows with `youtube_status` or `drive_status` = "completed"
2. Searched filesystem for files listed in `youtube_files` and `drive_files` columns
3. Compared current metadata assignments with CSV listings
4. Created definitive metadata files for correct mappings

### File Organization
- YouTube files: Found in `youtube_downloads/`
- Drive files: Found in `drive_downloads/`
- Metadata: JSON files with row assignments

## Conclusion

While 74.7% of files are correctly available, the 80 mapping conflicts represent a significant data integrity issue that should be resolved. The definitive mapping created provides a clear path forward for correction.