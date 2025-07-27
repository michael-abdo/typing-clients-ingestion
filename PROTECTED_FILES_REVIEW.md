# Protected Files - Client Review Needed

The following files have restricted permissions and should be reviewed by the client:

## Backup Data Files
- backup/gold_standard_20250630_125233/ (mapping backups)
- analysis/ directory (analysis outputs) 
- audit/ directory (quality audits)

## Test Output Files  
- e2e_test_outputs/ (end-to-end test results)
- benchmark_outputs/ (performance benchmarks)

## Compressed Backups
- backups/output/*.gz (compressed CSV backups)

## Legacy Content
- filtered_content/ (content filtering results)
- removed_duplicates/ (duplicate file cleanup)

**Recommendation**: Review these files for business value before removal.
**Action**: Client should determine which files to keep vs. remove based on operational needs.

