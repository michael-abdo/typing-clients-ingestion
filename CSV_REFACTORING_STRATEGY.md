# CSV Refactoring Strategy - Phase 2

## Overview
Based on the dry run analysis, 32 files need CSV consolidation. This phase will replace scattered CSV operations with the unified CSVManager.

## CSV Consolidation Targets

### High-Impact Files (Priority 1)
1. **run_complete_workflow.py** - Main workflow using direct csv.Reader/Writer
2. **utils/comprehensive_file_mapper.py** - Large file using pandas CSV
3. **utils/map_files_to_types.py** - Core mapping using pandas CSV
4. **utils/csv_file_integrity_mapper.py** - Dedicated CSV integrity tool

### Medium-Impact Files (Priority 2)
5. **run_all_tests.py** - Test runner using pandas CSV
6. **simple_workflow.py** - Minimal workflow using pandas CSV
7. **check_doc_links.py** - Documentation checker using pandas CSV
8. **utils/create_definitive_mapping.py** - Mapping utility using pandas CSV
9. **utils/fix_csv_file_mappings.py** - CSV fixer using pandas CSV
10. **utils/scrape_google_sheets.py** - Sheets scraper using direct csv

### Utility Scripts (Priority 3)
11. **utils/recover_unmapped_files.py** - Recovery utility
12. **utils/clean_file_mapper.py** - Cleaning utility
13. **utils/error_decorators.py** - Error handling (minimal CSV usage)
14. **create_snapshot.py** - Snapshot tool
15. **Multiple script files** - Various workflow scripts

## Refactoring Patterns

### Pattern 1: pandas.read_csv() → CSVManager.safe_csv_read()
```python
# Before
import pandas as pd
df = pd.read_csv('file.csv')

# After
from utils.csv_manager import CSVManager
df = CSVManager.safe_csv_read('file.csv', dtype_spec='tracking')
```

### Pattern 2: DataFrame.to_csv() → CSVManager.safe_csv_write()
```python
# Before
df.to_csv('file.csv', index=False)

# After
from utils.csv_manager import CSVManager
manager = CSVManager('file.csv')
success = manager.safe_csv_write(df, operation_name="data_export")
```

### Pattern 3: csv.DictReader/Writer → CSVManager.atomic_*()
```python
# Before
import csv
with open('file.csv', 'r') as f:
    reader = csv.DictReader(f)
    for row in reader:
        process(row)

# After
from utils.csv_manager import CSVManager
manager = CSVManager('file.csv')
df = manager.read(dtype_spec='all_string')
for _, row in df.iterrows():
    process(row.to_dict())
```

### Pattern 4: Atomic CSV Updates
```python
# Before
import csv
with open('file.csv', 'w') as f:
    writer = csv.DictWriter(f, fieldnames=headers)
    writer.writeheader()
    writer.writerows(data)

# After
from utils.csv_manager import CSVManager
manager = CSVManager('file.csv')
success = manager.atomic_write(data, fieldnames=headers)
```

## Implementation Strategy

### Phase 2A: High-Impact Files (Day 1)
1. **Test CSVManager API** - Verify all methods work correctly
2. **Refactor run_complete_workflow.py** - Most critical file
3. **Refactor utils/comprehensive_file_mapper.py** - Large complex file
4. **Test refactored files** - Ensure no regressions

### Phase 2B: Medium-Impact Files (Day 2)
1. **Refactor utils/map_files_to_types.py**
2. **Refactor utils/csv_file_integrity_mapper.py**
3. **Refactor run_all_tests.py**
4. **Test batch of refactored files**

### Phase 2C: Utility Scripts (Day 3)
1. **Refactor remaining utils/*.py files**
2. **Refactor script files**
3. **Update documentation**
4. **Final testing and performance comparison**

## Benefits Expected

### Code Reduction
- Eliminate ~500-800 lines of duplicate CSV handling
- Remove inconsistent error handling
- Standardize field sanitization

### Performance Improvements
- Automatic file locking prevents corruption
- Atomic operations ensure integrity
- Configurable chunk processing for large files

### Maintenance Benefits
- Single source of truth for CSV operations
- Consistent error messages and logging
- Automatic backup creation

## Risk Mitigation

### Backup Strategy
- All modified files get .backup extensions
- Test each file after refactoring
- Git commits after each successful batch

### Testing Strategy
- Unit tests for each refactored method
- Integration tests with real CSV files
- Performance benchmarks before/after

### Rollback Plan
- Keep .backup files until verification complete
- Git branch isolation for easy revert
- Verify workflows still function correctly

## Success Criteria

1. **All 32 files successfully refactored**
2. **No functional regressions**
3. **Performance equal or better**
4. **Test suite passes**
5. **Documentation updated**

## Next Steps

1. ✅ Create this strategy document
2. ⏳ Test CSVManager API thoroughly
3. ⏳ Start with run_complete_workflow.py refactoring
4. ⏳ Apply pattern-based refactoring to all targets
5. ⏳ Comprehensive testing and validation

---

*CSV Refactoring Strategy v1.0 - Phase 2 of DRY Refactoring*