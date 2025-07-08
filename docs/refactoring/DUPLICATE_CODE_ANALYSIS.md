# Duplicate Code Analysis - typing-clients-ingestion

## 1. Path Setup Duplication

### Pattern: `sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))`

**Found in 17 files** - This exact pattern is repeated across multiple test files and scripts to add the project root to Python path.

**Files affected:**
- run_complete_workflow.py (line 10)
- scripts/run_drive_downloads_async.py
- scripts/run_youtube_downloads_async.py 
- tests/test_*.py files
- scripts/test_single_download.py
- scripts/archive/run_complete_workflow_old.py

**Solution**: Already consolidated in `utils/path_setup.py` with functions like:
- `setup_project_path()`
- `init_project_imports()`

**Recommendation**: Replace all instances with:
```python
from utils.path_setup import init_project_imports
init_project_imports()
```

---

## 2. Subprocess Execution Patterns

### Pattern: Duplicate subprocess handling code

**Found in 12 files** - Similar subprocess.Popen/run patterns with real-time output handling.

**Example from run_complete_workflow.py (lines 51-65):**
```python
process = subprocess.Popen(
    command,
    stdout=subprocess.PIPE,
    stderr=subprocess.STDOUT,
    text=True,
    bufsize=1,
    universal_newlines=True
)
for line in process.stdout:
    print(line, end='')
process.wait()
```

**Similar patterns in:**
- utils/logger.py (lines 142-149)
- utils/error_handling.py
- run_all_tests.py
- run_cleanup.py
- scripts/run_drive_downloads_async.py
- scripts/run_youtube_downloads_async.py

**Solution**: Create a unified subprocess handler:
```python
# utils/process_utils.py
def run_command_with_output(command, description=None, logger=None):
    """Unified subprocess execution with real-time output"""
```

---

## 3. Error Handling Patterns

### Pattern: Generic exception handling with similar logging

**Found in 51 files** - Repetitive try/except blocks with similar error logging patterns.

**Common pattern:**
```python
try:
    # operation
except Exception as e:
    logger.error(f"Error message: {e}")
    # or
    print(f"Error: {e}")
```

**Solution**: Already partially consolidated in:
- utils/error_handling.py (ErrorHandler class)
- utils/error_decorators.py

**Recommendation**: Use decorators consistently:
```python
from utils.error_decorators import handle_file_operations

@handle_file_operations
def my_function():
    # code
```

---

## 4. Directory Creation Pattern

### Pattern: `os.makedirs(..., exist_ok=True)`

**Found in 10 files** - Repeated directory creation logic.

**Files affected:**
- run_complete_workflow.py (lines 140, 217)
- utils/organize_by_type_final.py
- utils/fix_mapping_issues.py
- utils/map_files_to_types.py
- utils/comprehensive_file_mapper.py
- scripts/download_large_drive_file_direct.py

**Solution**: Already exists in utils/config.py:
```python
def create_download_dir(download_dir: str, logger=None) -> Path:
    """Create download directory if it doesn't exist"""
```

---

## 5. CSV Reading/Writing Patterns

### Pattern: Duplicate CSV handling with pandas

**Multiple patterns:**
1. Safe CSV reading with consistent dtypes
2. CSV field sanitization
3. Atomic CSV writing with temp files
4. CSV integrity checking

**Found across:**
- utils/csv_manager.py (consolidated version)
- utils/csv_tracker.py
- utils/streaming_csv.py
- utils/csv_file_integrity_mapper.py

**Solution**: Already consolidated in `utils/csv_manager.py` with the CSVManager class.

---

## 6. Configuration Access Patterns

### Pattern: Repeated config value retrieval

**Common patterns:**
```python
config = get_config()
output_csv_path = config.get('paths.output_csv', 'output.csv')
youtube_dir = config.get('paths.youtube_downloads', 'youtube_downloads')
```

**Solution**: Already exists in utils/config.py with convenience functions:
- `get_output_csv_path()`
- `get_youtube_downloads_dir()`
- `get_drive_downloads_dir()`

---

## 7. Retry Logic Duplication

### Pattern: Multiple retry implementations

**Found in:**
- utils/retry_utils.py (main implementation)
- utils/download_youtube.py (custom retry logic)
- utils/download_drive.py (custom retry logic)
- Various download scripts with inline retry loops

**Solution**: Use the centralized retry decorator:
```python
from utils.retry_utils import retry_with_backoff

@retry_with_backoff(max_attempts=5, base_delay=2.0)
def download_function():
    # code
```

---

## 8. Import Error Handling

### Pattern: Try/except ImportError blocks

**Common pattern:**
```python
try:
    from module import function
except ImportError:
    from .module import function
```

**Found in many utils files** for relative/absolute import compatibility.

**Solution**: Already exists in utils/config.py:
```python
safe_import('module_name', 'function_name')
```

---

## 9. Logging Initialization

### Pattern: Duplicate logger setup code

**Multiple implementations:**
- utils/logger.py (DualLogger, PipelineLogger)
- utils/logging_config.py
- Individual logger setup in various scripts

**Recommendation**: Use centralized logging:
```python
from utils.logging_config import get_logger
logger = get_logger(__name__)
```

---

## 10. File Lock Usage

### Pattern: Repeated file locking patterns for CSV operations

**Common pattern:**
```python
with file_lock(csv_path, timeout=30):
    # CSV operations
```

**Solution**: Already consolidated in utils/csv_manager.py with automatic file locking.

---

## Summary of Recommendations

1. **Immediate Actions:**
   - Replace all `sys.path.insert` patterns with `utils.path_setup` functions
   - Use `CSVManager` class for all CSV operations
   - Use centralized retry decorators instead of custom retry loops

2. **Medium Priority:**
   - Consolidate subprocess handling into `utils.process_utils`
   - Use config convenience functions instead of direct `config.get()` calls
   - Replace generic exception handling with error decorators

3. **Long Term:**
   - Remove duplicate implementations in backup/ and archive/ directories
   - Standardize on single import pattern using `safe_import()`
   - Create a single source of truth for each utility function

4. **Code Cleanup:**
   - Remove files in backup/contaminated_utilities_*
   - Archive or remove scripts/archive/ directory
   - Consolidate similar run_*.py scripts where possible

By addressing these duplications, the codebase can be significantly simplified and maintenance burden reduced.