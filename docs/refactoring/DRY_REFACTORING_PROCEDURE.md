# DRY (Don't Repeat Yourself) Refactoring Procedure

## Overview
This document provides a meticulous, step-by-step procedure to eliminate code duplication in the typing-clients-ingestion codebase. Each step prioritizes extending existing modules over creating new files.

---

## Step 1: Consolidate Path Setup Operations (17 files affected)

### 1.1 Identify Duplication
The pattern `sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))` appears in 17 files.

### 1.2 Use Existing Solution
**Target Module**: `utils/path_setup.py` (already exists!)

### 1.3 Implementation
Replace in all affected files:
```python
# OLD CODE (remove this)
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# NEW CODE (use this)
from utils.path_setup import init_project_imports
init_project_imports()
```

### 1.4 Files to Update
- `run_complete_workflow.py`
- `run_all_tests.py`
- `run_git_setup.py`
- `simple_workflow.py`
- `check_entries.py`
- `download_emilie_folder.py`
- All test files in `tests/`

### 1.5 Testing
```bash
# Run each updated script to ensure imports work
python run_complete_workflow.py --help
python -m pytest tests/ -v
```

---

## Step 2: Consolidate CSV Operations (Multiple files affected)

### 2.1 Identify Duplication
CSV reading, writing, sanitization, and integrity checking code is scattered across multiple files.

### 2.2 Use Existing Solution
**Target Module**: `utils/csv_manager.py` (comprehensive consolidation already exists!)

### 2.3 Implementation
Replace all direct CSV operations with CSVManager:

```python
# OLD CODE (various patterns)
import csv
with open(csv_file, 'r') as f:
    reader = csv.DictReader(f)
    # ... custom processing

# NEW CODE
from utils.csv_manager import CSVManager
csv_manager = CSVManager()
data = csv_manager.read_csv(csv_file)
```

### 2.4 Specific Refactorings
- Replace `atomic_csv.py` usage → Use `csv_manager.write_atomic()`
- Replace `streaming_csv.py` usage → Use `csv_manager.stream_process()`
- Replace custom field sanitization → Use `csv_manager.sanitize_field()`
- Replace CSV integrity checks → Use `csv_manager.validate_integrity()`

### 2.5 Testing
```bash
# Test CSV operations
python -m pytest tests/test_csv_manager.py -v
# Run workflow with CSV processing
python simple_workflow.py
```

---

## Step 3: Standardize Error Handling (51 files affected)

### 3.1 Identify Duplication
Generic try/except blocks with similar logging patterns throughout codebase.

### 3.2 Use Existing Solution
**Target Module**: `utils/error_decorators.py` (decorators already exist!)

### 3.3 Implementation
Replace inline error handling with decorators:

```python
# OLD CODE
def process_file(filepath):
    try:
        with open(filepath, 'r') as f:
            # process file
    except FileNotFoundError:
        logger.error(f"File not found: {filepath}")
        return None
    except Exception as e:
        logger.error(f"Error processing file: {e}")
        return None

# NEW CODE
from utils.error_decorators import handle_file_operations

@handle_file_operations("process_file", return_on_error=None)
def process_file(filepath):
    with open(filepath, 'r') as f:
        # process file
```

### 3.4 Update Error Messages
Extend `utils/error_messages.py` with any missing error types:
```python
# In error_messages.py, add to existing messages
def processing_error(operation: str, details: str = None) -> str:
    """Generate standardized processing error message."""
    base = f"Processing error in {operation}"
    return f"{base}: {details}" if details else base
```

### 3.5 Testing
```bash
# Test error handling
python -m pytest tests/test_error_handling.py -v
# Test with intentional errors
python test_error_scenarios.py
```

---

## Step 4: Create Unified Subprocess Handler (12 files affected)

### 4.1 Identify Duplication
Subprocess execution with real-time output handling repeated across multiple files.

### 4.2 Extend Existing Module
**Target Module**: `utils/config.py` (add subprocess utilities here)

### 4.3 Implementation
Add to `utils/config.py`:
```python
def run_subprocess(command: List[str], description: str = None, 
                  capture_output: bool = True, check: bool = True) -> subprocess.CompletedProcess:
    """
    Run subprocess with real-time output and error handling.
    
    Args:
        command: Command list to execute
        description: Description for logging
        capture_output: Whether to capture output
        check: Whether to raise on non-zero exit
    
    Returns:
        CompletedProcess instance
    """
    logger = get_logger(__name__)
    desc = description or ' '.join(command[:2])
    
    logger.info(f"Running: {desc}")
    
    try:
        if capture_output:
            process = subprocess.Popen(
                command,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                bufsize=1
            )
            
            # Real-time output
            for line in process.stdout:
                print(line.rstrip())
                logger.debug(line.rstrip())
            
            process.wait()
            
            if check and process.returncode != 0:
                stderr = process.stderr.read() if process.stderr else ""
                raise subprocess.CalledProcessError(
                    process.returncode, command, stderr=stderr
                )
            
            return subprocess.CompletedProcess(
                command, process.returncode, 
                stdout=None, stderr=None
            )
        else:
            return subprocess.run(command, check=check)
            
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed: {desc}")
        logger.error(f"Exit code: {e.returncode}")
        if e.stderr:
            logger.error(f"Error output: {e.stderr}")
        raise
    except Exception as e:
        logger.error(f"Unexpected error running {desc}: {e}")
        raise
```

### 4.4 Replace Duplicated Code
Update all files using subprocess patterns:
```python
# OLD CODE
process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
# ... custom handling

# NEW CODE
from utils.config import run_subprocess
result = run_subprocess(cmd, description="Running analysis")
```

### 4.5 Testing
```bash
# Test subprocess handling
python -c "from utils.config import run_subprocess; run_subprocess(['echo', 'test'])"
# Run workflows using subprocess
python run_complete_workflow.py
```

---

## Step 5: Consolidate Directory Creation (10 files affected)

### 5.1 Identify Duplication
Pattern `os.makedirs(..., exist_ok=True)` repeated throughout.

### 5.2 Use Existing Solutions
**Target Modules**: 
- `utils/path_setup.py` has `ensure_directory_exists()`
- `utils/config.py` has `ensure_directory()`

### 5.3 Implementation
Standardize on `path_setup.ensure_directory_exists()`:
```python
# OLD CODE
os.makedirs(output_dir, exist_ok=True)

# NEW CODE
from utils.path_setup import ensure_directory_exists
from pathlib import Path
ensure_directory_exists(Path(output_dir))
```

### 5.4 Update config.py
Remove duplicate `ensure_directory()` from config.py and import from path_setup:
```python
# In config.py
from .path_setup import ensure_directory_exists as ensure_directory
```

### 5.5 Testing
```bash
# Test directory creation
python -c "from utils.path_setup import ensure_directory_exists, Path; ensure_directory_exists(Path('test_dir'))"
ls -la test_dir/
rm -rf test_dir/
```

---

## Step 6: Standardize Configuration Access (Multiple files)

### 6.1 Identify Duplication
Repeated patterns for getting config values with defaults.

### 6.2 Extend Existing Module
**Target Module**: `utils/config.py`

### 6.3 Implementation
Add convenience methods to Config class:
```python
# Add to Config class in config.py
def get_download_config(service: str = None) -> Dict[str, Any]:
    """Get download configuration for a specific service or all services."""
    downloads = self.get_section('downloads')
    return downloads.get(service, {}) if service else downloads

def get_retry_config(operation: str = None) -> Dict[str, Any]:
    """Get retry configuration for specific operation or defaults."""
    retry = self.get_section('retry')
    if operation and f"{operation}_max_retries" in retry:
        return {
            'max_retries': retry.get(f"{operation}_max_retries", retry.get('max_retries', 3)),
            'retry_delay': retry.get(f"{operation}_retry_delay", retry.get('retry_delay', 1.0))
        }
    return retry

def get_output_paths(create: bool = True) -> Dict[str, Path]:
    """Get all output paths, optionally creating directories."""
    from .path_setup import ensure_directory_exists, get_project_root
    
    paths = {
        'outputs': get_project_root() / 'outputs',
        'logs': get_project_root() / 'logs',
        'data': get_project_root() / 'data',
        'reports': get_project_root() / 'reports'
    }
    
    if create:
        for path in paths.values():
            ensure_directory_exists(path)
    
    return paths
```

### 6.4 Update Usage
```python
# OLD CODE
config = get_config()
max_workers = config.get('downloads.youtube.max_workers', 5)

# NEW CODE
config = get_config()
youtube_config = config.get_download_config('youtube')
max_workers = youtube_config.get('max_workers', 5)
```

### 6.5 Testing
```bash
# Test configuration access
python -c "from utils.config import get_config; c = get_config(); print(c.get_output_paths())"
```

---

## Step 7: Remove Redundant Retry Implementations

### 7.1 Identify Duplication
Multiple custom retry implementations alongside `utils/retry_utils.py`.

### 7.2 Use Existing Solution
**Target Module**: `utils/retry_utils.py`

### 7.3 Implementation
Replace all custom retry logic:
```python
# OLD CODE
for attempt in range(max_retries):
    try:
        # operation
        break
    except Exception as e:
        if attempt == max_retries - 1:
            raise
        time.sleep(retry_delay)

# NEW CODE
from utils.retry_utils import retry_with_backoff

@retry_with_backoff(max_retries=3, base_delay=1.0)
def operation():
    # operation code
```

### 7.4 Testing
```bash
# Test retry functionality
python -m pytest tests/test_retry_utils.py -v
```

---

## Step 8: Consolidate Import Handling

### 8.1 Identify Duplication
Try/except ImportError blocks for relative/absolute imports.

### 8.2 Use Existing Solution
**Target Module**: `utils/import_utils.py`

### 8.3 Implementation
Use existing `safe_import()` function:
```python
# OLD CODE
try:
    from config import get_config
except ImportError:
    from .config import get_config

# NEW CODE
from utils.import_utils import safe_import
get_config = safe_import('config', 'get_config', relative_fallback=True)
```

### 8.4 Testing
```bash
# Test imports
python -m pytest tests/test_import_utils.py -v
```

---

## Step 9: Clean Up Archive and Backup Directories

### 9.1 Identify Redundancy
Backup directories contain duplicate implementations.

### 9.2 Action
Move useful code patterns to main modules, then remove:
```bash
# Verify no unique code exists
grep -r "class\|def" backup/ scripts/archive/ backups/ | grep -v "__pycache__"

# If confirmed, remove directories
rm -rf backup/ scripts/archive/ backups/
```

### 9.3 Update .gitignore
```
# Add to .gitignore
backup/
backups/
*_backup.py
*_old.py
archive/
```

---

## Step 10: Final Documentation Update

### 10.1 Update README.md
Add section on code organization:
```markdown
## Code Organization

### Consolidated Utilities
- **Path Management**: Use `utils/path_setup.py` for all path operations
- **CSV Operations**: Use `utils/csv_manager.py` for all CSV handling
- **Error Handling**: Use decorators from `utils/error_decorators.py`
- **Configuration**: Access via `utils/config.py` convenience methods
- **Subprocess**: Use `utils/config.run_subprocess()` for external commands
- **Retries**: Use `utils/retry_utils.py` decorators

### Import Pattern
```python
from utils.path_setup import init_project_imports
init_project_imports()

# Now import project modules
from utils.csv_manager import CSVManager
from utils.config import get_config
```

### 10.2 Create Migration Guide
Create `MIGRATION_GUIDE.md`:
```markdown
# Migration Guide for DRY Refactoring

## Quick Reference

| Old Pattern | New Pattern | Module |
|------------|-------------|---------|
| `sys.path.insert(...)` | `init_project_imports()` | `utils.path_setup` |
| Direct CSV operations | `CSVManager` methods | `utils.csv_manager` |
| Try/except blocks | `@handle_*` decorators | `utils.error_decorators` |
| `subprocess.Popen(...)` | `run_subprocess()` | `utils.config` |
| `os.makedirs(...)` | `ensure_directory_exists()` | `utils.path_setup` |
```

---

## Testing Strategy

### Comprehensive Test Run
```bash
# 1. Run all unit tests
python -m pytest tests/ -v

# 2. Run integration test
python run_all_tests.py

# 3. Test main workflows
python simple_workflow.py
python run_complete_workflow.py --dry-run

# 4. Check for import errors
python -m py_compile **/*.py
```

### Rollback Plan
1. Git commit before each major step
2. Tag stable version: `git tag pre-dry-refactor`
3. Create feature branch: `git checkout -b dry-refactoring`
4. Test thoroughly before merging

---

## Summary

This refactoring eliminates:
- 17 duplicate path setup patterns
- 12 subprocess handling duplications  
- 51 scattered error handling blocks
- 10 directory creation patterns
- Multiple CSV operation implementations
- Redundant retry logic
- Duplicate import handling

**Key Principle**: Every consolidation uses or extends existing modules. No new files created.