# Detailed DRY Violations with Code Examples

## 1. Import Pattern Duplication

### Current Pattern (Found in 30+ files):
```python
# utils/csv_manager.py (lines 21-38)
try:
    from file_lock import file_lock
    from sanitization import sanitize_error_message, sanitize_csv_field
    from config import get_config
    # ... more imports
except ImportError:
    from .file_lock import file_lock
    from .sanitization import sanitize_error_message, sanitize_csv_field
    from .config import get_config
    # ... more imports
```

### Should Use Existing Solution:
```python
# From utils/config.py (lines 216-279)
from config import safe_import

file_lock = safe_import('file_lock', 'file_lock')
sanitize_funcs = safe_import('sanitization', ['sanitize_error_message', 'sanitize_csv_field'])
```

## 2. Configuration Loading Duplication

### Duplicate Implementation 1:
```python
# simple_workflow.py (lines 28-52)
from utils.config import get_config

class Config:
    _config = get_config()
    GOOGLE_SHEET_URL = _config.get('google_sheets.url')
    TARGET_DIV_ID = str(_config.get('google_sheets.target_div_id'))
    OUTPUT_DIR = Path("simple_downloads")
    # ...
```

### Duplicate Implementation 2:
```python
# minimal/simple_workflow.py (lines 28-48)
class Config:
    GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/..."
    TARGET_DIV_ID = "1159146182"
    OUTPUT_DIR = Path("simple_downloads")
    # ... hardcoded values instead of config
```

### Should Be:
```python
# Single source using utils/config.py
config = get_config()
GOOGLE_SHEET_URL = config.get('google_sheets.url')
TARGET_DIV_ID = config.get('google_sheets.target_div_id')
OUTPUT_DIR = Path(config.get('paths.simple_downloads', 'simple_downloads'))
```

## 3. CSV Operations Duplication

### Found in multiple files:
```python
# Direct pandas operations (simple_workflow.py and others)
df = pd.read_csv(csv_path)
df.to_csv(csv_path, index=False)

# Custom implementations in various utils
with open(csv_path, 'r') as f:
    reader = csv.DictReader(f)
    # ...
```

### Should Use:
```python
# utils/csv_manager.py already provides
csv_manager = CSVManager(csv_path)
df = csv_manager.read_csv()
csv_manager.write_csv(df)
```

## 4. Directory Creation Pattern

### Duplicated in many files:
```python
# Pattern 1
if not os.path.exists(dir_path):
    os.makedirs(dir_path)

# Pattern 2
Path(dir_path).mkdir(parents=True, exist_ok=True)

# Pattern 3
downloads_path = Path(download_dir)
if not downloads_path.exists():
    downloads_path.mkdir(parents=True)
```

### Already exists in utils/config.py:
```python
# Line 143-159
def create_download_dir(download_dir: str, logger=None) -> Path:
    downloads_path = Path(download_dir)
    if not downloads_path.exists():
        downloads_path.mkdir(parents=True)
        if logger:
            logger.info(f"Created downloads directory: {download_dir}")
    return downloads_path
```

## 5. Magic Numbers Duplication

### File Size Constants:
```python
# Found in multiple files
SMALL_FILE_SIZE = 1048576    # 1MB
MEDIUM_FILE_SIZE = 10485760  # 10MB 
LARGE_FILE_SIZE = 104857600  # 100MB
CHUNK_SIZE = 8388608         # 8MB
```

### Already centralized in config.yaml but hardcoded elsewhere:
```python
# utils/config.py (lines 162-181)
def get_download_chunk_size(file_size: int) -> int:
    # ... but values are still hardcoded
    if file_size > thresholds.get("large", 104857600):  # Should use constant
        return thresholds.get("large", 8388608)
```

## 6. Error Handling Pattern Duplication

### Pattern repeated in many files:
```python
try:
    # operation
except Exception as e:
    logger.error(f"Error in operation: {str(e)}")
    if "rate limit" in str(e).lower():
        time.sleep(60)
        # retry
    elif "network" in str(e).lower():
        time.sleep(10)
        # retry
    else:
        raise
```

### Should use utils/error_handling.py:
```python
error_context = error_handler.handle_error(e, context)
if error_handler.should_retry(error_context, attempt):
    delay = error_handler.get_retry_delay(error_context, attempt)
    time.sleep(delay)
```

## 7. YouTube/Drive ID Extraction

### Duplicated regex patterns:
```python
# In multiple files
youtube_pattern = r'[a-zA-Z0-9_-]{11}'
drive_pattern = r'/file/d/([a-zA-Z0-9_-]+)'

# Different implementations of same logic
match = re.search(youtube_pattern, url)
if match:
    video_id = match.group()
```

## 8. Rate Limiting Implementation

### Scattered implementations:
```python
# Pattern 1
time.sleep(2)  # Hardcoded delays

# Pattern 2  
if attempt < max_attempts:
    time.sleep(attempt * 2)

# Pattern 3
wait_time = min(2 ** attempt, 60)
time.sleep(wait_time)
```

### Should use utils/rate_limiter.py and retry_utils.py:
```python
@rate_limit(calls_per_second=1)
@retry_with_backoff(max_attempts=3)
def download_file(url):
    # ...
```

## 9. Logging Setup Duplication

### Found in multiple forms:
```python
# Pattern 1
logger = logging.getLogger(__name__)

# Pattern 2
import logging
logging.basicConfig(level=logging.INFO)

# Pattern 3 (utils/logger.py)
class DualLogger:
    # Complex implementation

# Pattern 4 (utils/logging_config.py)
def get_logger(name):
    # Different implementation
```

## 10. File Validation Patterns

### Repeated validation:
```python
# Check file exists
if not os.path.exists(file_path):
    raise FileNotFoundError(f"File not found: {file_path}")

# Check file size
file_size = os.path.getsize(file_path)
if file_size > MAX_SIZE:
    raise ValueError(f"File too large: {file_size}")

# Check file extension
if not file_path.endswith(('.csv', '.json')):
    raise ValueError("Invalid file type")
```

### Should centralize in utils/validation.py

## Summary of Redundancy

| Category | Files Affected | Estimated Duplicate Lines | Impact |
|----------|---------------|-------------------------|---------|
| Import patterns | 30+ | ~500 | High |
| CSV operations | 40+ | ~1000 | Critical |
| Config loading | 5+ | ~200 | High |
| Error handling | 20+ | ~600 | High |
| File operations | 25+ | ~400 | Medium |
| Logging setup | 15+ | ~300 | Medium |
| Validation | 20+ | ~400 | Medium |
| Constants | 30+ | ~200 | Low |
| **Total** | **~70 files** | **~3600 lines** | **Critical** |

## Recommended Refactoring Order

1. **Phase 1 - Core Infrastructure** (1-2 days)
   - Standardize all imports using safe_import()
   - Choose single logging solution
   - Centralize all constants

2. **Phase 2 - Data Operations** (2-3 days)
   - Migrate all CSV operations to CSVManager
   - Standardize file operations
   - Consolidate validation patterns

3. **Phase 3 - Error & Flow Control** (1-2 days)
   - Implement consistent error handling
   - Standardize retry/rate limiting
   - Remove duplicate utility functions

4. **Phase 4 - Cleanup** (1 day)
   - Remove redundant file mapper utilities
   - Delete backup copies of duplicate code
   - Update all references