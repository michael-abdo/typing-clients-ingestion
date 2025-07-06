# DRY Module Consolidation Map

## Visual Guide: What Goes Where

```
┌─────────────────────────────────────────────────────────────┐
│                     CONSOLIDATED MODULES                      │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────┐
│ utils/path_setup.py │ ← Path & Directory Operations
├─────────────────────┤
│ • sys.path.insert() │
│ • os.makedirs()     │
│ • Project paths     │
│ • Directory creation│
└─────────────────────┘

┌─────────────────────┐
│ utils/csv_manager.py│ ← ALL CSV Operations
├─────────────────────┤
│ • CSV reading       │
│ • CSV writing       │
│ • Field sanitization│
│ • Integrity checks  │
│ • Atomic writes     │
│ • Streaming ops     │
└─────────────────────┘

┌──────────────────────────┐
│ utils/error_decorators.py│ ← Error Handling Patterns
├──────────────────────────┤
│ • File operations        │
│ • Network operations     │
│ • Validation errors      │
│ • Generic exceptions     │
└──────────────────────────┘

┌─────────────────────┐
│ utils/config.py     │ ← Configuration & Subprocess
├─────────────────────┤
│ • Config access     │
│ • Default values    │
│ • run_subprocess()* │ ← To be added
│ • Path configs      │
└─────────────────────┘

┌─────────────────────┐
│ utils/retry_utils.py│ ← Retry Logic
├─────────────────────┤
│ • Retry decorators  │
│ • Backoff strategies│
│ • Custom retries    │
└─────────────────────┘

┌──────────────────────┐
│ utils/import_utils.py│ ← Import Handling
├──────────────────────┤
│ • safe_import()      │
│ • Relative imports   │
│ • Module discovery   │
└──────────────────────┘
```

## Quick Reference: What to Import

### Setting up imports in any file:
```python
from utils.path_setup import init_project_imports
init_project_imports()
```

### Working with CSV files:
```python
from utils.csv_manager import CSVManager
csv_manager = CSVManager()
```

### Adding error handling:
```python
from utils.error_decorators import handle_file_operations

@handle_file_operations("my_operation")
def my_function():
    # code here
```

### Getting configuration:
```python
from utils.config import get_config
config = get_config()
```

### Adding retry logic:
```python
from utils.retry_utils import retry_with_backoff

@retry_with_backoff(max_retries=3)
def unreliable_operation():
    # code here
```

### Safe imports:
```python
from utils.import_utils import safe_import
MyClass = safe_import('module', 'MyClass')
```

## Migration Priority

1. **🔴 HIGH**: Path setup (sys.path.insert) → `utils/path_setup.py`
2. **🔴 HIGH**: CSV operations → `utils/csv_manager.py`  
3. **🟡 MEDIUM**: Error handling → `utils/error_decorators.py`
4. **🟡 MEDIUM**: Subprocess calls → `utils/config.py`
5. **🟢 LOW**: Directory creation → `utils/path_setup.py`
6. **🟢 LOW**: Retry logic → `utils/retry_utils.py`

## Remember

✅ **DO**: Use existing consolidated modules
❌ **DON'T**: Create new utility files
✅ **DO**: Extend existing modules when needed
❌ **DON'T**: Keep duplicate implementations