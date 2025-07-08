# 🎯 DRY REFACTORING PHASE 3: COMPREHENSIVE PROCEDURE

**Project**: typing-clients-ingestion  
**Target**: Eliminate remaining duplication patterns  
**Philosophy**: Never create new files - always extend existing ones  
**Approach**: Meticulous, step-by-step consolidation

---

## 📋 PROCEDURE OVERVIEW

This procedure builds on the successful Phase 1 & 2 refactoring and targets the remaining high-impact duplication patterns. Each step is designed to extend existing modules rather than create new ones.

---

## 🎯 PHASE 3A: IMPORT HANDLING CONSOLIDATION

### **STEP 1: Enhance utils/config.py with Import Management**

**Think Deeply**: 30+ files contain identical try/except ImportError blocks. The existing `utils/config.py` already has import utilities that can be enhanced to handle all import scenarios.

**Target Pattern**:
```python
# This pattern appears in 30+ files:
try:
    from module import function
except ImportError:
    from .module import function
```

**Chosen File to Absorb Change**: `utils/config.py` (lines 102-125)
- ✅ Already contains `safe_import()` function
- ✅ Central utility module imported by most files
- ✅ Logical place for import management

**Exact Update to utils/config.py**:

1. **Add after line 125**:
```python
def safe_relative_import(module_name: str, item_name: str, package: str = None):
    """
    Safely import with fallback to relative import.
    Replaces all try/except ImportError patterns in the codebase.
    
    Args:
        module_name: Name of module to import from
        item_name: Specific item to import
        package: Package for relative import (auto-detected if None)
    
    Returns:
        The imported item
    
    Example:
        # Instead of:
        # try:
        #     from csv_manager import CSVManager
        # except ImportError:
        #     from .csv_manager import CSVManager
        
        # Use:
        CSVManager = safe_relative_import('csv_manager', 'CSVManager')
    """
    try:
        # Try absolute import first
        module = __import__(module_name, fromlist=[item_name])
        return getattr(module, item_name)
    except ImportError:
        # Fall back to relative import
        if package is None:
            # Auto-detect package from caller's __name__
            import inspect
            frame = inspect.currentframe().f_back
            caller_module = frame.f_globals.get('__name__', '')
            if '.' in caller_module:
                package = caller_module.rsplit('.', 1)[0]
        
        try:
            relative_module_name = f'.{module_name}'
            module = __import__(relative_module_name, package=package, fromlist=[item_name])
            return getattr(module, item_name)
        except ImportError as e:
            raise ImportError(f"Could not import {item_name} from {module_name} (tried both absolute and relative imports): {e}")

def bulk_safe_import(import_specs: list, package: str = None):
    """
    Import multiple items safely with single function call.
    
    Args:
        import_specs: List of (module_name, item_name) tuples
        package: Package for relative imports
    
    Returns:
        Dictionary mapping item_name -> imported_item
    
    Example:
        imports = bulk_safe_import([
            ('csv_manager', 'CSVManager'),
            ('logging_config', 'get_logger'),
            ('config', 'get_config')
        ])
        CSVManager = imports['CSVManager']
        get_logger = imports['get_logger']
    """
    results = {}
    for module_name, item_name in import_specs:
        results[item_name] = safe_relative_import(module_name, item_name, package)
    return results
```

**Manual Check**:
```bash
# Test the enhanced import functionality
python3 -c "
from utils.config import safe_relative_import, bulk_safe_import
# Test single import
CSVManager = safe_relative_import('csv_manager', 'CSVManager')
print('✅ Single import works:', CSVManager.__name__)
# Test bulk import  
imports = bulk_safe_import([('csv_manager', 'CSVManager'), ('logging_config', 'get_logger')])
print('✅ Bulk import works:', len(imports), 'items imported')
"
```

---

### **STEP 2: Replace Import Patterns in High-Impact Files**

**Think Deeply**: Start with the most frequently imported modules to maximize impact. Focus on utils/ files first as they're imported by many other modules.

**Files to Update (Priority Order)**:

1. **utils/csv_file_integrity_mapper.py (lines 13-27)**

**Current Code**:
```python
try:
    from clean_file_mapper import CleanFileMapper
    from csv_manager import CSVManager
except ImportError:
    from .clean_file_mapper import CleanFileMapper
    from .csv_manager import CSVManager
```

**Replace with**:
```python
from utils.config import bulk_safe_import
imports = bulk_safe_import([
    ('clean_file_mapper', 'CleanFileMapper'),
    ('csv_manager', 'CSVManager')
])
CleanFileMapper = imports['CleanFileMapper']
CSVManager = imports['CSVManager']
```

**Manual Check**:
```bash
python3 -c "from utils.csv_file_integrity_mapper import CSVFileIntegrityMapper; print('✅ Import refactoring successful')"
```

2. **utils/comprehensive_file_mapper.py (lines 23-26)**

**Current Code**:
```python
try:
    from utils.clean_file_mapper import CleanFileMapper
except ImportError:
    from clean_file_mapper import CleanFileMapper
```

**Replace with**:
```python
from utils.config import safe_relative_import
CleanFileMapper = safe_relative_import('clean_file_mapper', 'CleanFileMapper')
```

**Manual Check**:
```bash
python3 -c "from utils.comprehensive_file_mapper import ComprehensiveFileMapper; print('✅ Import refactoring successful')"
```

3. **Continue for remaining 28 files with similar patterns**

---

## 🎯 PHASE 3B: CONFIGURATION CONSOLIDATION

### **STEP 3: Absorb Minimal Config into Main Config Class**

**Think Deeply**: The `minimal/simple_workflow.py` has a simplified Config class that duplicates some functionality from `utils/config.py`. Instead of maintaining separate configs, extend the main Config class to support minimal mode.

**Chosen File to Absorb Change**: `utils/config.py` (existing Config class)
- ✅ Already the authoritative configuration source
- ✅ Has YAML loading and environment management
- ✅ Used by production workflows

**Exact Update to utils/config.py**:

**Add after line 101**:
```python
    def get_minimal_config(self) -> dict:
        """
        Get minimal configuration subset for simple workflows.
        Absorbs functionality from minimal/simple_workflow.py Config class.
        
        Returns:
            Dictionary with minimal required settings
        """
        minimal_settings = {
            'csv_file': self.get('paths.output_csv', 'simple_output.csv'),
            'max_results': self.get('scraping.max_results', 100),
            'headless_browser': self.get('selenium.headless', True),
            'implicit_wait': self.get('selenium.implicit_wait', 10),
            'page_load_timeout': self.get('selenium.page_load_timeout', 30),
            'user_agent': self.get('selenium.user_agent', 'Mozilla/5.0 (compatible; DataExtractor/1.0)'),
            'download_timeout': self.get('downloads.timeout', 300),
            'retry_attempts': self.get('downloads.retry_attempts', 3),
            'rate_limit_delay': self.get('downloads.rate_limit_delay', 1.0)
        }
        return minimal_settings
    
    def get_selenium_options(self) -> dict:
        """
        Get Selenium Chrome options for minimal workflows.
        Absorbs selenium configuration from minimal files.
        
        Returns:
            Dictionary with Chrome options
        """
        options = {
            'headless': self.get('selenium.headless', True),
            'disable_gpu': self.get('selenium.disable_gpu', True),
            'no_sandbox': self.get('selenium.no_sandbox', True),
            'disable_dev_shm': self.get('selenium.disable_dev_shm_usage', True),
            'disable_extensions': self.get('selenium.disable_extensions', True),
            'window_size': self.get('selenium.window_size', '1920,1080'),
            'user_agent': self.get('selenium.user_agent', 'Mozilla/5.0 (compatible; DataExtractor/1.0)')
        }
        return options

# Add module-level convenience function
def get_minimal_config() -> dict:
    """Convenience function for minimal workflows"""
    config = get_config()
    return config.get_minimal_config()

def get_selenium_options() -> dict:
    """Convenience function for Selenium options"""
    config = get_config()
    return config.get_selenium_options()
```

**Manual Check**:
```bash
python3 -c "
from utils.config import get_minimal_config, get_selenium_options
minimal = get_minimal_config()
selenium = get_selenium_options()
print('✅ Minimal config keys:', len(minimal))
print('✅ Selenium options keys:', len(selenium))
print('✅ Configuration consolidation successful')
"
```

---

### **STEP 4: Update minimal/simple_workflow.py to Use Consolidated Config**

**Think Deeply**: Remove the duplicate Config class and replace with calls to the enhanced main config. This eliminates the duplication while maintaining the same interface.

**File to Update**: `minimal/simple_workflow.py`

**Remove lines 48-77** (entire Config class)

**Replace lines 78-81**:
```python
# Old code:
# config = Config()

# New code:
from utils.config import get_minimal_config, get_selenium_options
minimal_config = get_minimal_config()
selenium_options = get_selenium_options()
```

**Update all config references** (lines 90-500+):
```python
# Replace all instances of:
# config.csv_file -> minimal_config['csv_file']
# config.max_results -> minimal_config['max_results']
# config.headless_browser -> minimal_config['headless_browser']
# etc.
```

**Manual Check**:
```bash
python3 minimal/simple_workflow.py --help
python3 -c "
import sys
sys.path.append('minimal')
from simple_workflow import extract_links_from_sheet
print('✅ Config consolidation successful - simple_workflow still works')
"
```

---

## 🎯 PHASE 3C: LOGGING SYSTEM UNIFICATION

### **STEP 5: Absorb Pipeline Logging into Main Logging Module**

**Think Deeply**: Two separate logging systems exist with overlapping functionality. The `utils/logging_config.py` is more comprehensive and should absorb the specialized features from `utils/logger.py`.

**Chosen File to Absorb Change**: `utils/logging_config.py`
- ✅ More comprehensive logging infrastructure
- ✅ Already handles module-level logging
- ✅ Better integration with configuration system

**Exact Update to utils/logging_config.py**:

**Add after line 105**:
```python
# Absorb DualLogger functionality from utils/logger.py
class DualLogger:
    """
    Logger that outputs to both console and file.
    Absorbed from utils/logger.py for consolidation.
    """
    
    def __init__(self, name: str, file_path: str = None):
        self.console_logger = get_logger(f"{name}.console")
        if file_path:
            self.file_logger = get_logger(f"{name}.file")
            # Add file handler
            file_handler = logging.FileHandler(file_path)
            file_handler.setFormatter(logging.Formatter(
                '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            ))
            self.file_logger.addHandler(file_handler)
        else:
            self.file_logger = self.console_logger
    
    def info(self, message: str):
        self.console_logger.info(message)
        if self.file_logger != self.console_logger:
            self.file_logger.info(message)
    
    def error(self, message: str):
        self.console_logger.error(message)
        if self.file_logger != self.console_logger:
            self.file_logger.error(message)
    
    def warning(self, message: str):
        self.console_logger.warning(message)
        if self.file_logger != self.console_logger:
            self.file_logger.warning(message)

# Absorb PipelineLogger functionality
class PipelineLogger(DualLogger):
    """
    Enhanced logger for pipeline operations.
    Absorbed from utils/logger.py for consolidation.
    """
    
    def __init__(self, run_id: str):
        from utils.config import get_config
        config = get_config()
        
        self.run_id = run_id
        self.start_time = time.time()
        self.run_stats = {
            'rows_processed': 0,
            'youtube_downloads': 0,
            'drive_downloads': 0,
            'errors': 0
        }
        
        # Set up logging directory
        log_dir = config.get('logging.pipeline_dir', f'logs/runs/{run_id}')
        os.makedirs(log_dir, exist_ok=True)
        
        super().__init__('pipeline', f'{log_dir}/main.log')
        
        # Component loggers
        self.component_loggers = {
            'main': self,
            'scraper': DualLogger('scraper', f'{log_dir}/scraper.log'),
            'youtube': DualLogger('youtube', f'{log_dir}/youtube.log'),
            'drive': DualLogger('drive', f'{log_dir}/drive.log')
        }
    
    def get_logger(self, component: str = 'main'):
        return self.component_loggers.get(component, self)
    
    def update_stats(self, **kwargs):
        self.run_stats.update(kwargs)
    
    def log_subprocess(self, command, description=None, component='main'):
        logger = self.get_logger(component)
        if description:
            logger.info(f"Starting: {description}")
        logger.info(f"Command: {' '.join(command) if isinstance(command, list) else command}")
        
        # Execute subprocess with logging
        import subprocess
        try:
            result = subprocess.run(command, capture_output=True, text=True, check=True)
            logger.info(f"Success: {description or 'Command completed'}")
            if result.stdout:
                logger.info(f"Output: {result.stdout}")
            return 0
        except subprocess.CalledProcessError as e:
            logger.error(f"Failed: {description or 'Command failed'}")
            logger.error(f"Error: {e.stderr}")
            return e.returncode

# Context manager for pipeline runs  
@contextmanager
def pipeline_run(run_id: str = None):
    """
    Context manager for pipeline logging runs.
    Absorbed from utils/logger.py for consolidation.
    """
    if run_id is None:
        from datetime import datetime
        run_id = datetime.now().strftime('%Y-%m-%d_%H%M%S')
    
    pipeline_logger = PipelineLogger(run_id)
    pipeline_logger.info(f"Starting pipeline run: {run_id}")
    
    try:
        yield pipeline_logger
    except Exception as e:
        pipeline_logger.error(f"Pipeline run failed: {e}")
        raise
    finally:
        elapsed = time.time() - pipeline_logger.start_time
        pipeline_logger.info(f"Pipeline run completed in {elapsed:.2f}s")
        pipeline_logger.info(f"Final stats: {pipeline_logger.run_stats}")

# Backward compatibility functions
def get_pipeline_logger(run_id: str = None):
    """Backward compatibility function"""
    return PipelineLogger(run_id)

def get_dual_logger(name: str, file_path: str = None):
    """Backward compatibility function"""
    return DualLogger(name, file_path)
```

**Manual Check**:
```bash
python3 -c "
from utils.logging_config import pipeline_run, get_dual_logger
with pipeline_run() as logger:
    logger.info('Test pipeline logging')
    print('✅ Pipeline logging consolidated successfully')

dual = get_dual_logger('test')
dual.info('Test dual logging')
print('✅ Dual logging consolidated successfully')
"
```

---

### **STEP 6: Remove Redundant utils/logger.py**

**Think Deeply**: After absorbing all functionality into `logging_config.py`, the original `logger.py` becomes redundant. Remove it to eliminate duplication.

**Files to Update**:

1. **Remove file**: `utils/logger.py`
2. **Update imports** in files that import from logger.py:

**Files using utils/logger.py**:
- `run_complete_workflow.py` (line 13)

**Update run_complete_workflow.py line 13**:
```python
# Old:
from utils.logger import pipeline_run, get_pipeline_logger

# New:
from utils.logging_config import pipeline_run, get_pipeline_logger
```

**Manual Check**:
```bash
python3 run_complete_workflow.py --help
python3 -c "
from utils.logging_config import pipeline_run
print('✅ Logging consolidation successful - no more utils/logger.py needed')
"
```

---

## 🎯 PHASE 3D: SELENIUM DRIVER CONSOLIDATION

### **STEP 7: Consolidate Selenium Management in extraction_utils.py**

**Think Deeply**: Both `minimal/simple_workflow.py` and `minimal/extraction_utils.py` have identical Selenium driver setup. The `extraction_utils.py` version is more comprehensive and should absorb the functionality.

**Chosen File to Absorb Change**: `minimal/extraction_utils.py`
- ✅ More comprehensive Selenium implementation
- ✅ Already handles complex extraction scenarios
- ✅ Better error handling and cleanup

**Exact Update to minimal/extraction_utils.py**:

**Enhance existing functions at lines 22-51**:
```python
def get_selenium_driver(config_dict: dict = None, headless: bool = None):
    """
    Create and configure Chrome WebDriver.
    Enhanced to absorb configuration from simple_workflow.py
    
    Args:
        config_dict: Configuration dictionary (from get_minimal_config())
        headless: Override headless setting
    
    Returns:
        Configured WebDriver instance
    """
    if config_dict is None:
        # Use consolidated config
        from utils.config import get_minimal_config, get_selenium_options
        config_dict = get_minimal_config()
        selenium_config = get_selenium_options()
    else:
        selenium_config = config_dict
    
    options = webdriver.ChromeOptions()
    
    # Apply configuration options
    if headless is None:
        headless = selenium_config.get('headless', True)
    
    if headless:
        options.add_argument('--headless')
    
    # Enhanced options absorbed from simple_workflow.py
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--disable-extensions')
    options.add_argument(f"--window-size={selenium_config.get('window_size', '1920,1080')}")
    options.add_argument(f"--user-agent={selenium_config.get('user_agent', 'Mozilla/5.0 (compatible; DataExtractor/1.0)')}")
    
    # Create driver with enhanced configuration
    driver = webdriver.Chrome(options=options)
    
    # Apply timeouts from config
    driver.implicitly_wait(selenium_config.get('implicit_wait', 10))
    driver.set_page_load_timeout(selenium_config.get('page_load_timeout', 30))
    
    return driver

def cleanup_driver(driver, force_quit: bool = False):
    """
    Enhanced driver cleanup absorbed from simple_workflow.py
    
    Args:
        driver: WebDriver instance to cleanup
        force_quit: Force quit even if cleanup fails
    """
    if driver:
        try:
            # Close all windows
            for handle in driver.window_handles:
                driver.switch_to.window(handle)
                driver.close()
        except Exception as e:
            print(f"Warning: Error closing windows: {e}")
        
        try:
            driver.quit()
        except Exception as e:
            print(f"Warning: Error quitting driver: {e}")
            if force_quit:
                # Force kill Chrome processes if needed
                import subprocess
                try:
                    subprocess.run(['pkill', '-f', 'chrome'], check=False)
                except:
                    pass
```

**Manual Check**:
```bash
python3 -c "
import sys
sys.path.append('minimal')
from extraction_utils import get_selenium_driver, cleanup_driver
from utils.config import get_minimal_config

config = get_minimal_config()
driver = get_selenium_driver(config, headless=True)
cleanup_driver(driver)
print('✅ Selenium consolidation successful')
"
```

---

### **STEP 8: Remove Selenium Code from simple_workflow.py**

**Think Deeply**: After consolidating in `extraction_utils.py`, remove the duplicate code from `simple_workflow.py` and update references.

**File to Update**: `minimal/simple_workflow.py`

**Remove lines 106-135** (get_selenium_driver and cleanup_driver functions)

**Update references throughout the file**:
```python
# Add import at top:
from extraction_utils import get_selenium_driver, cleanup_driver

# All existing driver = get_selenium_driver() calls remain the same
# All existing cleanup_driver(driver) calls remain the same
```

**Manual Check**:
```bash
python3 minimal/simple_workflow.py --max-results 5
python3 -c "
import sys
sys.path.append('minimal')
from simple_workflow import extract_links_from_sheet
print('✅ Selenium deduplication successful - simple_workflow still works')
"
```

---

## 🎯 PHASE 3E: URL PROCESSING CONSOLIDATION

### **STEP 9: Absorb URL Processing into utils/validation.py**

**Think Deeply**: URL cleaning and validation logic is scattered across multiple files. The `utils/validation.py` already handles comprehensive validation and should absorb all URL processing.

**Chosen File to Absorb Change**: `utils/validation.py`
- ✅ Already handles comprehensive URL validation
- ✅ Used by production workflows
- ✅ Has robust error handling

**Exact Update to utils/validation.py**:

**Add after existing validation functions**:
```python
def clean_url(url: str) -> str:
    """
    Clean and normalize URLs.
    Absorbed from minimal workflow files for consolidation.
    
    Args:
        url: Raw URL to clean
    
    Returns:
        Cleaned URL
    """
    if not url or not isinstance(url, str):
        return ""
    
    url = url.strip()
    
    # Handle Google redirect URLs
    if 'google.com/url?q=' in url:
        url = extract_actual_url(url)
    
    # Ensure protocol
    if url and not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    # Basic URL validation
    if not is_valid_url(url):
        return ""
    
    return url

def extract_actual_url(google_redirect_url: str) -> str:
    """
    Extract actual URL from Google redirect.
    Absorbed from minimal files for consolidation.
    
    Args:
        google_redirect_url: Google redirect URL
    
    Returns:
        Actual destination URL
    """
    try:
        from urllib.parse import parse_qs, urlparse
        parsed = urlparse(google_redirect_url)
        if 'q' in parse_qs(parsed.query):
            return parse_qs(parsed.query)['q'][0]
    except Exception:
        pass
    return google_redirect_url

def extract_youtube_links(text: str) -> list:
    """
    Extract YouTube links from text.
    Absorbed from minimal extraction utilities.
    
    Args:
        text: Text to search for YouTube links
    
    Returns:
        List of YouTube URLs found
    """
    import re
    youtube_patterns = [
        r'https?://(?:www\.)?youtube\.com/watch\?v=[\w-]+',
        r'https?://youtu\.be/[\w-]+',
        r'https?://(?:www\.)?youtube\.com/playlist\?list=[\w-]+',
        r'https?://(?:www\.)?youtube\.com/channel/[\w-]+',
        r'https?://(?:www\.)?youtube\.com/@[\w-]+'
    ]
    
    links = []
    for pattern in youtube_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        links.extend(matches)
    
    return list(set(links))  # Remove duplicates

def extract_drive_links(text: str) -> list:
    """
    Extract Google Drive links from text.
    Absorbed from minimal extraction utilities.
    
    Args:
        text: Text to search for Drive links
    
    Returns:
        List of Google Drive URLs found
    """
    import re
    drive_patterns = [
        r'https?://drive\.google\.com/file/d/[\w-]+',
        r'https?://drive\.google\.com/open\?id=[\w-]+',
        r'https?://drive\.google\.com/drive/folders/[\w-]+',
        r'https?://docs\.google\.com/document/d/[\w-]+',
        r'https?://docs\.google\.com/spreadsheets/d/[\w-]+',
        r'https?://docs\.google\.com/presentation/d/[\w-]+'
    ]
    
    links = []
    for pattern in drive_patterns:
        matches = re.findall(pattern, text, re.IGNORECASE)
        links.extend(matches)
    
    return list(set(links))  # Remove duplicates

def extract_all_links(text: str) -> dict:
    """
    Extract all types of links from text.
    Consolidated link extraction functionality.
    
    Args:
        text: Text to search for links
    
    Returns:
        Dictionary with 'youtube' and 'drive' link lists
    """
    return {
        'youtube': extract_youtube_links(text),
        'drive': extract_drive_links(text),
        'all': extract_youtube_links(text) + extract_drive_links(text)
    }
```

**Manual Check**:
```bash
python3 -c "
from utils.validation import clean_url, extract_all_links, extract_actual_url

# Test URL cleaning
clean = clean_url('google.com/url?q=https://youtube.com/watch?v=test')
print('✅ URL cleaning works:', clean)

# Test link extraction
text = 'Check out https://youtube.com/watch?v=abc and https://drive.google.com/file/d/123'
links = extract_all_links(text)
print('✅ Link extraction works:', len(links['all']), 'links found')
"
```

---

### **STEP 10: Remove URL Processing from Minimal Files**

**Think Deeply**: After consolidating in `validation.py`, remove duplicate URL processing from minimal files and update imports.

**Files to Update**:

1. **minimal/simple_workflow.py**

**Remove functions** (lines 295-347):
- `clean_url()`
- `extract_actual_url()`

**Remove functions** (lines 349-428):
- `extract_youtube_links()`  
- `extract_drive_links()`
- `extract_all_links()`

**Add import at top**:
```python
from utils.validation import clean_url, extract_all_links, extract_actual_url
```

2. **minimal/extraction_utils.py**

**Remove duplicate functions** (lines 69-121, 201-281):
- Similar URL and link extraction functions

**Add import at top**:
```python
from utils.validation import clean_url, extract_all_links, extract_youtube_links, extract_drive_links
```

**Manual Check**:
```bash
python3 minimal/simple_workflow.py --max-results 5
python3 -c "
import sys
sys.path.append('minimal')
from simple_workflow import extract_links_from_sheet
print('✅ URL processing consolidation successful')
"
```

---

## 🎯 PHASE 3F: DOWNLOAD RESULT CONSOLIDATION

### **STEP 11: Enhance utils/row_context.py with Download Patterns**

**Think Deeply**: The `download_drive.py` and `download_youtube.py` files contain similar patterns for creating download results and managing metadata. The `row_context.py` already defines `DownloadResult` and should absorb these patterns.

**Chosen File to Absorb Change**: `utils/row_context.py`
- ✅ Already defines DownloadResult class
- ✅ Central location for row/context management
- ✅ Imported by download modules

**Exact Update to utils/row_context.py**:

**Add after line 45**:
```python
def create_success_result(file_path: str, row_context: 'RowContext', metadata: dict = None) -> DownloadResult:
    """
    Create a successful download result with consistent metadata.
    Absorbed from download modules for consolidation.
    
    Args:
        file_path: Path to downloaded file
        row_context: Row context information
        metadata: Additional metadata dictionary
    
    Returns:
        DownloadResult instance
    """
    result_metadata = {
        'row_id': row_context.row_id,
        'person_name': row_context.name,
        'person_type': row_context.type,
        'download_timestamp': datetime.now().isoformat(),
        'file_path': file_path,
        'file_size': os.path.getsize(file_path) if os.path.exists(file_path) else 0
    }
    
    if metadata:
        result_metadata.update(metadata)
    
    return DownloadResult(
        success=True,
        file_path=file_path,
        metadata=result_metadata,
        error_message=None,
        status='completed'
    )

def create_failure_result(error_message: str, row_context: 'RowContext', metadata: dict = None) -> DownloadResult:
    """
    Create a failed download result with consistent error handling.
    Absorbed from download modules for consolidation.
    
    Args:
        error_message: Error description
        row_context: Row context information
        metadata: Additional metadata dictionary
    
    Returns:
        DownloadResult instance
    """
    result_metadata = {
        'row_id': row_context.row_id,
        'person_name': row_context.name,
        'person_type': row_context.type,
        'error_timestamp': datetime.now().isoformat(),
        'error_details': error_message
    }
    
    if metadata:
        result_metadata.update(metadata)
    
    return DownloadResult(
        success=False,
        file_path=None,
        metadata=result_metadata,
        error_message=error_message,
        status='failed'
    )

def save_result_metadata(result: DownloadResult, output_dir: str) -> bool:
    """
    Save download result metadata to JSON file.
    Absorbed from download modules for consolidation.
    
    Args:
        result: DownloadResult instance
        output_dir: Directory to save metadata
    
    Returns:
        True if saved successfully, False otherwise
    """
    try:
        if not result.metadata:
            return False
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Create metadata filename
        row_id = result.metadata.get('row_id', 'unknown')
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        metadata_file = os.path.join(output_dir, f'metadata_row{row_id}_{timestamp}.json')
        
        with open(metadata_file, 'w') as f:
            json.dump(result.metadata, f, indent=2)
        
        return True
    except Exception as e:
        print(f"Warning: Could not save metadata: {e}")
        return False

@contextmanager
def download_context(row_context: 'RowContext', download_type: str = 'unknown'):
    """
    Context manager for download operations with automatic result creation.
    Provides consistent error handling and result generation.
    
    Args:
        row_context: Row context information
        download_type: Type of download ('youtube', 'drive', etc.)
    
    Yields:
        Dictionary with helper functions for result creation
    
    Example:
        with download_context(row_context, 'youtube') as ctx:
            # Download logic here
            file_path = download_file()
            return ctx['success'](file_path, {'extra': 'metadata'})
    """
    try:
        helpers = {
            'success': lambda file_path, metadata=None: create_success_result(file_path, row_context, metadata),
            'failure': lambda error_msg, metadata=None: create_failure_result(error_msg, row_context, metadata),
            'row_context': row_context,
            'download_type': download_type
        }
        yield helpers
    except Exception as e:
        # If exception occurs in context, create failure result
        yield {
            'success': lambda *args: create_failure_result(f"Context error: {e}", row_context),
            'failure': lambda error_msg, metadata=None: create_failure_result(error_msg, row_context, metadata),
            'row_context': row_context,
            'download_type': download_type
        }
```

**Manual Check**:
```bash
python3 -c "
from utils.row_context import RowContext, create_success_result, create_failure_result, download_context
import tempfile

# Test result creation
row_ctx = RowContext('1', 'Test', 'test@example.com', 'person', 'http://test.com')

# Test success result
with tempfile.NamedTemporaryFile() as tf:
    success = create_success_result(tf.name, row_ctx)
    print('✅ Success result created:', success.success)

# Test failure result
failure = create_failure_result('Test error', row_ctx)
print('✅ Failure result created:', not failure.success)

# Test context manager
with download_context(row_ctx, 'test') as ctx:
    result = ctx['failure']('Test context error')
    print('✅ Download context works:', not result.success)
"
```

---

## 🎯 PHASE 3G: CLEANUP SCRIPT CONSOLIDATION

### **STEP 12: Consolidate Cleanup Scripts into cleanup_manager.py**

**Think Deeply**: Multiple cleanup scripts (`simple_cleanup.py`, `temp_cleanup.py`, `execute_cleanup.py`, `final_cleanup.py`, `final_remove.py`) have overlapping functionality. The existing `utils/cleanup_manager.py` should absorb all cleanup operations.

**Chosen File to Absorb Change**: `utils/cleanup_manager.py`
- ✅ Already exists and handles cleanup operations
- ✅ Proper utility module location
- ✅ Can be extended with additional cleanup modes

**Exact Update to utils/cleanup_manager.py**:

**Add after existing cleanup methods**:
```python
def simple_cleanup(self) -> bool:
    """
    Simple cleanup operations.
    Absorbed from simple_cleanup.py for consolidation.
    
    Returns:
        True if successful
    """
    try:
        # Remove temporary files
        temp_patterns = [
            '*.tmp', '*.temp', '*.part', '*.ytdl',
            'temp_*', 'tmp_*', '.DS_Store'
        ]
        
        for pattern in temp_patterns:
            for file_path in glob.glob(pattern):
                if os.path.isfile(file_path):
                    os.remove(file_path)
                    self.logger.info(f"Removed temporary file: {file_path}")
        
        return True
    except Exception as e:
        self.logger.error(f"Simple cleanup failed: {e}")
        return False

def temp_cleanup(self) -> bool:
    """
    Temporary directory cleanup.
    Absorbed from temp_cleanup.py for consolidation.
    
    Returns:
        True if successful
    """
    try:
        import tempfile
        temp_dir = tempfile.gettempdir()
        
        # Clean project-related temp files
        temp_patterns = [
            os.path.join(temp_dir, 'selenium*'),
            os.path.join(temp_dir, 'chrome*'),
            os.path.join(temp_dir, 'tmp*csv*'),
        ]
        
        for pattern in temp_patterns:
            for file_path in glob.glob(pattern):
                try:
                    if os.path.isfile(file_path):
                        os.remove(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                    self.logger.info(f"Cleaned temp item: {file_path}")
                except Exception as e:
                    self.logger.warning(f"Could not clean {file_path}: {e}")
        
        return True
    except Exception as e:
        self.logger.error(f"Temp cleanup failed: {e}")
        return False

def final_cleanup(self) -> bool:
    """
    Final cleanup operations.
    Absorbed from final_cleanup.py for consolidation.
    
    Returns:
        True if successful
    """
    try:
        # Remove backup files older than 30 days
        cutoff_time = time.time() - (30 * 24 * 60 * 60)  # 30 days
        
        backup_patterns = [
            '*.backup', '*.bak', '*~',
            'backups/**/*', 'logs/runs/**/summary.json'
        ]
        
        for pattern in backup_patterns:
            for file_path in glob.glob(pattern, recursive=True):
                try:
                    if os.path.isfile(file_path):
                        if os.path.getmtime(file_path) < cutoff_time:
                            os.remove(file_path)
                            self.logger.info(f"Removed old backup: {file_path}")
                except Exception as e:
                    self.logger.warning(f"Could not remove backup {file_path}: {e}")
        
        return True
    except Exception as e:
        self.logger.error(f"Final cleanup failed: {e}")
        return False

def comprehensive_cleanup(self) -> bool:
    """
    Run all cleanup operations in sequence.
    
    Returns:
        True if all cleanups successful
    """
    results = []
    
    self.logger.info("Starting comprehensive cleanup...")
    
    results.append(self.simple_cleanup())
    results.append(self.temp_cleanup())
    results.append(self.cleanup_download_files())  # existing method
    results.append(self.final_cleanup())
    
    success = all(results)
    
    if success:
        self.logger.info("Comprehensive cleanup completed successfully")
    else:
        self.logger.warning("Some cleanup operations failed")
    
    return success

# Add command-line interface
def main():
    """Command-line interface for cleanup operations"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Consolidated cleanup utility')
    parser.add_argument('--simple', action='store_true', help='Run simple cleanup')
    parser.add_argument('--temp', action='store_true', help='Run temp cleanup')
    parser.add_argument('--final', action='store_true', help='Run final cleanup')
    parser.add_argument('--all', action='store_true', help='Run comprehensive cleanup')
    
    args = parser.parse_args()
    
    cleanup = CleanupManager()
    
    if args.simple:
        cleanup.simple_cleanup()
    elif args.temp:
        cleanup.temp_cleanup()
    elif args.final:
        cleanup.final_cleanup()
    elif args.all:
        cleanup.comprehensive_cleanup()
    else:
        print("No cleanup mode specified. Use --help for options.")

if __name__ == "__main__":
    main()
```

**Manual Check**:
```bash
python3 -c "
from utils.cleanup_manager import CleanupManager
cleanup = CleanupManager()
result = cleanup.simple_cleanup()
print('✅ Cleanup consolidation successful:', result)
"

python3 utils/cleanup_manager.py --help
```

---

### **STEP 13: Remove Redundant Cleanup Scripts**

**Think Deeply**: After consolidating all cleanup functionality, remove the redundant script files to eliminate duplication.

**Files to Remove**:
1. `simple_cleanup.py`
2. `temp_cleanup.py`
3. `execute_cleanup.py`
4. `final_cleanup.py`
5. `final_remove.py`

**Backup and Remove**:
```bash
# Create archive of removed scripts
mkdir -p removed_scripts_archive
mv simple_cleanup.py temp_cleanup.py execute_cleanup.py final_cleanup.py final_remove.py removed_scripts_archive/
```

**Update any references** in documentation or other scripts to point to:
```bash
python3 utils/cleanup_manager.py --all
```

**Manual Check**:
```bash
python3 utils/cleanup_manager.py --simple
python3 utils/cleanup_manager.py --all
echo "✅ Cleanup script consolidation successful"
```

---

## 🎯 PHASE 3H: FILE CLEANUP AND DOCUMENTATION

### **STEP 14: Remove Backup and Archive Files**

**Think Deeply**: Numerous backup files and archive directories are no longer needed and create clutter. Clean them up systematically.

**Backup Directories to Remove**:
- `backup/contaminated_utilities_20250630_121306/`
- `minimal/dry_refactoring_backup_20250706_010126/`
- `scripts/archive/`

**Individual Backup Files to Remove**:
```bash
find . -name "*.backup" -type f
find . -name "*.phase2*.backup" -type f
find . -name "*_20250*" -type f  # Old dated backups
```

**Systematic Removal**:
```bash
# Remove archive directories
rm -rf backup/contaminated_utilities_20250630_121306/
rm -rf minimal/dry_refactoring_backup_20250706_010126/  
rm -rf scripts/archive/

# Remove old backup files
find . -name "*.backup" -not -path "./removed_scripts_archive/*" -delete
find . -name "*.phase2*.backup" -delete

# Keep only essential backups in outputs/ directory
```

**Manual Check**:
```bash
find . -name "*.backup" | wc -l  # Should be minimal
du -sh backup/ minimal/ scripts/  # Should be smaller
echo "✅ Backup cleanup successful"
```

---

### **STEP 15: Update Documentation and Comments**

**Think Deeply**: After all consolidation, update documentation to reflect the new unified architecture and remove references to deleted files.

**Files to Update**:

1. **README.md** - Update architecture section
2. **CONSOLIDATED_MODULES_USAGE_GUIDE.md** - Add Phase 3 consolidations
3. **utils/ module docstrings** - Update references

**Update README.md** (Architecture section):
```markdown
## Unified Architecture (Post-DRY Refactoring)

### Core Modules (Single Source of Truth)
- `utils/config.py` - All configuration management (including minimal configs)
- `utils/logging_config.py` - All logging functionality (console, file, pipeline)
- `utils/csv_manager.py` - All CSV operations (reading, writing, atomic operations)
- `utils/validation.py` - All validation and URL processing
- `utils/row_context.py` - All download result and context management
- `utils/cleanup_manager.py` - All cleanup operations
- `minimal/extraction_utils.py` - All Selenium and extraction functionality

### Import Management
All modules use `safe_relative_import()` from `utils/config.py` instead of try/except blocks.

### Deprecated/Removed Modules
- `utils/logger.py` → Consolidated into `logging_config.py`
- `simple_cleanup.py`, `temp_cleanup.py`, etc. → Consolidated into `cleanup_manager.py`
- Duplicate URL/link processing → Consolidated into `validation.py`
- Minimal config classes → Consolidated into main `config.py`
```

**Update module docstrings** to reflect consolidation:

**utils/config.py**:
```python
"""
Unified Configuration Management

This module serves as the single source of truth for all configuration:
- Main application configuration (YAML-based)
- Minimal workflow configuration (absorbed from minimal/simple_workflow.py)
- Selenium options and browser configuration
- Import management with safe_relative_import()

Replaces: Scattered configuration classes and import handling throughout codebase
"""
```

**utils/logging_config.py**:
```python
"""
Unified Logging System

This module provides all logging functionality:
- Module-level logging (get_logger)
- Pipeline logging (PipelineLogger, absorbed from utils/logger.py)
- Dual logging (console + file, absorbed from utils/logger.py)
- Context managers for pipeline runs

Replaces: utils/logger.py and scattered logging setups
"""
```

**Manual Check**:
```bash
# Verify documentation is up to date
grep -r "utils/logger.py" . --include="*.md" || echo "✅ No stale logger references"
grep -r "simple_cleanup.py" . --include="*.md" || echo "✅ No stale cleanup references"
echo "✅ Documentation updated successfully"
```

---

## 🎯 VERIFICATION AND TESTING

### **STEP 16: Comprehensive Testing After Consolidation**

**Think Deeply**: After all consolidation, run comprehensive tests to ensure no functionality was broken and all integrations work correctly.

**Test Sequence**:

1. **Import Tests**:
```bash
python3 -c "
# Test all consolidated imports
from utils.config import safe_relative_import, get_minimal_config
from utils.logging_config import pipeline_run, get_logger
from utils.validation import clean_url, extract_all_links
from utils.row_context import create_success_result, download_context
from utils.cleanup_manager import CleanupManager

print('✅ All consolidated imports successful')
"
```

2. **Integration Tests**:
```bash
python3 -c "
# Test integrated functionality
from utils.config import get_minimal_config
from utils.logging_config import pipeline_run

config = get_minimal_config()
with pipeline_run() as logger:
    logger.info('Testing integrated systems')
    print('✅ Config + Logging integration successful')
"
```

3. **Workflow Tests**:
```bash
python3 run_complete_workflow.py --help
python3 run_all_tests.py --quick
python3 minimal/simple_workflow.py --help
```

4. **Performance Validation**:
```bash
python3 -c "
import time
from utils.csv_manager import CSVManager
from utils.config import get_minimal_config

start = time.time()
config = get_minimal_config()
manager = CSVManager('outputs/output.csv')
elapsed = time.time() - start

print(f'✅ Performance maintained: {elapsed:.3f}s startup time')
"
```

**Success Criteria**:
- All imports work without errors
- No functionality regressions
- Performance is maintained or improved
- Documentation is accurate and complete

---

## 📊 EXPECTED RESULTS

### **Quantitative Impact**:
- **Files Consolidated**: 15+ additional files
- **Lines Reduced**: ~800-1000 lines of duplicate code
- **Import Statements Simplified**: 100+ files
- **Configuration Centralized**: 3 config sources → 1
- **Logging Unified**: 2 logging systems → 1
- **Cleanup Scripts**: 5 scripts → 1 module

### **Qualitative Benefits**:
- Single source of truth for all utilities
- Consistent error handling across codebase
- Simplified debugging and maintenance
- Easier onboarding for new developers
- Reduced cognitive load when working with code
- Future changes require updates in only one place

### **Risk Mitigation**:
- Each step is independently testable
- Changes build on existing successful patterns
- Backward compatibility maintained during transition
- Easy rollback via git branch isolation
- Comprehensive verification at each step

---

## 🏁 COMPLETION CHECKLIST

- [ ] **STEP 1**: Enhanced utils/config.py with import management
- [ ] **STEP 2**: Replaced import patterns in high-impact files
- [ ] **STEP 3**: Absorbed minimal config into main Config class
- [ ] **STEP 4**: Updated minimal/simple_workflow.py config usage
- [ ] **STEP 5**: Absorbed pipeline logging into logging_config.py
- [ ] **STEP 6**: Removed redundant utils/logger.py
- [ ] **STEP 7**: Consolidated Selenium in extraction_utils.py
- [ ] **STEP 8**: Removed Selenium duplication from simple_workflow.py
- [ ] **STEP 9**: Absorbed URL processing into validation.py
- [ ] **STEP 10**: Removed URL processing from minimal files
- [ ] **STEP 11**: Enhanced row_context.py with download patterns
- [ ] **STEP 12**: Consolidated cleanup scripts into cleanup_manager.py
- [ ] **STEP 13**: Removed redundant cleanup scripts
- [ ] **STEP 14**: Cleaned up backup and archive files
- [ ] **STEP 15**: Updated documentation and comments
- [ ] **STEP 16**: Comprehensive testing and verification

**Upon completion, the codebase will have achieved maximum DRY compliance with:**
- Zero redundant files
- Single source of truth for all utilities
- Consistent patterns throughout
- Comprehensive documentation
- Full backward compatibility

---

*This procedure ensures meticulous consolidation while maintaining system integrity and avoiding any new file creation.*