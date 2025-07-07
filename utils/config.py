"""Configuration management module for centralized settings."""
import os
import yaml
import importlib
from pathlib import Path
from typing import Dict, Any, Optional, Union, List, Iterator
import logging

# Singleton pattern for configuration
_config = None
_config_path = None

class Config:
    """Configuration manager that loads settings from YAML file."""
    
    def __init__(self, config_path: Optional[str] = None):
        """
        Initialize configuration from YAML file.
        
        Args:
            config_path: Path to config file. If None, looks for config.yaml in project root.
        """
        if config_path is None:
            # Look for config.yaml in config directory (relative to project root)
            utils_dir = Path(__file__).parent
            project_root = utils_dir.parent
            config_path = project_root / "config" / "config.yaml"
        
        self.config_path = Path(config_path)
        self._data = {}
        self._load_config()
    
    def _load_config(self):
        """Load configuration from YAML file."""
        if not self.config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {self.config_path}")
        
        with open(self.config_path, 'r') as f:
            self._data = yaml.safe_load(f)
    
    def get(self, key_path: str, default: Any = None) -> Any:
        """
        Get configuration value using dot notation.
        
        Args:
            key_path: Dot-separated path to config value (e.g., "downloads.youtube.max_workers")
            default: Default value if key not found
        
        Returns:
            Configuration value or default
        """
        keys = key_path.split('.')
        value = self._data
        
        for key in keys:
            if isinstance(value, dict) and key in value:
                value = value[key]
            else:
                return default
        
        return value
    
    def get_section(self, section: str) -> Dict[str, Any]:
        """
        Get entire configuration section.
        
        Args:
            section: Section name (e.g., "downloads", "retry")
        
        Returns:
            Dictionary containing section configuration
        """
        return self._data.get(section, {})
    
    def reload(self):
        """Reload configuration from file."""
        self._load_config()
    
    @property
    def all(self) -> Dict[str, Any]:
        """Get all configuration data."""
        return self._data.copy()


def get_config(config_path: Optional[str] = None) -> Config:
    """
    Get singleton configuration instance.
    
    Args:
        config_path: Path to config file (only used on first call)
    
    Returns:
        Configuration instance
    """
    global _config, _config_path
    
    if _config is None or (config_path and config_path != _config_path):
        _config = Config(config_path)
        _config_path = config_path
    
    return _config


# Convenience functions for common config values
def get_youtube_downloads_dir() -> str:
    """Get YouTube downloads directory."""
    return get_config().get("paths.youtube_downloads", "youtube_downloads")


def get_drive_downloads_dir() -> str:
    """Get Drive downloads directory."""
    return get_config().get("paths.drive_downloads", "drive_downloads")


def get_output_csv_path() -> str:
    """Get output CSV file path."""
    return get_config().get("paths.output_csv", "simple_output.csv")


def get_google_sheets_url() -> str:
    """Get Google Sheets URL."""
    return get_config().get("google_sheets.url", "https://docs.google.com/spreadsheets/u/1/d/e/2PACX-1vRqqjqoaj8sEZBfZRw0Og7g8ms_0yTL2MsegTubcjhhBnXr1s1jFBwIVAsbkyj1xD0TMj06LvGTQIHU/pubhtml?pli=1#")


def get_target_div_id() -> str:
    """Get target div ID for Google Sheets parsing."""
    return get_config().get("google_sheets.target_div_id", "1159146182")


def get_database_path() -> str:
    """Get database file path."""
    return get_config().get("database.path", "xenodx.db")


def get_batch_size() -> int:
    """Get processing batch size."""
    return get_config().get("processing.batch_size", 10)


def get_max_retries() -> int:
    """Get maximum retry attempts."""
    return get_config().get("processing.max_retries", 3)


def get_progress_file() -> str:
    """Get progress tracking file path."""
    return get_config().get("files.progress", "extraction_progress.json")


def get_failed_docs_file() -> str:
    """Get failed documents tracking file path."""
    return get_config().get("files.failed_docs", "failed_extractions.json")


def get_retry_config() -> Dict[str, Any]:
    """Get retry configuration."""
    return get_config().get_section("retry")


# DRY: CLI Argument Utilities (Phase 5 - consolidates argparse patterns across 33+ files)
import argparse
from typing import List, Optional, Callable


class StandardCLIArguments:
    """Standardized CLI argument groups to eliminate duplication across scripts"""
    
    @staticmethod
    def add_file_arguments(parser: argparse.ArgumentParser, 
                          include_csv: bool = True,
                          include_output: bool = True, 
                          include_directory: bool = False) -> None:
        """Add standardized file path arguments"""
        if include_csv:
            parser.add_argument('--csv', '--csv-path', default='outputs/output.csv',
                              help='Path to CSV file (default: outputs/output.csv)')
        if include_output:
            parser.add_argument('--output', '--output-dir',
                              help='Output directory or file path')
        if include_directory:
            parser.add_argument('--directory', 
                              help='Working directory')
    
    @staticmethod 
    def add_processing_arguments(parser: argparse.ArgumentParser,
                               include_max_rows: bool = True,
                               include_batch_size: bool = False,
                               include_reset: bool = True,
                               include_dry_run: bool = False) -> None:
        """Add standardized processing control arguments"""
        if include_max_rows:
            parser.add_argument('--max-rows', type=int,
                              help='Maximum number of rows to process')
        if include_batch_size:
            parser.add_argument('--batch-size', type=int, default=10,
                              help='Process N items per batch (default: 10)')
        if include_reset:
            parser.add_argument('--reset', action='store_true',
                              help='Reset processing status and reprocess')
        if include_dry_run:
            parser.add_argument('--dry-run', action='store_true',
                              help='Test mode without making changes')
    
    @staticmethod
    def add_download_arguments(parser: argparse.ArgumentParser,
                             include_youtube: bool = True,
                             include_drive: bool = True,
                             include_skip_options: bool = True) -> None:
        """Add standardized download control arguments"""
        if include_youtube:
            parser.add_argument('--max-youtube', type=int,
                              help='Maximum YouTube videos to download')
        if include_drive:
            parser.add_argument('--max-drive', type=int,
                              help='Maximum Drive files to download')
        if include_skip_options:
            parser.add_argument('--skip-youtube', action='store_true',
                              help='Skip YouTube downloads')
            parser.add_argument('--skip-drive', action='store_true',  
                              help='Skip Google Drive downloads')
    
    @staticmethod
    def add_workflow_arguments(parser: argparse.ArgumentParser,
                             include_sheet: bool = True,
                             include_logging: bool = True,
                             include_cache: bool = False) -> None:
        """Add standardized workflow control arguments"""
        if include_sheet:
            parser.add_argument('--skip-sheet', action='store_true',
                              help='Skip Google Sheet scraping')
        if include_logging:
            parser.add_argument('--no-logging', action='store_true',
                              help='Disable logging to files')
        if include_cache:
            parser.add_argument('--use-cache', action='store_true',
                              help='Use cached data instead of downloading')
    
    @staticmethod
    def add_monitoring_arguments(parser: argparse.ArgumentParser,
                               include_status: bool = True,
                               include_detailed: bool = True,
                               include_alerts: bool = False) -> None:
        """Add standardized monitoring and reporting arguments"""
        if include_status:
            parser.add_argument('--status', action='store_true',
                              help='Show current system status')
        if include_detailed:
            parser.add_argument('--detailed', action='store_true',
                              help='Include detailed information')
        if include_alerts:
            parser.add_argument('--alerts', action='store_true',
                              help='Check alert conditions')


def get_standard_components(module_name: str):
    """
    Get standard logger and config for a module
    DRY: Consolidates duplicate initialization patterns across 16+ files
    
    Args:
        module_name: Usually __name__ from the calling module
        
    Returns:
        tuple: (logger, config)
    """
    from utils.logging_config import get_logger
    logger = get_logger(module_name)
    config = get_config()
    return logger, config

def setup_csv_environment():
    """
    Setup CSV processing environment with proper field size limits
    DRY: Consolidates duplicate CSV setup across multiple scripts
    """
    import csv
    import sys
    config = get_config()
    csv.field_size_limit(config.get('file_processing.max_csv_field_size', sys.maxsize))

def read_csv_rows(csv_file_path: str, encoding: str = 'utf-8', start_row: int = 1):
    """
    Read CSV file rows with standardized error handling and encoding
    DRY: Consolidates duplicate CSV reading patterns across 10+ files
    
    Args:
        csv_file_path: Path to CSV file
        encoding: File encoding (default: utf-8)
        start_row: Row number to start enumeration from (default: 1)
        
    Yields:
        tuple: (row_number, row_dict) for each row in CSV
        
    Example:
        for row_num, row in read_csv_rows('output.csv', start_row=2):
            print(f"Row {row_num}: {row['name']}")
    """
    import csv
    import os
    
    if not os.path.exists(csv_file_path):
        raise FileNotFoundError(f"CSV file not found: {csv_file_path}")
    
    try:
        with open(csv_file_path, 'r', encoding=encoding) as f:
            reader = csv.DictReader(f)
            for row_num, row in enumerate(reader, start=start_row):
                yield row_num, row
    except Exception as e:
        raise Exception(f"Error reading CSV file {csv_file_path}: {str(e)}")

def read_csv_mapping(csv_file_path: str, key_field: str, encoding: str = 'utf-8'):
    """
    Read CSV file and create a mapping dictionary
    DRY: Consolidates duplicate CSV mapping patterns
    
    Args:
        csv_file_path: Path to CSV file
        key_field: Field to use as dictionary key
        encoding: File encoding (default: utf-8)
        
    Returns:
        dict: Mapping of key_field values to row dictionaries
        
    Example:
        mapping = read_csv_mapping('output.csv', 'row_id')
        person = mapping.get('496')  # Get row by row_id
    """
    mapping = {}
    for row_num, row in read_csv_rows(csv_file_path, encoding):
        key = row.get(key_field)
        if key:
            if key not in mapping:
                mapping[key] = []
            mapping[key].append(row)
    return mapping

def parse_file_size_from_html(html_content: str, target_unit: str = 'GB') -> float:
    """
    Parse file size from Google Drive HTML content (e.g., '4.8G', '100M', '2K')
    DRY: Consolidates duplicate logic from download_large_drive_files.py and download_small_drive_files.py
    
    Args:
        html_content: HTML containing size info like "(4.8G)"
        target_unit: Target unit ('GB', 'MB', 'KB')
    
    Returns:
        Size in target unit, or 0 if not found
    """
    import re
    size_match = re.search(r'\(([0-9.]+)([GMK])\)', html_content)
    
    if not size_match:
        return 0
        
    size_value = float(size_match.group(1))
    size_unit = size_match.group(2)
    
    # Convert to bytes first
    if size_unit == 'G':
        bytes_value = size_value * 1024 * 1024 * 1024
    elif size_unit == 'M':
        bytes_value = size_value * 1024 * 1024
    elif size_unit == 'K':
        bytes_value = size_value * 1024
    else:
        return 0
    
    # Convert to target unit
    if target_unit.upper() == 'GB':
        return bytes_value / (1024 * 1024 * 1024)
    elif target_unit.upper() == 'MB':
        return bytes_value / (1024 * 1024)
    elif target_unit.upper() == 'KB':
        return bytes_value / 1024
    else:
        return bytes_value

def create_standard_parser(description: str, 
                         argument_groups: Optional[List[str]] = None) -> argparse.ArgumentParser:
    """
    Create a parser with standardized argument groups
    
    Args:
        description: Parser description
        argument_groups: List of argument groups to include:
            - 'files': File path arguments (--csv, --output, --directory)
            - 'processing': Processing control (--max-rows, --reset, --dry-run)
            - 'downloads': Download control (--max-youtube, --max-drive, --skip-*)
            - 'workflow': Workflow control (--skip-sheet, --no-logging)
            - 'monitoring': Monitoring/reporting (--status, --detailed, --alerts)
    
    Returns:
        ArgumentParser with requested argument groups
        
    Example:
        parser = create_standard_parser(
            "Download YouTube videos",
            ['files', 'processing', 'downloads']
        )
    """
    parser = argparse.ArgumentParser(description=description)
    
    if argument_groups:
        for group in argument_groups:
            if group == 'files':
                StandardCLIArguments.add_file_arguments(parser)
            elif group == 'processing':
                StandardCLIArguments.add_processing_arguments(parser)
            elif group == 'downloads':
                StandardCLIArguments.add_download_arguments(parser)
            elif group == 'workflow':
                StandardCLIArguments.add_workflow_arguments(parser)
            elif group == 'monitoring':
                StandardCLIArguments.add_monitoring_arguments(parser)
    
    return parser


def validate_standard_arguments(args: argparse.Namespace) -> bool:
    """
    Validate common argument combinations and constraints
    
    Args:
        args: Parsed arguments namespace
        
    Returns:
        True if arguments are valid, False otherwise
    """
    # Validate positive integer arguments
    for attr in ['max_rows', 'max_youtube', 'max_drive', 'batch_size']:
        value = getattr(args, attr, None)
        if value is not None and value <= 0:
            print(f"Error: --{attr.replace('_', '-')} must be positive")
            return False
    
    # Validate file paths exist if specified
    csv_path = getattr(args, 'csv', None) or getattr(args, 'csv_path', None)
    if csv_path and not Path(csv_path).exists():
        print(f"Warning: CSV file not found: {csv_path}")
    
    return True


def execute_with_standard_args(main_func: Callable, 
                             parser: argparse.ArgumentParser,
                             validate_args: bool = True) -> int:
    """
    Execute main function with standardized argument handling
    
    Args:
        main_func: Main function to execute, should accept args parameter
        parser: Configured ArgumentParser
        validate_args: Whether to validate arguments
        
    Returns:
        Exit code (0 for success, 1 for failure)
    """
    try:
        args = parser.parse_args()
        
        if validate_args and not validate_standard_arguments(args):
            return 1
        
        result = main_func(args)
        return 0 if result is None else result
        
    except KeyboardInterrupt:
        print("\nOperation cancelled by user")
        return 1
    except Exception as e:
        print(f"Error: {e}")
        return 1


def get_timeout(timeout_type: str = "default") -> float:
    """
    Get timeout value.
    
    Args:
        timeout_type: Type of timeout (default, file_lock, http_request, etc.)
    
    Returns:
        Timeout value in seconds
    """
    return get_config().get(f"timeouts.{timeout_type}", 30.0)


def create_download_dir(download_dir: str, logger=None) -> Path:
    """
    Create download directory if it doesn't exist.
    
    Args:
        download_dir: Directory path to create
        logger: Optional logger instance
    
    Returns:
        Path object for the directory
    """
    downloads_path = Path(download_dir)
    if not downloads_path.exists():
        downloads_path.mkdir(parents=True)
        if logger:
            logger.info(f"Created downloads directory: {download_dir}")
    return downloads_path

def ensure_directory_exists(directory_path: Union[str, Path], logger=None) -> Path:
    """
    Ensure directory exists, creating it if necessary
    DRY: Consolidates duplicate directory creation patterns from multiple files
    
    Args:
        directory_path: Path to directory (string or Path object)
        logger: Optional logger instance
        
    Returns:
        Path object for the directory
        
    Example:
        data_dir = ensure_directory_exists('data/downloads')
        cache_dir = ensure_directory_exists(Path('cache'))
    """
    dir_path = Path(directory_path)
    if not dir_path.exists():
        dir_path.mkdir(parents=True, exist_ok=True)
        if logger:
            logger.info(f"Created directory: {directory_path}")
    return dir_path

def validate_file_exists(file_path: Union[str, Path], error_message: str = None) -> Path:
    """
    Validate that a file exists, raising FileNotFoundError if not
    DRY: Consolidates duplicate file existence validation patterns
    
    Args:
        file_path: Path to file (string or Path object)  
        error_message: Custom error message (optional)
        
    Returns:
        Path object for the file
        
    Raises:
        FileNotFoundError: If file does not exist
        
    Example:
        csv_file = validate_file_exists('data.csv', 'Input CSV file not found')
    """
    path = Path(file_path)
    if not path.exists():
        if error_message:
            raise FileNotFoundError(error_message)
        else:
            raise FileNotFoundError(f"File not found: {file_path}")
    return path

def safe_file_check(file_path: Union[str, Path]) -> bool:
    """
    Safely check if file exists without raising exceptions
    DRY: Consolidates duplicate file existence check patterns
    
    Args:
        file_path: Path to file (string or Path object)
        
    Returns:
        bool: True if file exists, False otherwise
        
    Example:
        if safe_file_check('optional_config.json'):
            load_config()
    """
    try:
        return Path(file_path).exists()
    except (OSError, ValueError):
        return False

def safe_execute(func: Callable, default_return: Any = None, log_errors: bool = True, 
                operation_name: str = "operation") -> Any:
    """
    Safely execute a function with standardized error handling
    DRY: Consolidates duplicate try/except patterns across files
    
    Args:
        func: Function to execute
        default_return: Value to return on error (default: None)
        log_errors: Whether to log errors (default: True)
        operation_name: Name for logging (default: 'operation')
        
    Returns:
        Function result or default_return on error
        
    Example:
        result = safe_execute(lambda: risky_operation(), 
                            default_return=[], 
                            operation_name="data loading")
    """
    try:
        return func()
    except Exception as e:
        if log_errors:
            print(f"Error in {operation_name}: {str(e)}")
        return default_return

def retry_operation(func: Callable, max_attempts: int = 3, delay: float = 1.0, 
                   operation_name: str = "operation") -> Any:
    """
    Retry an operation with simple exponential backoff
    DRY: Consolidates duplicate retry logic patterns
    
    Args:
        func: Function to retry
        max_attempts: Maximum number of attempts (default: 3)
        delay: Base delay between attempts (default: 1.0 seconds)
        operation_name: Name for logging
        
    Returns:
        Function result
        
    Raises:
        Exception: Last exception if all attempts fail
        
    Example:
        result = retry_operation(lambda: api_call(), 
                               max_attempts=5, 
                               operation_name="API request")
    """
    import time
    last_exception = None
    
    for attempt in range(max_attempts):
        try:
            return func()
        except Exception as e:
            last_exception = e
            if attempt < max_attempts - 1:
                wait_time = delay * (2 ** attempt)  # Exponential backoff
                print(f"{operation_name} attempt {attempt + 1} failed: {str(e)}")
                print(f"Retrying in {wait_time:.1f} seconds...")
                time.sleep(wait_time)
            else:
                print(f"{operation_name} failed after {max_attempts} attempts")
                raise last_exception

def extract_actual_url(google_url: str) -> str:
    """
    Extract the actual URL from a Google redirect URL
    DRY: Consolidates duplicate URL extraction from validation.py and scrape_google_sheets.py
    
    Args:
        google_url: Google redirect URL or regular URL
        
    Returns:
        str: Actual URL without redirect wrapper
        
    Example:
        actual = extract_actual_url('https://www.google.com/url?q=https://example.com&sa=U')
        # Returns: 'https://example.com'
    """
    from urllib.parse import urlparse, parse_qs, unquote
    
    if 'google.com/url?' in google_url:
        try:
            parsed = urlparse(google_url)
            params = parse_qs(parsed.query)
            if 'q' in params and params['q']:
                actual_url = params['q'][0]
                # URL decode and return
                return unquote(actual_url)
        except Exception:
            pass
    return google_url

def extract_file_id(url: str) -> Optional[str]:
    """
    Extract Google Drive file ID from various URL formats
    DRY: Consolidates file ID extraction logic from download_drive.py
    
    Args:
        url: Google Drive URL in any format
        
    Returns:
        str: File ID if found, None otherwise
        
    Example:
        file_id = extract_file_id('https://drive.google.com/file/d/ABC123/view')
        # Returns: 'ABC123'
    """
    import re
    from urllib.parse import urlparse, parse_qs
    
    # Pattern for different Google Drive URL formats
    patterns = [
        r'/file/d/([a-zA-Z0-9_-]+)',  # /file/d/{fileId}
        r'id=([a-zA-Z0-9_-]+)',       # id={fileId}
        r'drive.google.com/open\?id=([a-zA-Z0-9_-]+)'  # open?id={fileId}
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    # Try parsing the URL query parameters
    try:
        parsed_url = urlparse(url)
        query_params = parse_qs(parsed_url.query)
        
        # Check various possible parameter names
        for param in ['id', 'file_id', 'fileId', 'docid']:
            if param in query_params:
                return query_params[param][0]
    except Exception:
        pass
    
    return None

def extract_youtube_id(url: str) -> Optional[str]:
    """
    Extract YouTube video ID from various URL formats
    DRY: Consolidates YouTube ID extraction patterns
    
    Args:
        url: YouTube URL in any format
        
    Returns:
        str: Video ID if found, None otherwise
        
    Example:
        video_id = extract_youtube_id('https://www.youtube.com/watch?v=ABC123')
        # Returns: 'ABC123'
    """
    import re
    
    # YouTube URL patterns
    patterns = [
        r'(?:youtube\.com/watch\?v=|youtu\.be/)([a-zA-Z0-9_-]{11})',
        r'youtube\.com/embed/([a-zA-Z0-9_-]{11})',
        r'youtube\.com/v/([a-zA-Z0-9_-]{11})'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    return None

def log_progress(current: int, total: int, item_name: str = "item", operation: str = "Processing", logger=None):
    """
    Log progress information in standardized format
    DRY: Consolidates duplicate progress logging patterns
    
    Args:
        current: Current item number (1-based)
        total: Total number of items
        item_name: Name of item being processed (default: 'item')
        operation: Operation being performed (default: 'Processing')
        logger: Logger instance (optional, prints if None)
        
    Example:
        log_progress(5, 100, "file", "Processing", logger)
        # Logs: "Processing file 5/100"
    """
    progress_msg = f"{operation} {item_name} {current}/{total}"
    if logger:
        logger.info(progress_msg)
    else:
        print(progress_msg)

def log_completion_summary(total: int, successful: int, operation: str = "Processed", 
                          item_name: str = "items", logger=None):
    """
    Log completion summary in standardized format
    DRY: Consolidates duplicate completion logging patterns
    
    Args:
        total: Total number of items processed
        successful: Number of successful operations
        operation: Past tense operation name (default: 'Processed')
        item_name: Name of items (default: 'items')
        logger: Logger instance (optional, prints if None)
        
    Example:
        log_completion_summary(100, 95, "Downloaded", "files", logger)
        # Logs: "Downloaded 100 files, 95 successful downloads"
    """
    if successful == total:
        summary_msg = f"{operation} {total} {item_name}, all successful"
    else:
        failed = total - successful
        summary_msg = f"{operation} {total} {item_name}, {successful} successful, {failed} failed"
    
    if logger:
        logger.info(summary_msg)
    else:
        print(summary_msg)

def create_progress_tracker(total_items: int, operation: str = "Processing", 
                           item_name: str = "item", logger=None):
    """
    Create a progress tracking function for iterative operations
    DRY: Consolidates duplicate progress tracking patterns
    
    Args:
        total_items: Total number of items to process
        operation: Operation being performed
        item_name: Name of item type
        logger: Logger instance
        
    Returns:
        tuple: (progress_function, completion_function)
        
    Example:
        track_progress, log_completion = create_progress_tracker(100, "Downloading", "file", logger)
        for i, file in enumerate(files, 1):
            track_progress(i)
            process_file(file)
        log_completion(successful_count)
    """
    def track_progress(current: int):
        log_progress(current, total_items, item_name, operation, logger)
    
    def log_completion(successful: int):
        log_completion_summary(total_items, successful, operation.replace("ing", "ed"), 
                             item_name + "s" if not item_name.endswith("s") else item_name, 
                             logger)
    
    return track_progress, log_completion

def setup_download_directories(base_dir: str, subdirs: List[str] = None, logger=None) -> Dict[str, Path]:
    """
    Set up download directory structure with standardized subdirectories
    DRY: Consolidates duplicate download directory setup patterns
    
    Args:
        base_dir: Base download directory path
        subdirs: List of subdirectory names (default: ['files', 'metadata', 'temp'])
        logger: Logger instance
        
    Returns:
        dict: Mapping of subdirectory names to Path objects
        
    Example:
        dirs = setup_download_directories('downloads', ['files', 'logs'])
        files_dir = dirs['files']
        logs_dir = dirs['logs']
    """
    if subdirs is None:
        subdirs = ['files', 'metadata', 'temp']
    
    base_path = ensure_directory_exists(base_dir, logger)
    
    dir_mapping = {'base': base_path}
    for subdir in subdirs:
        subdir_path = ensure_directory_exists(base_path / subdir, logger)
        dir_mapping[subdir] = subdir_path
    
    return dir_mapping

def safe_file_move(source: Union[str, Path], destination: Union[str, Path], 
                  backup_suffix: str = ".bak", logger=None) -> bool:
    """
    Safely move a file with backup and error handling
    DRY: Consolidates duplicate file move patterns with error handling
    
    Args:
        source: Source file path
        destination: Destination file path
        backup_suffix: Suffix for backup file (default: '.bak')
        logger: Logger instance
        
    Returns:
        bool: True if successful, False otherwise
        
    Example:
        success = safe_file_move('temp.csv', 'output.csv', logger=logger)
    """
    import shutil
    import os
    
    source_path = Path(source)
    dest_path = Path(destination)
    
    try:
        # Create destination directory if needed
        ensure_directory_exists(dest_path.parent, logger)
        
        # Create backup if destination exists
        if dest_path.exists():
            backup_path = dest_path.with_suffix(dest_path.suffix + backup_suffix)
            shutil.copy2(dest_path, backup_path)
            if logger:
                logger.info(f"Created backup: {backup_path}")
        
        # Move the file
        shutil.move(str(source_path), str(dest_path))
        if logger:
            logger.info(f"Moved {source_path} to {dest_path}")
        
        return True
        
    except Exception as e:
        if logger:
            logger.error(f"Failed to move {source_path} to {dest_path}: {str(e)}")
        return False

def safe_file_rename(file_path: Union[str, Path], new_name: str, logger=None) -> Optional[Path]:
    """
    Safely rename a file with error handling
    DRY: Consolidates duplicate file rename patterns
    
    Args:
        file_path: Current file path
        new_name: New filename (not full path)
        logger: Logger instance
        
    Returns:
        Path: New file path if successful, None otherwise
        
    Example:
        new_path = safe_file_rename('old_name.txt', 'new_name.txt', logger)
    """
    import os
    
    try:
        current_path = Path(file_path)
        new_path = current_path.parent / new_name
        
        # Avoid overwriting existing files
        if new_path.exists() and new_path != current_path:
            counter = 1
            base_name = Path(new_name).stem
            extension = Path(new_name).suffix
            while new_path.exists():
                new_name = f"{base_name}_{counter}{extension}"
                new_path = current_path.parent / new_name
                counter += 1
            
            if logger:
                logger.warning(f"File exists, renamed to: {new_name}")
        
        os.rename(str(current_path), str(new_path))
        if logger:
            logger.info(f"Renamed {current_path.name} to {new_path.name}")
        
        return new_path
        
    except Exception as e:
        if logger:
            logger.error(f"Failed to rename {file_path}: {str(e)}")
        return None

def cleanup_temp_files(directory: Union[str, Path], patterns: List[str] = None, 
                      age_hours: int = 24, logger=None) -> int:
    """
    Clean up temporary files based on patterns and age
    DRY: Consolidates duplicate temp file cleanup patterns
    
    Args:
        directory: Directory to clean
        patterns: File patterns to match (default: ['*.tmp', '*.temp', '*.crdownload'])
        age_hours: Delete files older than this many hours
        logger: Logger instance
        
    Returns:
        int: Number of files deleted
        
    Example:
        deleted = cleanup_temp_files('downloads', ['*.tmp', '*.log'], age_hours=1)
    """
    import glob
    import time
    import os
    
    if patterns is None:
        patterns = ['*.tmp', '*.temp', '*.crdownload', '*.part']
    
    dir_path = Path(directory)
    if not dir_path.exists():
        return 0
    
    deleted_count = 0
    current_time = time.time()
    age_seconds = age_hours * 3600
    
    for pattern in patterns:
        for file_path in dir_path.glob(pattern):
            try:
                if file_path.is_file():
                    file_age = current_time - file_path.stat().st_mtime
                    if file_age > age_seconds:
                        file_path.unlink()
                        deleted_count += 1
                        if logger:
                            logger.debug(f"Deleted temp file: {file_path}")
            except Exception as e:
                if logger:
                    logger.warning(f"Failed to delete {file_path}: {str(e)}")
    
    if deleted_count > 0 and logger:
        logger.info(f"Cleaned up {deleted_count} temporary files from {directory}")
    
    return deleted_count


def get_download_chunk_size(file_size: int) -> int:
    """
    Get appropriate chunk size based on file size.
    
    Args:
        file_size: Size of file in bytes
    
    Returns:
        Chunk size in bytes
    """
    config = get_config()
    thresholds = config.get_section("downloads").get("drive", {})
    
    if file_size > thresholds.get("size_thresholds", {}).get("large", 104857600):
        return thresholds.get("chunk_sizes", {}).get("large", 8388608)
    elif file_size > thresholds.get("size_thresholds", {}).get("medium", 10485760):
        return thresholds.get("chunk_sizes", {}).get("medium", 2097152)
    else:
        return thresholds.get("chunk_sizes", {}).get("small", 1048576)


def get_parallel_config() -> Dict[str, Any]:
    """Get parallel processing configuration."""
    return get_config().get_section("parallel")


def get_logging_config() -> Dict[str, Any]:
    """Get logging configuration."""
    return get_config().get_section("logging")


def is_ssl_verify_enabled() -> bool:
    """Check if SSL verification is enabled."""
    return get_config().get("security.ssl_verify", True)


def get_allowed_domains(service: str) -> list:
    """
    Get allowed domains for a service.
    
    Args:
        service: Service name (youtube, drive)
    
    Returns:
        List of allowed domains
    """
    return get_config().get(f"security.allowed_domains.{service}", [])


def get_streaming_threshold() -> int:
    """Get file size threshold for streaming operations."""
    return get_config().get("file_processing.streaming_threshold", 5242880)


def create_chrome_driver(config_type: str = "extraction", download_dir: Optional[str] = None, headless: bool = True):
    """
    Create Chrome WebDriver with consolidated configuration
    DRY: Eliminates duplicate Chrome setup from extraction_utils.py and download_drive_files_from_html.py
    
    Args:
        config_type: Type of configuration ('extraction', 'download')
        download_dir: Download directory (required for download config)
        headless: Whether to run in headless mode
        
    Returns:
        Configured Chrome WebDriver instance
    """
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options
    from selenium.webdriver.chrome.service import Service
    import time
    import subprocess
    
    chrome_options = Options()
    
    # Common options for both configurations
    if headless:
        chrome_options.add_argument("--headless")
    
    # Anti-detection measures (common to both)
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
    chrome_options.add_experimental_option('useAutomationExtension', False)
    
    # Realistic browser behavior
    chrome_options.add_argument("--user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
    chrome_options.add_argument("--window-size=1920,1080")
    
    # Performance and stability (common to both)
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-extensions")
    
    if config_type == "extraction":
        # Configuration for document extraction (from extraction_utils.py)
        chrome_options.add_argument("--start-maximized")
        chrome_options.add_argument("--disable-plugins")
        chrome_options.add_argument("--remote-debugging-port=0")
        chrome_options.add_argument("--no-first-run")
        chrome_options.add_argument("--disable-default-apps")
        chrome_options.add_argument("--disable-background-timer-throttling")
        chrome_options.add_argument("--disable-renderer-backgrounding")
        chrome_options.add_argument("--disable-backgrounding-occluded-windows")
        chrome_options.add_argument("--enable-javascript")
        chrome_options.add_argument("--disable-web-security")
        chrome_options.add_argument("--allow-running-insecure-content")
        
    elif config_type == "download":
        # Configuration for file downloads (from download_drive_files_from_html.py)
        if not download_dir:
            raise ValueError("download_dir is required for download configuration")
            
        # Set download directory and preferences
        prefs = {
            "download.default_directory": str(Path(download_dir).absolute()),
            "download.prompt_for_download": False,
            "download.directory_upgrade": True,
            "safebrowsing.enabled": True,
            "safebrowsing.disable_download_protection": True,
        }
        chrome_options.add_experimental_option("prefs", prefs)
        
    # Clean up any existing Chrome processes
    try:
        subprocess.run(['pkill', '-f', 'chrome'], capture_output=True)
        time.sleep(1)
    except:
        pass
    
    # DRY: Use consolidated retry logic with decorator pattern
    # Import retry decorator at function level to avoid circular imports
    from .retry_utils import retry_with_backoff
    
    @retry_with_backoff(max_attempts=3, base_delay=2.0, logger=None)
    def _create_driver():
        # Use ChromeDriver service
        service = Service('/usr/bin/chromedriver')
        chrome_options.binary_location = '/usr/bin/chromium-browser'
        
        driver = webdriver.Chrome(service=service, options=chrome_options)
        
        # Additional anti-detection measures for extraction
        if config_type == "extraction":
            driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
            driver.execute_cdp_cmd('Network.setUserAgentOverride', {
                "userAgent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
            })
        
        # Enable automatic downloads for download configuration
        elif config_type == "download":
            driver.implicitly_wait(10)
            driver.execute_cdp_cmd("Page.setDownloadBehavior", {
                "behavior": "allow",
                "downloadPath": str(Path(download_dir).absolute())
            })
        
        return driver
    
    return _create_driver()


def get_minimal_config():
    """
    Get minimal workflow configuration (DRY: absorbed from minimal/simple_workflow.py).
    
    Returns:
        Object with all minimal workflow configuration constants
    """
    from pathlib import Path
    
    class MinimalConfig:
        # Google Sheets configuration
        GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/u/1/d/e/2PACX-1vRqqjqoaj8sEZBfZRw0Og7g8ms_0yTL2MsegTubcjhhBnXr1s1jFBwIVAsbkyj1xD0TMj06LvGTQIHU/pubhtml?pli=1#"
        TARGET_DIV_ID = "1159146182"
        OUTPUT_DIR = Path("simple_downloads")
        OUTPUT_CSV_FULL = "simple_output.csv"
        OUTPUT_CSV_BASIC = "simple_output.csv"  # Unified output file
        
        # Database configuration (DRY: moved from simple_workflow_db.py)
        DATABASE_PATH = "xenodx.db"
        USE_DATABASE = False  # Default to CSV mode for backward compatibility
        OUTPUT_FORMAT = "csv"  # Options: "csv", "db", "both"
        
        # Processing modes
        BASIC_ONLY = False  # True = extract only basic columns, False = full processing
        TEXT_ONLY = False   # True = extract only text (basic + document_text), False = full processing
        TEST_LIMIT = None   # None = process all, int = limit for testing
        
        # Batch processing settings
        BATCH_SIZE = 10     # Process N documents at a time
        RETRY_ATTEMPTS = 3  # Retry failed extractions N times
        DELAY_BETWEEN_DOCS = 2  # Seconds between document extractions
        
        # Row range filtering
        START_ROW = None    # Start from row N (inclusive)
        END_ROW = None      # End at row N (inclusive)
        
        # Incremental processing (DRY: moved from simple_workflow_db.py)
        SKIP_PROCESSED = True
        RETRY_FAILED = False
        
        # Progress tracking
        PROGRESS_FILE = "extraction_progress.json"
        FAILED_DOCS_FILE = "failed_extractions.json"
    
    return MinimalConfig()


def safe_import(module_names: Union[str, List[str]], from_items: Optional[Union[str, List[str]]] = None, 
                package: Optional[str] = None) -> Any:
    """
    Centralized import management with automatic fallback for relative/absolute imports.
    
    Eliminates the need for try/except ImportError blocks throughout the codebase.
    
    Args:
        module_names: Module name(s) to import from
        from_items: Specific items to import (functions, classes, etc.)
        package: Package name for relative imports
    
    Returns:
        Imported module or specific items
        
    Examples:
        # Import entire module
        csv_tracker = safe_import('csv_tracker')
        
        # Import specific functions
        reset_func = safe_import('csv_tracker', 'reset_all_download_status')
        
        # Import multiple items
        funcs = safe_import('csv_tracker', ['reset_all_download_status', 'ensure_tracking_columns'])
    """
    if isinstance(module_names, str):
        module_names = [module_names]
    if isinstance(from_items, str):
        from_items = [from_items]
    
    last_error = None
    
    # Try absolute import first
    for module_name in module_names:
        try:
            module = importlib.import_module(module_name)
            if from_items:
                if len(from_items) == 1:
                    return getattr(module, from_items[0])
                else:
                    return [getattr(module, item) for item in from_items]
            return module
        except ImportError as e:
            last_error = e
            continue
    
    # Try relative import as fallback
    if package:
        for module_name in module_names:
            try:
                module = importlib.import_module(f'.{module_name}', package=package)
                if from_items:
                    if len(from_items) == 1:
                        return getattr(module, from_items[0])
                    else:
                        return [getattr(module, item) for item in from_items]
                return module
            except ImportError as e:
                last_error = e
                continue
    
    # If all imports fail, raise the last error
    raise last_error


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
            if package:
                module = __import__(relative_module_name, fromlist=[item_name])
            else:
                module = __import__(relative_module_name, fromlist=[item_name])
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

def universal_import(module_specs: List[tuple], package: str = None, 
                    return_dict: bool = True):
    """
    Universal import handler eliminating all try/except ImportError patterns
    DRY: Consolidates 60+ lines of duplicate import patterns across 20+ files
    
    Args:
        module_specs: List of (module_name, item_name) tuples. item_name=None imports entire module
        package: Package name for relative imports (auto-detected if None)
        return_dict: If True returns dict, if False returns list in spec order
        
    Returns:
        Dict mapping names to imported items or list in specification order
        
    Example:
        # Replace all try/except ImportError blocks with:
        imports = universal_import([
            ('config', 'get_config'),
            ('logging_config', 'get_logger'),
            ('csv_manager', None)  # Import entire module
        ])
        get_config = imports['get_config']
        get_logger = imports['get_logger'] 
        csv_manager = imports['csv_manager']
        
        # Or as list:
        get_config, get_logger, csv_manager = universal_import([...], return_dict=False)
    """
    if package is None:
        # Auto-detect package from caller
        import inspect
        frame = inspect.currentframe().f_back
        caller_module = frame.f_globals.get('__name__', '')
        if '.' in caller_module:
            package = caller_module.rsplit('.', 1)[0]
    
    results = {} if return_dict else []
    
    for module_name, item_name in module_specs:
        try:
            # Try absolute import first
            module = importlib.import_module(module_name)
            imported_item = getattr(module, item_name) if item_name else module
        except ImportError:
            try:
                # Fall back to relative import
                relative_module_name = f'.{module_name}'
                module = importlib.import_module(relative_module_name, package=package)
                imported_item = getattr(module, item_name) if item_name else module
            except ImportError as e:
                item_desc = f"{module_name}.{item_name}" if item_name else module_name
                raise ImportError(f"Cannot import {item_desc} (tried absolute and relative): {e}")
        
        if return_dict:
            key = item_name if item_name else module_name
            results[key] = imported_item
        else:
            results.append(imported_item)
    
    return results

def simple_import(module_name: str, item_name: str = None, package: str = None) -> Any:
    """
    Simple import function for single items - shorthand for universal_import
    DRY: Eliminates individual try/except ImportError blocks
    
    Args:
        module_name: Module to import from
        item_name: Specific item to import (None = whole module)
        package: Package for relative imports
        
    Returns:
        Imported item or module
        
    Example:
        # Instead of:
        # try:
        #     from config import get_config
        # except ImportError:
        #     from .config import get_config
        
        # Use:
        get_config = simple_import('config', 'get_config')
    """
    result = universal_import([(module_name, item_name)], package=package, return_dict=False)
    return result[0]

def safe_json_save(data: Any, file_path: Union[str, Path], indent: int = 2, 
                  backup: bool = True, logger=None) -> bool:
    """
    Standardized JSON saving with error handling and backup
    DRY: Consolidates 36+ lines of duplicate JSON save patterns across 12+ files
    
    Args:
        data: Data to save as JSON
        file_path: Path to save JSON file
        indent: JSON indentation (default: 2)
        backup: Create backup if file exists (default: True)
        logger: Logger instance
        
    Returns:
        bool: True if successful, False otherwise
        
    Example:
        # Replace all instances of:
        # with open('file.json', 'w') as f:
        #     json.dump(data, f, indent=2)
        
        # With:
        success = safe_json_save(data, 'file.json', logger=logger)
    """
    import json
    import shutil
    
    try:
        file_path = Path(file_path)
        
        # Create directory if needed
        ensure_directory_exists(file_path.parent, logger)
        
        # Create backup if file exists
        if backup and file_path.exists():
            backup_path = file_path.with_suffix(file_path.suffix + '.bak')
            shutil.copy2(file_path, backup_path)
            if logger:
                logger.debug(f"Created backup: {backup_path}")
        
        # Write JSON atomically using temp file
        temp_path = file_path.with_suffix(file_path.suffix + '.tmp')
        with open(temp_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent, ensure_ascii=False)
        
        # Atomic move to final location
        temp_path.rename(file_path)
        
        if logger:
            logger.debug(f"Saved JSON: {file_path}")
        
        return True
        
    except Exception as e:
        if logger:
            logger.error(f"Failed to save JSON to {file_path}: {str(e)}")
        return False

def safe_json_load(file_path: Union[str, Path], default: Any = None, 
                  logger=None, encoding: str = 'utf-8') -> Any:
    """
    Standardized JSON loading with error handling and fallback
    DRY: Consolidates duplicate JSON load patterns across multiple files
    
    Args:
        file_path: Path to JSON file
        default: Default value if loading fails (default: None)
        logger: Logger instance
        encoding: File encoding (default: utf-8)
        
    Returns:
        Loaded data or default value
        
    Example:
        # Replace:
        # try:
        #     with open('file.json', 'r') as f:
        #         data = json.load(f)
        # except:
        #     data = {}
        
        # With:
        data = safe_json_load('file.json', default={}, logger=logger)
    """
    import json
    
    try:
        file_path = Path(file_path)
        
        if not file_path.exists():
            if logger:
                logger.debug(f"JSON file not found: {file_path}, returning default")
            return default
        
        with open(file_path, 'r', encoding=encoding) as f:
            data = json.load(f)
        
        if logger:
            logger.debug(f"Loaded JSON: {file_path}")
        
        return data
        
    except Exception as e:
        if logger:
            logger.warning(f"Failed to load JSON from {file_path}: {str(e)}, returning default")
        return default

def atomic_json_update(file_path: Union[str, Path], update_func: Callable, 
                      default: Any = None, logger=None) -> bool:
    """
    Atomic JSON file updates with rollback on failure
    DRY: Consolidates complex JSON update patterns
    
    Args:
        file_path: Path to JSON file
        update_func: Function that takes current data and returns updated data
        default: Default data if file doesn't exist (default: None)
        logger: Logger instance
        
    Returns:
        bool: True if successful, False otherwise
        
    Example:
        # Update JSON file atomically
        def add_item(data):
            if data is None:
                data = []
            data.append({'new': 'item'})
            return data
        
        success = atomic_json_update('items.json', add_item, default=[], logger=logger)
    """
    try:
        # Load current data
        current_data = safe_json_load(file_path, default=default, logger=logger)
        
        # Apply update function
        updated_data = update_func(current_data)
        
        # Save updated data
        return safe_json_save(updated_data, file_path, logger=logger)
        
    except Exception as e:
        if logger:
            logger.error(f"Failed to update JSON {file_path}: {str(e)}")
        return False

def merge_json_files(file_paths: List[Union[str, Path]], output_path: Union[str, Path], 
                    merge_strategy: str = 'update', logger=None) -> bool:
    """
    Merge multiple JSON files into one with configurable strategy
    DRY: Consolidates JSON merge patterns
    
    Args:
        file_paths: List of JSON files to merge
        output_path: Output file path
        merge_strategy: 'update' (dict.update), 'extend' (list.extend), 'custom'
        logger: Logger instance
        
    Returns:
        bool: True if successful, False otherwise
        
    Example:
        success = merge_json_files(['file1.json', 'file2.json'], 'merged.json')
    """
    try:
        if merge_strategy == 'update':
            result = {}
            for file_path in file_paths:
                data = safe_json_load(file_path, default={}, logger=logger)
                if isinstance(data, dict):
                    result.update(data)
        elif merge_strategy == 'extend':
            result = []
            for file_path in file_paths:
                data = safe_json_load(file_path, default=[], logger=logger)
                if isinstance(data, list):
                    result.extend(data)
        else:
            raise ValueError(f"Unsupported merge strategy: {merge_strategy}")
        
        return safe_json_save(result, output_path, logger=logger)
        
    except Exception as e:
        if logger:
            logger.error(f"Failed to merge JSON files: {str(e)}")
        return False

def format_error(operation: str, item: str, error: Exception, context: dict = None, 
                include_traceback: bool = False) -> str:
    """
    Standardized error message formatting across all operations
    DRY: Consolidates 80+ lines of duplicate error formatting across 40+ files
    
    Args:
        operation: Operation being performed (e.g., "processing", "downloading")
        item: Item being processed (e.g., filename, URL, row ID)
        error: Exception object
        context: Additional context information (optional)
        include_traceback: Include full traceback in message
        
    Returns:
        str: Formatted error message
        
    Example:
        # Replace all instances of:
        # print(f"Error processing {file}: {str(e)}")
        # logger.error(f"Failed to download {url}: {error}")
        
        # With:
        error_msg = format_error("processing", file, e, context={'row': 123})
        print(error_msg)
    """
    import traceback
    
    # Basic error message
    base_msg = f"Error {operation} {item}: {str(error)}"
    
    # Add context if provided
    if context:
        context_parts = []
        for key, value in context.items():
            context_parts.append(f"{key}={value}")
        if context_parts:
            base_msg += f" (context: {', '.join(context_parts)})"
    
    # Add traceback if requested
    if include_traceback:
        tb = traceback.format_exc()
        base_msg += f"\\nTraceback:\\n{tb}"
    
    return base_msg

def log_error(operation: str, item: str, error: Exception, logger=None, 
             context: dict = None, level: str = 'error') -> str:
    """
    Standardized error logging with consistent format
    DRY: Consolidates duplicate error logging patterns
    
    Args:
        operation: Operation being performed
        item: Item being processed
        error: Exception object
        logger: Logger instance (prints if None)
        context: Additional context information
        level: Log level ('error', 'warning', 'info')
        
    Returns:
        str: The formatted error message that was logged
        
    Example:
        # Replace:
        # logger.error(f"Error processing {file}: {str(e)}")
        
        # With:
        log_error("processing", file, e, logger, context={'row': 123})
    """
    error_msg = format_error(operation, item, error, context)
    
    if logger:
        log_func = getattr(logger, level, logger.error)
        log_func(error_msg)
    else:
        print(f"[{level.upper()}] {error_msg}")
    
    return error_msg

def handle_operation_error(operation: str, item: str, error: Exception, 
                          logger=None, return_value: Any = None, 
                          context: dict = None, reraise: bool = False) -> Any:
    """
    Complete error handling pipeline with logging and return value
    DRY: Consolidates complete error handling patterns
    
    Args:
        operation: Operation being performed
        item: Item being processed
        error: Exception object
        logger: Logger instance
        return_value: Value to return on error
        context: Additional context information
        reraise: Whether to re-raise the exception after logging
        
    Returns:
        return_value if error handled, otherwise raises exception
        
    Example:
        # Replace complex error handling blocks with:
        try:
            result = risky_operation(item)
        except Exception as e:
            return handle_operation_error("processing", item, e, logger, 
                                        return_value=None, context={'batch': 1})
    """
    log_error(operation, item, error, logger, context)
    
    if reraise:
        raise error
    
    return return_value

def format_progress_error(current: int, total: int, item: str, error: Exception, 
                         operation: str = "processing") -> str:
    """
    Format error messages with progress context
    DRY: Consolidates progress-aware error formatting
    
    Args:
        current: Current item number
        total: Total items
        item: Item being processed
        error: Exception object
        operation: Operation name
        
    Returns:
        str: Formatted error message with progress
        
    Example:
        error_msg = format_progress_error(5, 100, "file.txt", e, "downloading")
        # Returns: "Error downloading file.txt (5/100): Connection timeout"
    """
    return f"Error {operation} {item} ({current}/{total}): {str(error)}"

def create_error_handler(operation: str, logger=None, context: dict = None):
    """
    Create a reusable error handler function for specific operations
    DRY: Consolidates error handler creation patterns
    
    Args:
        operation: Operation name
        logger: Logger instance
        context: Base context for all errors
        
    Returns:
        Function that handles errors for this operation
        
    Example:
        handle_download_error = create_error_handler("downloading", logger, {'batch': 1})
        
        try:
            download_file(url)
        except Exception as e:
            handle_download_error(url, e)
    """
    def error_handler(item: str, error: Exception, additional_context: dict = None, 
                     return_value: Any = None):
        combined_context = context.copy() if context else {}
        if additional_context:
            combined_context.update(additional_context)
        
        return handle_operation_error(operation, item, error, logger, 
                                    return_value, combined_context)
    
    return error_handler

def format_validation_error(field: str, value: Any, expected: str, 
                           actual: str = None) -> str:
    """
    Format validation error messages consistently
    DRY: Consolidates validation error formatting
    
    Args:
        field: Field being validated
        value: Value that failed validation
        expected: Expected format/type
        actual: Actual format/type (optional)
        
    Returns:
        str: Formatted validation error
        
    Example:
        error = format_validation_error("email", "invalid", "valid email format")
        # Returns: "Invalid email 'invalid': expected valid email format"
    """
    msg = f"Invalid {field} '{value}': expected {expected}"
    if actual:
        msg += f", got {actual}"
    return msg

def read_text_file(file_path: Union[str, Path], encoding: str = 'utf-8', 
                  strip_lines: bool = True, skip_empty: bool = False) -> List[str]:
    """
    Standardized text file reading with encoding and error handling
    DRY: Consolidates 85+ lines of duplicate file reading patterns across 17+ files
    
    Args:
        file_path: Path to text file
        encoding: File encoding (default: utf-8)
        strip_lines: Strip whitespace from each line (default: True)
        skip_empty: Skip empty lines (default: False)
        
    Returns:
        List of lines from the file
        
    Example:
        # Replace all instances of:
        # with open('file.txt', 'r', encoding='utf-8') as f:
        #     lines = [line.strip() for line in f]
        
        # With:
        lines = read_text_file('file.txt', strip_lines=True)
    """
    try:
        file_path = Path(file_path)
        
        if not file_path.exists():
            return []
        
        with open(file_path, 'r', encoding=encoding) as f:
            lines = f.readlines()
        
        if strip_lines:
            lines = [line.strip() for line in lines]
        
        if skip_empty:
            lines = [line for line in lines if line]
        
        return lines
        
    except Exception as e:
        print(f"Error reading text file {file_path}: {str(e)}")
        return []

def read_file_with_fallback(file_path: Union[str, Path], 
                           encodings: List[str] = None) -> str:
    """
    Read file with encoding fallback for problematic files
    DRY: Consolidates encoding fallback patterns
    
    Args:
        file_path: Path to file
        encodings: List of encodings to try (default: ['utf-8', 'latin-1', 'cp1252'])
        
    Returns:
        File content as string
        
    Example:
        content = read_file_with_fallback('problematic.txt')
    """
    if encodings is None:
        encodings = ['utf-8', 'latin-1', 'cp1252', 'utf-16']
    
    file_path = Path(file_path)
    
    for encoding in encodings:
        try:
            with open(file_path, 'r', encoding=encoding) as f:
                return f.read()
        except UnicodeDecodeError:
            continue
        except Exception as e:
            print(f"Error reading {file_path} with {encoding}: {str(e)}")
            continue
    
    raise ValueError(f"Could not read {file_path} with any of the attempted encodings: {encodings}")

def process_file_lines(file_path: Union[str, Path], processor: Callable, 
                      encoding: str = 'utf-8', batch_size: int = None) -> List[Any]:
    """
    Process file line by line with custom processor function
    DRY: Consolidates line-by-line processing patterns
    
    Args:
        file_path: Path to file
        processor: Function that takes a line and returns processed result
        encoding: File encoding
        batch_size: Process in batches (None = all at once)
        
    Returns:
        List of processed results
        
    Example:
        # Process CSV lines
        def parse_csv_line(line):
            return line.split(',')
        
        rows = process_file_lines('data.csv', parse_csv_line)
    """
    try:
        file_path = Path(file_path)
        results = []
        
        with open(file_path, 'r', encoding=encoding) as f:
            if batch_size is None:
                # Process all lines at once
                for line in f:
                    result = processor(line)
                    if result is not None:
                        results.append(result)
            else:
                # Process in batches
                batch = []
                for line in f:
                    batch.append(line)
                    if len(batch) >= batch_size:
                        for batch_line in batch:
                            result = processor(batch_line)
                            if result is not None:
                                results.append(result)
                        batch = []
                
                # Process remaining lines
                for batch_line in batch:
                    result = processor(batch_line)
                    if result is not None:
                        results.append(result)
        
        return results
        
    except Exception as e:
        print(f"Error processing file {file_path}: {str(e)}")
        return []

def read_file_chunks(file_path: Union[str, Path], chunk_size: int = 8192, 
                    encoding: str = 'utf-8') -> Iterator[str]:
    """
    Read file in chunks for memory-efficient processing
    DRY: Consolidates chunk-based file reading patterns
    
    Args:
        file_path: Path to file
        chunk_size: Size of each chunk in bytes
        encoding: File encoding
        
    Yields:
        File chunks as strings
        
    Example:
        for chunk in read_file_chunks('large_file.txt'):
            process_chunk(chunk)
    """
    try:
        with open(file_path, 'r', encoding=encoding) as f:
            while True:
                chunk = f.read(chunk_size)
                if not chunk:
                    break
                yield chunk
    except Exception as e:
        print(f"Error reading file chunks from {file_path}: {str(e)}")

def safe_file_write(file_path: Union[str, Path], content: str, 
                   encoding: str = 'utf-8', backup: bool = True, 
                   atomic: bool = True) -> bool:
    """
    Safe file writing with backup and atomic operations
    DRY: Consolidates safe file writing patterns
    
    Args:
        file_path: Path to write file
        content: Content to write
        encoding: File encoding
        backup: Create backup if file exists
        atomic: Use atomic write (temp file + rename)
        
    Returns:
        bool: True if successful, False otherwise
        
    Example:
        success = safe_file_write('output.txt', content, atomic=True)
    """
    try:
        file_path = Path(file_path)
        
        # Create directory if needed
        ensure_directory_exists(file_path.parent)
        
        # Create backup if requested and file exists
        if backup and file_path.exists():
            backup_path = file_path.with_suffix(file_path.suffix + '.bak')
            import shutil
            shutil.copy2(file_path, backup_path)
        
        if atomic:
            # Atomic write using temp file
            temp_path = file_path.with_suffix(file_path.suffix + '.tmp')
            with open(temp_path, 'w', encoding=encoding) as f:
                f.write(content)
            temp_path.rename(file_path)
        else:
            # Direct write
            with open(file_path, 'w', encoding=encoding) as f:
                f.write(content)
        
        return True
        
    except Exception as e:
        print(f"Error writing file {file_path}: {str(e)}")
        return False

def read_binary_file(file_path: Union[str, Path], chunk_size: int = None) -> bytes:
    """
    Read binary file with optional chunking
    DRY: Consolidates binary file reading patterns
    
    Args:
        file_path: Path to binary file
        chunk_size: Read in chunks if specified
        
    Returns:
        File content as bytes
        
    Example:
        data = read_binary_file('image.png')
    """
    try:
        file_path = Path(file_path)
        
        if chunk_size is None:
            with open(file_path, 'rb') as f:
                return f.read()
        else:
            data = b''
            with open(file_path, 'rb') as f:
                while True:
                    chunk = f.read(chunk_size)
                    if not chunk:
                        break
                    data += chunk
            return data
            
    except Exception as e:
        print(f"Error reading binary file {file_path}: {str(e)}")
        return b''


# ================================================================================================
# STRING MANIPULATION UTILITIES
# DRY: Consolidates 54+ lines of duplicate string manipulation patterns across 27+ files
# Phase 9 Step 5: Eliminates repetitive string cleaning, formatting, and parsing operations
# ================================================================================================

def clean_filename(filename: str, replacement: str = '_') -> str:
    """
    Clean filename by removing/replacing invalid characters
    DRY: Consolidates filename cleaning across multiple files
    
    Args:
        filename: Original filename
        replacement: Character to replace invalid chars with
        
    Returns:
        Cleaned filename safe for filesystem
        
    Example:
        # Replace:
        # clean_type = self.type.replace('/', '-').replace(' ', '_').replace('#', 'num')
        # With:
        clean_type = clean_filename(self.type, replacement='-')
    """
    import re
    
    # Remove/replace problematic characters
    invalid_chars = r'[<>:"/\\|?*#()]'
    filename = re.sub(invalid_chars, replacement, filename)
    
    # Replace spaces with replacement char
    filename = filename.replace(' ', replacement)
    
    # Collapse multiple replacement chars
    while replacement + replacement in filename:
        filename = filename.replace(replacement + replacement, replacement)
    
    # Trim replacement chars from ends
    filename = filename.strip(replacement)
    
    return filename


def safe_truncate(text: str, max_length: int = 50, suffix: str = '...') -> str:
    """
    Safely truncate text to specified length with suffix
    DRY: Consolidates text truncation patterns across files
    
    Args:
        text: Text to truncate
        max_length: Maximum length including suffix
        suffix: Suffix to add if truncated
        
    Returns:
        Truncated text with suffix if needed
        
    Example:
        # Replace:
        # safe_name = safe_name.replace(' ', '_')[:20]
        # With:
        safe_name = safe_truncate(safe_name.replace(' ', '_'), 20)
    """
    if not text or len(text) <= max_length:
        return text
    
    truncate_at = max_length - len(suffix)
    if truncate_at <= 0:
        return suffix[:max_length]
    
    return text[:truncate_at] + suffix


def normalize_whitespace(text: str) -> str:
    """
    Normalize whitespace by collapsing multiple spaces and trimming
    DRY: Consolidates whitespace normalization patterns
    
    Args:
        text: Text to normalize
        
    Returns:
        Text with normalized whitespace
        
    Example:
        # Replace:
        # value = ' '.join(value.split())
        # With:
        value = normalize_whitespace(value)
    """
    import re
    
    if not text:
        return ""
    
    # Replace all whitespace sequences with single space
    text = re.sub(r'\s+', ' ', text)
    
    # Trim leading/trailing whitespace
    return text.strip()


def escape_csv_content(text: str) -> str:
    """
    Escape text content for safe CSV storage
    DRY: Consolidates CSV escaping patterns across files
    
    Args:
        text: Text to escape for CSV
        
    Returns:
        CSV-safe text
        
    Example:
        # Replace:
        # value = value.replace(',', ';').replace('\n', ' ').replace('\r', ' ')
        # With:
        value = escape_csv_content(value)
    """
    if not text:
        return ""
    
    # Replace CSV-breaking characters
    text = text.replace(',', ';')
    text = text.replace('\n', ' ')
    text = text.replace('\r', ' ')
    text = text.replace('\t', ' ')
    
    # Normalize whitespace
    return normalize_whitespace(text)


def parse_pipe_separated(text: str, strip_empty: bool = True) -> List[str]:
    """
    Parse pipe-separated values consistently
    DRY: Consolidates pipe-separated parsing across files
    
    Args:
        text: Pipe-separated text
        strip_empty: Remove empty values
        
    Returns:
        List of parsed values
        
    Example:
        # Replace:
        # links = row['youtube_playlist'].split('|')
        # links = [link.strip() for link in links if link.strip()]
        # With:
        links = parse_pipe_separated(row['youtube_playlist'])
    """
    if not text or text == '-':
        return []
    
    items = text.split('|')
    
    if strip_empty:
        items = [item.strip() for item in items if item.strip()]
    else:
        items = [item.strip() for item in items]
    
    return items


def format_file_size(size_bytes: int) -> str:
    """
    Format file size in human-readable format
    DRY: Consolidates file size formatting across files
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Human-readable size string
        
    Example:
        # Replace:
        # size_mb = round(new_path.stat().st_size / (1024 * 1024), 1)
        # With:
        size_str = format_file_size(new_path.stat().st_size)
    """
    if size_bytes == 0:
        return "0 B"
    
    units = ['B', 'KB', 'MB', 'GB', 'TB']
    size = float(size_bytes)
    unit_index = 0
    
    while size >= 1024 and unit_index < len(units) - 1:
        size /= 1024
        unit_index += 1
    
    if unit_index == 0:
        return f"{int(size)} {units[unit_index]}"
    else:
        return f"{size:.1f} {units[unit_index]}"


def extract_url_id(url: str, url_type: str = 'auto') -> Optional[str]:
    """
    Extract ID from various URL types (YouTube, Google Drive, etc.)
    DRY: Consolidates URL ID extraction patterns
    
    Args:
        url: URL to extract ID from
        url_type: Type of URL ('youtube', 'drive', 'auto')
        
    Returns:
        Extracted ID or None
        
    Example:
        # Replace duplicate YouTube ID extraction patterns
        video_id = extract_url_id(url, 'youtube')
    """
    import re
    
    if not url:
        return None
    
    if url_type == 'youtube' or (url_type == 'auto' and ('youtube.com' in url or 'youtu.be' in url)):
        # YouTube video ID patterns
        patterns = [
            r'(?:youtube\.com/watch\?v=|youtu\.be/)([^&\n?#]+)',
            r'youtube\.com/embed/([^&\n?#]+)',
            r'youtube\.com/v/([^&\n?#]+)'
        ]
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
    
    elif url_type == 'drive' or (url_type == 'auto' and 'drive.google.com' in url):
        # Google Drive file ID patterns  
        patterns = [
            r'/file/d/([a-zA-Z0-9-_]+)',
            r'id=([a-zA-Z0-9-_]+)',
            r'/folders/([a-zA-Z0-9-_]+)'
        ]
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
    
    return None


def create_safe_identifier(text: str, max_length: int = 30) -> str:
    """
    Create safe identifier from text (alphanumeric + underscores)
    DRY: Consolidates identifier creation patterns
    
    Args:
        text: Text to convert to identifier
        max_length: Maximum length for identifier
        
    Returns:
        Safe identifier string
        
    Example:
        # Replace:
        # safe_name = ''.join(c for c in self.name if c.isalnum() or c in (' ', '-', '_')).strip()
        # safe_name = safe_name.replace(' ', '_')
        # With:
        safe_name = create_safe_identifier(self.name)
    """
    if not text:
        return "unknown"
    
    # Keep only alphanumeric, spaces, hyphens, underscores
    import re
    text = re.sub(r'[^\w\s-]', '', text)
    
    # Replace spaces and hyphens with underscores
    text = re.sub(r'[\s-]+', '_', text)
    
    # Remove leading/trailing underscores
    text = text.strip('_')
    
    # Ensure not empty
    if not text:
        return "unknown"
    
    # Truncate if needed
    return safe_truncate(text, max_length, suffix='')


# Example usage
if __name__ == "__main__":
    # Test configuration loading
    config = get_config()
    
    print("Configuration loaded successfully!")
    print(f"YouTube downloads directory: {get_youtube_downloads_dir()}")
    print(f"Default timeout: {get_timeout()} seconds")
    print(f"Retry config: {get_retry_config()}")
    print(f"Parallel workers: {get_parallel_config().get('max_workers', 4)}")
    print(f"SSL verification: {is_ssl_verify_enabled()}")
    
    # Test getting nested values
    print(f"YouTube max workers: {config.get('downloads.youtube.max_workers', 4)}")
    print(f"Large file chunk size: {config.get('downloads.drive.chunk_sizes.large', 8388608)}")