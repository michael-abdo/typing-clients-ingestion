"""Configuration management module for centralized settings."""
import os
import yaml
import importlib
from pathlib import Path
from typing import Dict, Any, Optional, Union, List
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
    return get_config().get("paths.output_csv", "output.csv")


def get_google_sheets_url() -> str:
    """Get Google Sheets URL."""
    return get_config().get("google_sheets.url", "")


def get_retry_config() -> Dict[str, Any]:
    """Get retry configuration."""
    return get_config().get_section("retry")


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


def ensure_directory(dir_path: Union[str, Path], parents: bool = True, exist_ok: bool = True, logger=None) -> Path:
    """
    Ensure directory exists, creating it if necessary (DRY path utility).
    Consolidates various directory creation patterns throughout codebase.
    
    Args:
        dir_path: Directory path to ensure exists
        parents: Create parent directories if needed
        exist_ok: Don't raise error if directory already exists
        logger: Optional logger instance
    
    Returns:
        Path object for the directory
    """
    path = Path(dir_path)
    if not path.exists():
        path.mkdir(parents=parents, exist_ok=exist_ok)
        if logger:
            logger.info(f"Created directory: {dir_path}")
    return path


def ensure_parent_dir(file_path: Union[str, Path], logger=None) -> Path:
    """
    Ensure parent directory of a file exists (DRY path utility).
    Common pattern for ensuring output files can be written.
    
    Args:
        file_path: File path whose parent directory should exist
        logger: Optional logger instance
    
    Returns:
        Path object for the parent directory
    """
    file_path = Path(file_path)
    parent_dir = file_path.parent
    if not parent_dir.exists():
        parent_dir.mkdir(parents=True, exist_ok=True)
        if logger:
            logger.info(f"Created parent directory: {parent_dir}")
    return parent_dir


def get_project_root() -> Path:
    """
    Get project root directory (DRY path utility).
    
    Returns:
        Path object for project root
    """
    return Path(__file__).parent.parent


def get_outputs_dir() -> Path:
    """
    Get outputs directory, creating if necessary (DRY path utility).
    
    Returns:
        Path object for outputs directory
    """
    config = get_config()
    outputs_dir = get_project_root() / config.get("paths.output_dir", "outputs")
    return ensure_directory(outputs_dir)


def get_logs_dir() -> Path:
    """
    Get logs directory, creating if necessary (DRY path utility).
    
    Returns:
        Path object for logs directory
    """
    config = get_config()
    logs_dir = get_project_root() / config.get("paths.logs_dir", "logs")
    return ensure_directory(logs_dir)


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


# Simple error categorization utilities (DRY)
def categorize_error(error: Exception) -> str:
    """
    Simple error categorization for consistent error handling (DRY).
    Consolidates scattered error type checking throughout codebase.
    
    Args:
        error: Exception to categorize
    
    Returns:
        Error category string
    """
    error_str = str(error).lower()
    
    # Network-related errors
    if any(keyword in error_str for keyword in ['timeout', 'connection', 'network', 'dns', 'ssl']):
        return 'network'
    elif any(keyword in error_str for keyword in ['rate limit', 'quota', '429', 'too many requests']):
        return 'rate_limit'
    elif any(keyword in error_str for keyword in ['http', '404', '403', '401', '500', '502', '503']):
        return 'http'
    
    # File I/O errors
    elif any(keyword in error_str for keyword in ['file not found', 'no such file', 'permission denied', 'access denied']):
        return 'file_io'
    elif any(keyword in error_str for keyword in ['disk', 'space', 'full']):
        return 'disk_space'
    
    # Data errors
    elif any(keyword in error_str for keyword in ['csv', 'parsing', 'format', 'decode', 'encode']):
        return 'data_format'
    
    # System errors
    elif any(keyword in error_str for keyword in ['memory', 'resource', 'system']):
        return 'system'
    
    # Default category
    else:
        return 'unknown'


def format_error_message(operation: str, error: Exception, context: str = None) -> str:
    """
    Format error message consistently (DRY).
    Consolidates scattered error message formatting patterns.
    
    Args:
        operation: Operation that failed
        error: Exception that occurred
        context: Optional context information
    
    Returns:
        Formatted error message
    """
    category = categorize_error(error)
    base_msg = f"✗ {operation} failed ({category}): {str(error)}"
    
    if context:
        base_msg += f" | Context: {context}"
    
    return base_msg


def load_json_state(filename: str, default: Optional[dict] = None) -> dict:
    """
    Load JSON state file with default fallback (DRY state management).
    Consolidates progress/state loading patterns throughout codebase.
    
    Args:
        filename: Path to JSON file
        default: Default dict to return if file doesn't exist
    
    Returns:
        Loaded JSON data or default
    """
    import json
    filepath = Path(filename)
    if filepath.exists():
        try:
            with open(filepath, 'r') as f:
                return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            logging.warning(f"Failed to load JSON state from {filename}: {e}")
            return default or {}
    return default or {}


def save_json_state(filename: str, data: dict) -> None:
    """
    Save state to JSON file (DRY state management).
    Consolidates progress/state saving patterns throughout codebase.
    
    Args:
        filename: Path to JSON file
        data: Dict to save as JSON
    """
    import json
    try:
        with open(filename, 'w') as f:
            json.dump(data, f, indent=2)
    except IOError as e:
        logging.error(f"Failed to save JSON state to {filename}: {e}")
        raise


# === STATUS FORMATTING FUNCTIONS (DRY) ===

class StatusIcons:
    """Centralized status icons for consistent output formatting"""
    SUCCESS = "✓"
    FAILURE = "✗"
    WARNING = "⚠"
    ERROR = "❌"
    COMPLETE = "✅"
    STATS = "📊"
    CELEBRATE = "🎉"
    FILE = "📄"
    DATABASE = "🗄️"
    PACKAGE = "📦"
    ROCKET = "🚀"
    
    # Status prefixes
    SUCCESS_PREFIX = "✓"
    ERROR_PREFIX = "✗"
    WARNING_PREFIX = "⚠️"
    INFO_PREFIX = "📊"
    

def format_success(message: str, icon: bool = True) -> str:
    """Format a success message (DRY)"""
    return f"{StatusIcons.SUCCESS} {message}" if icon else message


def format_error(message: str, icon: bool = True) -> str:
    """Format an error message (DRY)"""
    return f"{StatusIcons.ERROR} {message}" if icon else message


def format_warning(message: str, icon: bool = True) -> str:
    """Format a warning message (DRY)"""
    return f"{StatusIcons.WARNING} {message}" if icon else message


def format_stats(label: str, value: Any, icon: bool = True) -> str:
    """Format a statistics message (DRY)"""
    return f"{StatusIcons.STATS} {label}: {value}" if icon else f"{label}: {value}"


def format_status_line(status: str, message: str) -> str:
    """Format a status line with appropriate icon (DRY)
    
    Args:
        status: Status type ('success', 'error', 'warning', 'info', 'complete')
        message: The message to format
        
    Returns:
        Formatted status line
    """
    status_map = {
        'success': StatusIcons.SUCCESS,
        'error': StatusIcons.ERROR,
        'warning': StatusIcons.WARNING,
        'info': StatusIcons.STATS,
        'complete': StatusIcons.COMPLETE,
        'failure': StatusIcons.FAILURE,
        'celebrate': StatusIcons.CELEBRATE,
        'file': StatusIcons.FILE,
        'database': StatusIcons.DATABASE,
        'package': StatusIcons.PACKAGE,
        'rocket': StatusIcons.ROCKET
    }
    
    icon = status_map.get(status.lower(), '')
    return f"{icon} {message}" if icon else message


def format_batch_header(batch_num: int, total_batches: int, batch_size: int) -> str:
    """Format batch processing header (DRY)"""
    return f"\n{StatusIcons.PACKAGE} BATCH {batch_num}/{total_batches} ({batch_size} documents)"


def format_progress_indicator(current: int, total: int, label: str = "Processing") -> str:
    """Format progress indicator (DRY)"""
    return f"[{current}/{total}] {label}"


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


# ============================================================================
# CLI ARGUMENT PARSER CONSOLIDATION (DRY Phase 2)
# ============================================================================

def create_standard_parser(description: str, **standard_args) -> 'argparse.ArgumentParser':
    """
    Create standardized ArgumentParser with common arguments (DRY consolidation).
    
    Eliminates duplicate argparse setup patterns across 20+ scripts.
    
    Args:
        description: Description for the parser
        **standard_args: Standard argument flags to include:
            - csv: bool - Add --csv argument for CSV file path
            - max_rows: bool - Add --max-rows argument  
            - directory: bool - Add --directory argument
            - debug: bool - Add --debug/--verbose arguments
            - output: bool - Add --output argument
            - dry_run: bool - Add --dry-run argument
            
    Returns:
        Configured ArgumentParser instance
    """
    import argparse
    
    parser = argparse.ArgumentParser(description=description)
    
    # Standard CSV file argument
    if standard_args.get('csv', False):
        default_csv = get_config().get('paths.output_csv', 'outputs/output.csv')
        parser.add_argument('--csv', default=default_csv,
                          help=f'Path to CSV file (default: {default_csv})')
    
    # Standard max rows argument
    if standard_args.get('max_rows', False):
        parser.add_argument('--max-rows', type=int, default=None,
                          help='Maximum number of rows to process')
    
    # Standard directory argument
    if standard_args.get('directory', False):
        parser.add_argument('--directory', default='youtube_downloads',
                          help='Directory path (default: youtube_downloads)')
    
    # Standard debug/verbose arguments
    if standard_args.get('debug', False):
        parser.add_argument('--debug', action='store_true',
                          help='Enable debug output')
        parser.add_argument('--verbose', '-v', action='store_true',
                          help='Enable verbose output')
    
    # Standard output argument
    if standard_args.get('output', False):
        parser.add_argument('--output', '-o', type=str,
                          help='Output file path')
    
    # Standard dry run argument
    if standard_args.get('dry_run', False):
        parser.add_argument('--dry-run', action='store_true',
                          help='Show what would be done without executing')
    
    return parser


def standard_script_main(main_func, description: str, **parser_args):
    """
    Standard script entry point wrapper with error handling (DRY consolidation).
    
    Eliminates duplicate main() patterns across 78+ script files.
    
    Args:
        main_func: Function to call with parsed args
        description: Description for argument parser
        **parser_args: Arguments to pass to create_standard_parser()
        
    Returns:
        Exit code (0 for success, 1 for error)
    """
    try:
        parser = create_standard_parser(description, **parser_args)
        args = parser.parse_args()
        result = main_func(args)
        return result if isinstance(result, int) else 0
    except KeyboardInterrupt:
        print("\n⚠️  Script interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

# ============================================================================
# CONSOLIDATED CONSTANTS AND MAGIC NUMBERS (DRY Phase 2)
# ============================================================================

class Constants:
    """Centralized constants to eliminate magic numbers across the codebase (DRY consolidation)."""
    
    # File size constants (bytes)
    BYTES_PER_KB = 1024
    BYTES_PER_MB = 1024 * 1024
    BYTES_PER_GB = 1024 * 1024 * 1024
    
    # Progress display constants
    DEFAULT_PROGRESS_BAR_WIDTH = 40
    PROGRESS_UPDATE_INTERVAL = 1.0  # seconds
    
    # Default delays (seconds)
    DEFAULT_DELAY = 2.0
    SHORT_DELAY = 1.0
    LONG_DELAY = 5.0


def bytes_to_mb(bytes_value: int) -> float:
    """Convert bytes to megabytes (DRY utility)."""
    return bytes_value / Constants.BYTES_PER_MB


def bytes_to_gb(bytes_value: int) -> float:
    """Convert bytes to gigabytes (DRY utility)."""
    return bytes_value / Constants.BYTES_PER_GB

