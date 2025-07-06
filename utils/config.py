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