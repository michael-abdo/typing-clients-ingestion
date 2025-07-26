#!/usr/bin/env python3
"""
Centralized logging configuration for the entire codebase.
Provides standardized logging setup for all modules.
"""
import logging
import sys
from pathlib import Path
from typing import Optional, Dict, Any, List
# Use basic logging setup since logger.py was archived
import logging
from datetime import datetime

# Mapping of module names to their component categories
COMPONENT_MAP = {
    'download_youtube': 'youtube',
    'download_drive': 'drive',
    'extract_links': 'scraper',
    'scrape_google_sheets': 'scraper',
    'master_scraper': 'scraper',
    'validation': 'main',
    'file_lock': 'main',
    'parallel_processor': 'main',
    'streaming_csv': 'main',
    'atomic_csv': 'main',
    'http_pool': 'main',
    'config': 'main'
}

def get_logger(name: Optional[str] = None, component: Optional[str] = None) -> logging.Logger:
    """
    Get a logger for a specific module.
    
    Args:
        name: Module name (e.g., __name__)
        component: Component category ('youtube', 'drive', 'scraper', 'main', 'errors')
                  If not provided, will be inferred from module name
    
    Returns:
        Logger instance configured for the module
    """
    if name is None:
        name = 'main'
    
    # Extract base module name
    module_base = name.split('.')[-1] if '.' in name else name
    
    # Determine component if not provided
    if component is None:
        component = COMPONENT_MAP.get(module_base, 'main')
    
    # Create basic logger
    logger = logging.getLogger(f"{component}.{module_base}")
    
    # Setup basic configuration if not already configured
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        formatter = logging.Formatter('[%(asctime)s] [%(name)s] [%(levelname)s] %(message)s')
        handler.setFormatter(formatter)
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
    
    return logger


def get_pipeline_logger():
    """Basic pipeline logger replacement."""
    class MockPipelineLogger:
        current_run = None
        def get_logger(self, component):
            return get_logger(component, component)
    
    return MockPipelineLogger()


def setup_component_logging(component: str) -> logging.Logger:
    """Setup component logging - basic replacement."""
    return get_logger(component, component)

# Module logging configuration merged into get_logger() for consistency

class LoggingAdapter:
    """
    Adapter to replace print statements with proper logging.
    Provides print-like interface that routes to logging.
    """
    
    def __init__(self, logger: logging.Logger):
        self.logger = logger
    
    def __call__(self, *args, **kwargs):
        """Print replacement that logs at INFO level"""
        message = ' '.join(str(arg) for arg in args)
        self.logger.info(message)
    
    def error(self, *args, **kwargs):
        """Print error messages"""
        message = ' '.join(str(arg) for arg in args)
        self.logger.error(message)
    
    def warning(self, *args, **kwargs):
        """Print warning messages"""
        message = ' '.join(str(arg) for arg in args)
        self.logger.warning(message)
    
    def debug(self, *args, **kwargs):
        """Print debug messages"""
        message = ' '.join(str(arg) for arg in args)
        self.logger.debug(message)

def setup_print_redirect(module_name: str) -> LoggingAdapter:
    """
    Setup a print redirect for modules that heavily use print statements.
    
    Args:
        module_name: The module name
    
    Returns:
        LoggingAdapter that can replace print
    """
    logger = get_logger(module_name)
    return LoggingAdapter(logger)

# Convenience functions for quick logging setup
def log_info(message: str, component: str = 'main'):
    """Quick info logging without module setup"""
    logger = get_logger(component=component)
    logger.info(message)

def log_error(message: str, component: str = 'main'):
    """Quick error logging without module setup"""
    logger = get_logger(component=component)
    logger.error(message)

def log_warning(message: str, component: str = 'main'):
    """Quick warning logging without module setup"""
    logger = get_logger(component=component)
    logger.warning(message)

def log_debug(message: str, component: str = 'main'):
    """Quick debug logging without module setup"""
    logger = get_logger(component=component)
    logger.debug(message)


# === DRY PHASE 2: CONSOLIDATED PRINT PATTERNS ===

def print_section_header(title: str, width: int = 80, char: str = "="):
    """
    Standard section header printing.
    
    Consolidates the repeated pattern found in 25+ files:
        print("=" * 80)
        print("DESCRIPTION")
        print("=" * 80)
    
    Args:
        title: Section title to display
        width: Total width of the header (default: 80)
        char: Character to use for the border (default: "=")
        
    Example:
        print_section_header("CSV Processing Results")
        print_section_header("Error Summary", width=60, char="-")
    """
    print(char * width)
    print(title.center(width) if len(title) < width else title)
    print(char * width)


def print_subsection(title: str, width: int = 60, char: str = "-"):
    """
    Standard subsection header printing.
    
    Consolidates the repeated pattern for smaller section headers.
    
    Args:
        title: Subsection title to display
        width: Total width of the header (default: 60)
        char: Character to use for the border (default: "-")
        
    Example:
        print_subsection("Download Status")
        print_subsection("Validation Results", width=40)
    """
    print(char * width)
    print(title)
    print(char * width)


def print_status_line(status: str, message: str, use_emoji: bool = True):
    """
    Standard status line printing.
    
    Consolidates the repeated pattern:
        print("✓ Operation successful")
        print("❌ Operation failed")
    
    Args:
        status: Status type ("success", "error", "warning", "info")
        message: Status message to display
        use_emoji: Whether to use emoji symbols (default: True)
        
    Example:
        print_status_line("success", "Downloaded 5 files")
        print_status_line("error", "Failed to connect to server")
        print_status_line("warning", "Skipping corrupted file", use_emoji=False)
    """
    emoji_map = {
        "success": "✓", 
        "error": "❌", 
        "warning": "⚠️", 
        "info": "📋",
        "pending": "⏳",
        "completed": "✅",
        "failed": "❌"
    }
    
    if use_emoji:
        prefix = emoji_map.get(status, "•")
    else:
        prefix = status.upper()
    
    print(f"{prefix} {message}")


def print_progress_bar(current: int, total: int, width: int = 50, 
                      prefix: str = "Progress", suffix: str = "Complete"):
    """
    Standard progress bar printing.
    
    Consolidates progress display patterns used in download scripts.
    
    Args:
        current: Current progress value
        total: Total maximum value
        width: Width of the progress bar (default: 50)
        prefix: Text to display before progress bar
        suffix: Text to display after progress bar
        
    Example:
        print_progress_bar(30, 100, prefix="Downloading")
        # Output: Downloading |███████████████                                   | 30% Complete
    """
    if total == 0:
        return
    
    progress = current / total
    filled_width = int(width * progress)
    bar = "█" * filled_width + "░" * (width - filled_width)
    percent = int(progress * 100)
    
    print(f"\r{prefix} |{bar}| {percent}% {suffix}", end="", flush=True)
    
    if current == total:
        print()  # New line when complete


def print_table_header(headers: List[str], widths: Optional[List[int]] = None, 
                      separator: str = " | "):
    """
    Standard table header printing.
    
    Consolidates table display patterns used in status scripts.
    
    Args:
        headers: List of column headers
        widths: List of column widths (auto-calculated if None)
        separator: Column separator string
        
    Example:
        print_table_header(["Name", "Status", "Files", "Size"])
        print_table_row(["John Doe", "Complete", "3", "1.2MB"])
    """
    if widths is None:
        widths = [max(len(header), 10) for header in headers]
    
    # Print header row
    header_row = separator.join(header.ljust(width) for header, width in zip(headers, widths))
    print(header_row)
    
    # Print separator line
    separator_line = separator.join("-" * width for width in widths)
    print(separator_line)


def print_table_row(values: List[str], widths: Optional[List[int]] = None, 
                   separator: str = " | "):
    """
    Standard table row printing.
    
    Args:
        values: List of column values
        widths: List of column widths (auto-calculated if None)
        separator: Column separator string
    """
    if widths is None:
        widths = [max(len(str(value)), 10) for value in values]
    
    row = separator.join(str(value).ljust(width) for value, width in zip(values, widths))
    print(row)


def print_summary_stats(stats: Dict[str, Any], title: str = "Summary"):
    """
    Standard summary statistics printing.
    
    Consolidates stats display patterns used throughout the codebase.
    
    Args:
        stats: Dictionary of statistics to display
        title: Title for the summary section
        
    Example:
        stats = {"Total Files": 45, "Downloaded": 42, "Failed": 3}
        print_summary_stats(stats, "Download Summary")
    """
    print_subsection(title)
    
    for key, value in stats.items():
        # Format numbers with commas for readability
        if isinstance(value, (int, float)) and value >= 1000:
            formatted_value = f"{value:,}"
        else:
            formatted_value = str(value)
        
        print(f"{key}: {formatted_value}")
    
    print()  # Empty line after summary


# === DRY PHASE 2: CONSOLIDATED LOGGING SETUP ===

def setup_basic_logging(level: str = "INFO", 
                       format_string: Optional[str] = None,
                       log_file: Optional[str] = None,
                       console: bool = True) -> logging.Logger:
    """
    Setup basic logging configuration (DRY consolidation).
    
    Eliminates duplicate logging.basicConfig() calls across 6+ files.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR)
        format_string: Custom format string (uses default if None)
        log_file: Optional log file path
        console: Whether to log to console (default: True)
        
    Returns:
        Root logger instance
        
    Example:
        # Simple setup
        logger = setup_basic_logging()
        
        # With file output
        logger = setup_basic_logging(level="DEBUG", log_file="debug.log")
        
        # Custom format
        logger = setup_basic_logging(format_string="%(levelname)s - %(message)s")
    """
    # Default format if not provided
    if format_string is None:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    handlers = []
    
    # Console handler
    if console:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(logging.Formatter(format_string))
        handlers.append(console_handler)
    
    # File handler
    if log_file:
        from pathlib import Path
        log_path = Path(log_file)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(logging.Formatter(format_string))
        handlers.append(file_handler)
    
    # Configure basic logging
    logging.basicConfig(
        level=getattr(logging, level.upper()),
        handlers=handlers,
        force=True  # Force reconfiguration
    )
    
    return logging.getLogger()


def setup_module_logger(module_name: str, 
                       level: Optional[str] = None,
                       propagate: bool = True) -> logging.Logger:
    """
    Setup a logger for a specific module (DRY consolidation).
    
    Eliminates duplicate getLogger() and setLevel() patterns.
    
    Args:
        module_name: Module name (typically __name__)
        level: Optional logging level override
        propagate: Whether to propagate to parent logger
        
    Returns:
        Logger instance for the module
        
    Example:
        logger = setup_module_logger(__name__)
        logger = setup_module_logger(__name__, level="DEBUG")
    """
    logger = logging.getLogger(module_name)
    
    if level:
        logger.setLevel(getattr(logging, level.upper()))
    
    logger.propagate = propagate
    
    return logger


def setup_null_logger(module_name: str) -> logging.Logger:
    """
    Setup a null logger that discards all messages (DRY consolidation).
    
    Useful for libraries that should be silent by default.
    
    Args:
        module_name: Module name
        
    Returns:
        Logger with NullHandler
        
    Example:
        logger = setup_null_logger(__name__)
    """
    logger = logging.getLogger(module_name)
    logger.addHandler(logging.NullHandler())
    logger.propagate = False
    return logger


def add_file_handler(logger: logging.Logger, 
                    log_file: str,
                    level: Optional[str] = None,
                    format_string: Optional[str] = None) -> logging.FileHandler:
    """
    Add a file handler to an existing logger (DRY consolidation).
    
    Args:
        logger: Logger instance to add handler to
        log_file: Path to log file
        level: Optional level for this handler
        format_string: Optional format string for this handler
        
    Returns:
        The FileHandler that was added
        
    Example:
        logger = get_logger(__name__)
        add_file_handler(logger, "debug.log", level="DEBUG")
    """
    from pathlib import Path
    
    # Ensure directory exists
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create handler
    handler = logging.FileHandler(log_file)
    
    # Set level if provided
    if level:
        handler.setLevel(getattr(logging, level.upper()))
    
    # Set formatter
    if format_string is None:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    handler.setFormatter(logging.Formatter(format_string))
    
    # Add to logger
    logger.addHandler(handler)
    
    return handler


def create_rotating_file_handler(log_file: str,
                               max_bytes: int = 10485760,  # 10MB
                               backup_count: int = 5,
                               format_string: Optional[str] = None) -> logging.Handler:
    """
    Create a rotating file handler (DRY consolidation).
    
    Args:
        log_file: Path to log file
        max_bytes: Maximum size before rotation (default: 10MB)
        backup_count: Number of backup files to keep
        format_string: Optional format string
        
    Returns:
        RotatingFileHandler instance
        
    Example:
        handler = create_rotating_file_handler("app.log", max_bytes=5*1024*1024)
        logger.addHandler(handler)
    """
    from logging.handlers import RotatingFileHandler
    from pathlib import Path
    
    # Ensure directory exists
    log_path = Path(log_file)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    
    # Create handler
    handler = RotatingFileHandler(
        log_file, 
        maxBytes=max_bytes,
        backupCount=backup_count
    )
    
    # Set formatter
    if format_string is None:
        format_string = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    handler.setFormatter(logging.Formatter(format_string))
    
    return handler


def log_exception(logger: logging.Logger, 
                 message: str = "Exception occurred",
                 exc_info: bool = True) -> None:
    """
    Log an exception with traceback (DRY consolidation).
    
    Consolidates exception logging patterns.
    
    Args:
        logger: Logger instance
        message: Error message
        exc_info: Whether to include exception info
        
    Example:
        try:
            risky_operation()
        except Exception:
            log_exception(logger, "Failed to process file")
    """
    logger.error(message, exc_info=exc_info)


def log_function_call(logger: logging.Logger,
                     func_name: str,
                     args: Optional[tuple] = None,
                     kwargs: Optional[dict] = None) -> None:
    """
    Log a function call with arguments (DRY consolidation).
    
    Useful for debugging and tracing.
    
    Args:
        logger: Logger instance
        func_name: Function name
        args: Positional arguments
        kwargs: Keyword arguments
        
    Example:
        log_function_call(logger, "download_file", args=("video.mp4",), kwargs={"timeout": 30})
    """
    msg_parts = [f"Calling {func_name}"]
    
    if args:
        msg_parts.append(f"args={args}")
    
    if kwargs:
        msg_parts.append(f"kwargs={kwargs}")
    
    logger.debug(" ".join(msg_parts))


def create_logger_with_context(module_name: str,
                             context: Dict[str, Any]) -> logging.LoggerAdapter:
    """
    Create a logger with contextual information (DRY consolidation).
    
    Args:
        module_name: Module name
        context: Context dictionary to include in all log messages
        
    Returns:
        LoggerAdapter with context
        
    Example:
        logger = create_logger_with_context(__name__, {"user_id": 123, "session": "abc"})
        logger.info("Processing request")  # Will include context in log
    """
    base_logger = get_logger(module_name)
    return logging.LoggerAdapter(base_logger, context)


# Logging format templates
class LogFormats:
    """Common logging format strings (DRY consolidation)."""
    
    SIMPLE = "%(levelname)s - %(message)s"
    STANDARD = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    DETAILED = "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s"
    JSON = '{"time": "%(asctime)s", "level": "%(levelname)s", "module": "%(name)s", "message": "%(message)s"}'
    COLORED = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"  # Use with colorlog
    
    @staticmethod
    def with_thread():
        """Format including thread information."""
        return "%(asctime)s - [%(threadName)s] - %(name)s - %(levelname)s - %(message)s"
    
    @staticmethod
    def with_process():
        """Format including process information."""
        return "%(asctime)s - [%(processName)s] - %(name)s - %(levelname)s - %(message)s"


def configure_json_logging(logger: logging.Logger) -> None:
    """
    Configure logger for JSON output (DRY consolidation).
    
    Useful for structured logging to be consumed by log aggregation systems.
    
    Args:
        logger: Logger to configure for JSON output
        
    Example:
        logger = get_logger(__name__)
        configure_json_logging(logger)
    """
    for handler in logger.handlers:
        handler.setFormatter(logging.Formatter(LogFormats.JSON))


def suppress_library_logging(library_names: List[str], level: str = "WARNING") -> None:
    """
    Suppress verbose logging from third-party libraries (DRY consolidation).
    
    Args:
        library_names: List of library names to suppress
        level: Minimum level to allow (default: WARNING)
        
    Example:
        suppress_library_logging(["urllib3", "requests", "botocore"])
    """
    for lib_name in library_names:
        logging.getLogger(lib_name).setLevel(getattr(logging, level.upper()))