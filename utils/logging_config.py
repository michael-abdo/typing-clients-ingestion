#!/usr/bin/env python3
"""
Centralized logging configuration for the entire codebase.
Provides standardized logging setup for all modules.
"""
import logging
import sys
from pathlib import Path
from typing import Optional, Dict, Any
try:
    from logger import get_pipeline_logger, setup_component_logging
except ImportError:
    from .logger import get_pipeline_logger, setup_component_logging

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
    
    # Get pipeline logger
    pipeline_logger = get_pipeline_logger()
    
    # If no active run, setup component logging
    if not pipeline_logger.current_run:
        return setup_component_logging(component)
    
    # Get component logger
    return pipeline_logger.get_logger(component)

def configure_module_logging(module_name: str) -> logging.Logger:
    """
    Configure logging for a specific module with proper formatting.
    
    Args:
        module_name: The module name (typically __name__)
    
    Returns:
        Configured logger instance
    """
    logger = get_logger(module_name)
    
    # Set module-specific name for better tracking
    logger.logger.name = module_name
    
    return logger

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