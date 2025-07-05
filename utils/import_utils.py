#!/usr/bin/env python3
"""
Import Utilities - Centralized import handling to eliminate duplicate patterns
Consolidates try/except ImportError blocks and standardizes import organization
"""

import sys
import os
import importlib
from typing import Any, List, Optional, Union, Dict


def setup_project_path():
    """
    Add project root to sys.path for imports
    Replaces duplicate sys.path manipulation across test files and scripts
    """
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)


def safe_import(module_name: str, from_items: Optional[List[str]] = None, 
               fallback_relative: bool = True, package: Optional[str] = None) -> Any:
    """
    Safely import modules with automatic fallback to relative imports
    
    Eliminates the need for try/except ImportError blocks throughout the codebase
    
    Args:
        module_name: Name of module to import (e.g., 'logging_config')
        from_items: List of items to import from module (e.g., ['get_logger', 'setup_logging'])
        fallback_relative: Whether to try relative import on absolute import failure
        package: Package name for relative imports (auto-detected if None)
        
    Returns:
        - If from_items is None: the imported module
        - If from_items has one item: the imported item
        - If from_items has multiple items: tuple of imported items
        
    Example:
        # Instead of:
        try:
            from logging_config import get_logger
        except ImportError:
            from .logging_config import get_logger
            
        # Use:
        get_logger = safe_import('logging_config', ['get_logger'])
    """
    try:
        # Try absolute import first
        module = importlib.import_module(module_name)
        
        if from_items is None:
            return module
        elif len(from_items) == 1:
            return getattr(module, from_items[0])
        else:
            return tuple(getattr(module, item) for item in from_items)
            
    except ImportError:
        if not fallback_relative:
            raise
        
        try:
            # Try relative import
            if package is None:
                # Auto-detect package from caller's __name__
                frame = sys._getframe(1)
                caller_globals = frame.f_globals
                caller_name = caller_globals.get('__name__', '')
                if '.' in caller_name:
                    package = caller_name.rsplit('.', 1)[0]
                else:
                    package = None
            
            relative_name = f'.{module_name}'
            module = importlib.import_module(relative_name, package=package)
            
            if from_items is None:
                return module
            elif len(from_items) == 1:
                return getattr(module, from_items[0])
            else:
                return tuple(getattr(module, item) for item in from_items)
                
        except ImportError:
            # Both absolute and relative imports failed
            raise ImportError(f"Could not import {module_name} (tried both absolute and relative)")


def safe_import_multiple(import_specs: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Import multiple modules safely with a single call
    
    Args:
        import_specs: List of import specifications, each containing:
            - module: module name
            - from_items: items to import (optional)
            - alias: alias for imported items (optional)
            - fallback_relative: whether to try relative import (default: True)
            
    Returns:
        Dictionary mapping aliases to imported items
        
    Example:
        imports = safe_import_multiple([
            {'module': 'logging_config', 'from_items': ['get_logger'], 'alias': 'get_logger'},
            {'module': 'config', 'from_items': ['get_config'], 'alias': 'get_config'},
            {'module': 'file_lock', 'from_items': ['file_lock'], 'alias': 'file_lock'}
        ])
        
        get_logger = imports['get_logger']
        get_config = imports['get_config']
        file_lock = imports['file_lock']
    """
    results = {}
    
    for spec in import_specs:
        module_name = spec['module']
        from_items = spec.get('from_items')
        alias = spec.get('alias', module_name)
        fallback_relative = spec.get('fallback_relative', True)
        
        try:
            imported = safe_import(module_name, from_items, fallback_relative)
            results[alias] = imported
        except ImportError as e:
            # Store the error for debugging but continue with other imports
            results[alias] = ImportError(f"Failed to import {module_name}: {e}")
    
    return results


def optional_import(module_name: str, from_items: Optional[List[str]] = None, 
                   default: Any = None, fallback_relative: bool = True) -> Any:
    """
    Import module optionally, returning default value if import fails
    
    Args:
        module_name: Name of module to import
        from_items: Items to import from module
        default: Value to return if import fails
        fallback_relative: Whether to try relative import
        
    Returns:
        Imported module/items or default value
        
    Example:
        # Optional pandas import
        pd = optional_import('pandas', alias='pd', default=None)
        if pd is not None:
            # Use pandas functionality
    """
    try:
        return safe_import(module_name, from_items, fallback_relative)
    except ImportError:
        return default


# Common import patterns used throughout the codebase
class CommonImports:
    """Pre-defined import specifications for common module groups"""
    
    # Core utility imports (used in most utils files)
    CORE_UTILS = [
        {'module': 'logging_config', 'from_items': ['get_logger'], 'alias': 'get_logger'},
        {'module': 'config', 'from_items': ['get_config'], 'alias': 'get_config'},
        {'module': 'file_lock', 'from_items': ['file_lock'], 'alias': 'file_lock'}
    ]
    
    # Download-related imports
    DOWNLOAD_UTILS = [
        {'module': 'validation', 'from_items': ['validate_youtube_url', 'validate_google_drive_url'], 'alias': 'validation_funcs'},
        {'module': 'retry_utils', 'from_items': ['retry_with_backoff', 'retry_subprocess'], 'alias': 'retry_funcs'},
        {'module': 'rate_limiter', 'from_items': ['rate_limit'], 'alias': 'rate_limit'},
        {'module': 'row_context', 'from_items': ['RowContext', 'DownloadResult'], 'alias': 'context_classes'}
    ]
    
    # CSV processing imports  
    CSV_UTILS = [
        {'module': 'csv_tracker', 'from_items': ['safe_csv_read', 'safe_csv_write'], 'alias': 'csv_funcs'},
        {'module': 'atomic_csv', 'from_items': ['AtomicCSVWriter'], 'alias': 'AtomicCSVWriter'},
        {'module': 'streaming_csv', 'from_items': ['StreamingCSVProcessor'], 'alias': 'StreamingCSVProcessor'}
    ]
    
    # Error handling imports
    ERROR_HANDLING = [
        {'module': 'error_handling', 'from_items': ['ErrorHandler', 'ErrorContext'], 'alias': 'error_classes'},
        {'module': 'error_messages', 'from_items': ['ErrorMessages'], 'alias': 'ErrorMessages'},
        {'module': 'error_decorators', 'from_items': ['handle_file_operations', 'handle_network_operations'], 'alias': 'error_decorators'}
    ]
    
    # Testing imports
    TEST_UTILS = [
        {'module': 'unittest', 'from_items': ['TestCase'], 'alias': 'TestCase'},
        {'module': 'pathlib', 'from_items': ['Path'], 'alias': 'Path'},
        {'module': 'tempfile', 'from_items': ['TemporaryDirectory'], 'alias': 'TemporaryDirectory'}
    ]


def import_core_utils() -> Dict[str, Any]:
    """Import core utility functions used in most files"""
    return safe_import_multiple(CommonImports.CORE_UTILS)


def import_download_utils() -> Dict[str, Any]:
    """Import download-related utilities"""
    return safe_import_multiple(CommonImports.DOWNLOAD_UTILS)


def import_csv_utils() -> Dict[str, Any]:
    """Import CSV processing utilities"""
    return safe_import_multiple(CommonImports.CSV_UTILS)


def import_error_handling() -> Dict[str, Any]:
    """Import error handling utilities"""
    return safe_import_multiple(CommonImports.ERROR_HANDLING)


def import_test_utils() -> Dict[str, Any]:
    """Import testing utilities for test files"""
    setup_project_path()  # Automatically setup path for tests
    return safe_import_multiple(CommonImports.TEST_UTILS)


# Convenience functions for the most common import patterns
def get_standard_imports():
    """
    Get the most commonly used imports across the codebase
    Returns get_logger, get_config, file_lock functions
    """
    imports = import_core_utils()
    return imports['get_logger'], imports['get_config'], imports['file_lock']


def get_error_handling_imports():
    """
    Get error handling imports
    Returns ErrorHandler, ErrorMessages, and decorator functions
    """
    imports = import_error_handling()
    error_classes = imports['error_classes']
    return error_classes[0], imports['ErrorMessages'], imports['error_decorators']


# Module-level convenience for immediate use
def setup_standard_imports():
    """
    Setup standard imports and return them as a namespace object
    Usage: imports = setup_standard_imports()
           logger = imports.get_logger(__name__)
    """
    class ImportNamespace:
        pass
    
    namespace = ImportNamespace()
    core_imports = import_core_utils()
    
    # Add core utilities to namespace
    for name, func in core_imports.items():
        if not isinstance(func, ImportError):
            setattr(namespace, name, func)
    
    return namespace


if __name__ == "__main__":
    # Test the import utilities
    print("=== Import Utilities Test ===")
    
    # Test core imports
    print("\nTesting core imports:")
    try:
        get_logger, get_config, file_lock = get_standard_imports()
        print("✓ Core imports successful")
    except Exception as e:
        print(f"✗ Core imports failed: {e}")
    
    # Test safe_import with specific modules
    print("\nTesting safe_import:")
    try:
        get_logger = safe_import('logging_config', ['get_logger'])
        print("✓ safe_import successful")
    except Exception as e:
        print(f"✗ safe_import failed: {e}")
    
    # Test optional import
    print("\nTesting optional imports:")
    pandas = optional_import('pandas', default=None)
    if pandas is not None:
        print("✓ pandas available")
    else:
        print("- pandas not available (expected)")
    
    # Test namespace setup
    print("\nTesting namespace setup:")
    try:
        imports = setup_standard_imports()
        print("✓ Namespace setup successful")
        
        # List available imports
        available = [attr for attr in dir(imports) if not attr.startswith('_')]
        print(f"Available imports: {available}")
    except Exception as e:
        print(f"✗ Namespace setup failed: {e}")
    
    print("\n=== Import utilities ready for use! ===")