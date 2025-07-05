#!/usr/bin/env python3
"""
Error Handling Decorators - Standardized error handling patterns
Consolidates common try-catch patterns and provides reusable error handling
"""

import functools
import traceback
from typing import Optional, Callable, Any, Dict, Type, Union, List
from pathlib import Path

try:
    from error_handling import ErrorHandler, ErrorContext, ErrorCategory, ErrorSeverity
    from error_messages import ErrorMessages, file_error, network_error, system_error
    from logging_config import get_logger
except ImportError:
    from .error_handling import ErrorHandler, ErrorContext, ErrorCategory, ErrorSeverity
    from .error_messages import ErrorMessages, file_error, network_error, system_error
    from .logging_config import get_logger


def handle_file_operations(operation_name: str, return_on_error: Any = None, 
                         log_errors: bool = True, context: Optional[Dict] = None):
    """
    Decorator for standardized file operation error handling
    
    Args:
        operation_name: Name of the file operation for error messages
        return_on_error: Value to return if error occurs (default: None)
        log_errors: Whether to log errors (default: True)
        context: Additional context for error handling
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger(func.__module__)
            error_handler = ErrorHandler(logger)
            
            try:
                return func(*args, **kwargs)
            except FileNotFoundError as e:
                error_msg = file_error('FILE_NOT_FOUND', path=str(e))
                if log_errors:
                    logger.error(f"{operation_name} failed: {error_msg}")
                
                error_context = {
                    'operation': operation_name,
                    'file_path': str(e),
                    **(context or {})
                }
                error_handler.handle_error(e, error_context)
                return return_on_error
                
            except PermissionError as e:
                error_msg = file_error('PERMISSION_DENIED', path=str(e))
                if log_errors:
                    logger.error(f"{operation_name} failed: {error_msg}")
                
                error_context = {
                    'operation': operation_name,
                    'file_path': str(e),
                    **(context or {})
                }
                error_handler.handle_error(e, error_context)
                return return_on_error
                
            except OSError as e:
                # Handle disk full, access denied, etc.
                if "No space left" in str(e):
                    error_msg = ErrorMessages.format_error('FILE_OPERATIONS', 'DISK_FULL', operation=operation_name)
                else:
                    error_msg = file_error('ACCESS_DENIED', resource=operation_name)
                
                if log_errors:
                    logger.error(f"{operation_name} failed: {error_msg}")
                
                error_context = {
                    'operation': operation_name,
                    'error_details': str(e),
                    **(context or {})
                }
                error_handler.handle_error(e, error_context)
                return return_on_error
                
            except Exception as e:
                error_msg = f"Unexpected error in {operation_name}: {str(e)}"
                if log_errors:
                    logger.error(error_msg)
                
                error_context = {
                    'operation': operation_name,
                    'error_details': str(e),
                    **(context or {})
                }
                error_handler.handle_error(e, error_context)
                return return_on_error
                
        return wrapper
    return decorator


def handle_network_operations(operation_name: str, return_on_error: Any = None,
                            retry_count: int = 0, log_errors: bool = True,
                            context: Optional[Dict] = None):
    """
    Decorator for standardized network operation error handling
    
    Args:
        operation_name: Name of the network operation for error messages
        return_on_error: Value to return if error occurs
        retry_count: Number of retries for network errors (default: 0)
        log_errors: Whether to log errors
        context: Additional context for error handling
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger(func.__module__)
            error_handler = ErrorHandler(logger)
            
            for attempt in range(retry_count + 1):
                try:
                    return func(*args, **kwargs)
                    
                except (ConnectionError, TimeoutError) as e:
                    error_msg = network_error('CONNECTION_TIMEOUT', 
                                            timeout=30, resource=operation_name)
                    
                    if attempt < retry_count:
                        if log_errors:
                            logger.warning(f"Attempt {attempt + 1}/{retry_count + 1} failed: {error_msg}")
                        continue
                    else:
                        if log_errors:
                            logger.error(f"{operation_name} failed after {retry_count + 1} attempts: {error_msg}")
                        
                        error_context = {
                            'operation': operation_name,
                            'attempts': retry_count + 1,
                            **(context or {})
                        }
                        error_handler.handle_error(e, error_context)
                        return return_on_error
                        
                except Exception as e:
                    # Check if it's a network-related error based on message
                    error_str = str(e).lower()
                    if any(keyword in error_str for keyword in ['network', 'dns', 'ssl', 'connection']):
                        error_msg = network_error('NETWORK_ERROR', 
                                                resource=operation_name, details=str(e))
                    else:
                        error_msg = f"Unexpected error in {operation_name}: {str(e)}"
                    
                    if log_errors:
                        logger.error(error_msg)
                    
                    error_context = {
                        'operation': operation_name,
                        'error_details': str(e),
                        **(context or {})
                    }
                    error_handler.handle_error(e, error_context)
                    return return_on_error
                    
        return wrapper
    return decorator


def handle_csv_operations(operation_name: str, return_on_error: Any = None,
                         backup_on_write: bool = True, validate_result: bool = True,
                         log_errors: bool = True, context: Optional[Dict] = None):
    """
    Decorator for standardized CSV operation error handling
    
    Args:
        operation_name: Name of the CSV operation for error messages
        return_on_error: Value to return if error occurs
        backup_on_write: Whether to create backup before write operations
        validate_result: Whether to validate CSV after operations
        log_errors: Whether to log errors
        context: Additional context for error handling
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger(func.__module__)
            error_handler = ErrorHandler(logger)
            
            try:
                result = func(*args, **kwargs)
                
                # Validate CSV if requested and result looks like a file path
                if validate_result and isinstance(result, (str, Path)):
                    try:
                        import pandas as pd
                        df = pd.read_csv(result)
                        if len(df.columns) == 0:
                            raise ValueError("CSV has no columns")
                    except Exception as validation_error:
                        error_msg = ErrorMessages.format_error('CSV_PROCESSING', 'CSV_VALIDATION_FAILED',
                                                             path=result, details=str(validation_error))
                        if log_errors:
                            logger.error(f"{operation_name} validation failed: {error_msg}")
                        
                        error_context = {
                            'operation': operation_name,
                            'validation_error': str(validation_error),
                            'file_path': str(result),
                            **(context or {})
                        }
                        error_handler.handle_error(validation_error, error_context)
                        
                return result
                
            except pd.errors.EmptyDataError as e:
                error_msg = ErrorMessages.format_error('CSV_PROCESSING', 'CSV_READ_ERROR',
                                                     path='unknown', error='Empty CSV file')
                if log_errors:
                    logger.error(f"{operation_name} failed: {error_msg}")
                
                error_context = {
                    'operation': operation_name,
                    'error_details': str(e),
                    **(context or {})
                }
                error_handler.handle_error(e, error_context)
                return return_on_error
                
            except pd.errors.ParserError as e:
                error_msg = ErrorMessages.format_error('CSV_PROCESSING', 'CSV_CORRUPTION',
                                                     path='unknown', details=str(e))
                if log_errors:
                    logger.error(f"{operation_name} failed: {error_msg}")
                
                error_context = {
                    'operation': operation_name,
                    'parser_error': str(e),
                    **(context or {})
                }
                error_handler.handle_error(e, error_context)
                return return_on_error
                
            except Exception as e:
                error_msg = f"Unexpected error in CSV {operation_name}: {str(e)}"
                if log_errors:
                    logger.error(error_msg)
                
                error_context = {
                    'operation': operation_name,
                    'error_details': str(e),
                    **(context or {})
                }
                error_handler.handle_error(e, error_context)
                return return_on_error
                
        return wrapper
    return decorator


def handle_download_operations(operation_name: str, download_type: str = 'generic',
                             return_on_error: Any = None, retry_count: int = 3,
                             log_errors: bool = True, context: Optional[Dict] = None):
    """
    Decorator for standardized download operation error handling
    
    Args:
        operation_name: Name of the download operation
        download_type: Type of download (youtube, drive, generic)
        return_on_error: Value to return if error occurs
        retry_count: Number of retries for failed downloads
        log_errors: Whether to log errors
        context: Additional context for error handling
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger(func.__module__)
            error_handler = ErrorHandler(logger)
            
            for attempt in range(retry_count + 1):
                try:
                    return func(*args, **kwargs)
                    
                except Exception as e:
                    error_str = str(e).lower()
                    
                    # Categorize download errors
                    if download_type == 'youtube':
                        if 'video unavailable' in error_str or 'private video' in error_str:
                            error_msg = ErrorMessages.format_error('DOWNLOAD', 'VIDEO_UNAVAILABLE',
                                                                 video_id=context.get('video_id', 'unknown'))
                        elif 'quota' in error_str:
                            error_msg = ErrorMessages.format_error('DOWNLOAD', 'QUOTA_EXCEEDED',
                                                                 service='YouTube')
                        else:
                            error_msg = ErrorMessages.format_error('DOWNLOAD', 'YOUTUBE_ERROR',
                                                                 video_id=context.get('video_id', 'unknown'),
                                                                 details=str(e))
                    elif download_type == 'drive':
                        if 'quota' in error_str:
                            error_msg = ErrorMessages.format_error('DOWNLOAD', 'QUOTA_EXCEEDED',
                                                                 service='Google Drive')
                        else:
                            error_msg = ErrorMessages.format_error('DOWNLOAD', 'DRIVE_ERROR',
                                                                 file_id=context.get('file_id', 'unknown'),
                                                                 details=str(e))
                    else:
                        error_msg = ErrorMessages.format_error('DOWNLOAD', 'DOWNLOAD_FAILED',
                                                             url=context.get('url', 'unknown'),
                                                             error=str(e))
                    
                    if attempt < retry_count:
                        if log_errors:
                            logger.warning(f"Download attempt {attempt + 1}/{retry_count + 1} failed: {error_msg}")
                        continue
                    else:
                        if log_errors:
                            logger.error(f"{operation_name} failed after {retry_count + 1} attempts: {error_msg}")
                        
                        error_context = {
                            'operation': operation_name,
                            'download_type': download_type,
                            'attempts': retry_count + 1,
                            **(context or {})
                        }
                        error_handler.handle_error(e, error_context)
                        return return_on_error
                        
        return wrapper
    return decorator


def handle_validation_errors(operation_name: str, return_on_error: Any = None,
                           strict_mode: bool = False, log_errors: bool = True,
                           context: Optional[Dict] = None):
    """
    Decorator for standardized validation error handling
    
    Args:
        operation_name: Name of the validation operation
        return_on_error: Value to return if validation fails
        strict_mode: Whether to treat warnings as errors
        log_errors: Whether to log errors
        context: Additional context for error handling
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger(func.__module__)
            error_handler = ErrorHandler(logger)
            
            try:
                return func(*args, **kwargs)
                
            except ValueError as e:
                error_msg = ErrorMessages.format_error('VALIDATION', 'VALIDATION_FAILED',
                                                     field=operation_name, reason=str(e))
                if log_errors:
                    logger.error(f"{operation_name} validation failed: {error_msg}")
                
                error_context = {
                    'operation': operation_name,
                    'validation_error': str(e),
                    **(context or {})
                }
                error_handler.handle_error(e, error_context)
                return return_on_error
                
            except TypeError as e:
                error_msg = ErrorMessages.format_error('VALIDATION', 'INVALID_FORMAT',
                                                     type=operation_name, value=str(e))
                if log_errors:
                    logger.error(f"{operation_name} type validation failed: {error_msg}")
                
                error_context = {
                    'operation': operation_name,
                    'type_error': str(e),
                    **(context or {})
                }
                error_handler.handle_error(e, error_context)
                return return_on_error
                
            except Exception as e:
                error_msg = f"Unexpected validation error in {operation_name}: {str(e)}"
                if log_errors:
                    logger.error(error_msg)
                
                error_context = {
                    'operation': operation_name,
                    'error_details': str(e),
                    **(context or {})
                }
                error_handler.handle_error(e, error_context)
                return return_on_error
                
        return wrapper
    return decorator


def log_and_suppress_errors(operation_name: str, return_on_error: Any = None,
                          log_level: str = 'error', include_traceback: bool = False):
    """
    Simple decorator to log and suppress all errors
    
    Args:
        operation_name: Name of the operation for logging
        return_on_error: Value to return if error occurs
        log_level: Logging level (debug, info, warning, error, critical)
        include_traceback: Whether to include full traceback in logs
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            logger = get_logger(func.__module__)
            
            try:
                return func(*args, **kwargs)
            except Exception as e:
                error_msg = f"{operation_name} failed: {str(e)}"
                if include_traceback:
                    error_msg += f"\n{traceback.format_exc()}"
                
                log_method = getattr(logger, log_level.lower(), logger.error)
                log_method(error_msg)
                
                return return_on_error
                
        return wrapper
    return decorator


# Example usage patterns
if __name__ == "__main__":
    # Example usage of the decorators
    
    @handle_file_operations("copy file", return_on_error=False)
    def copy_file(source: str, dest: str) -> bool:
        import shutil
        shutil.copy2(source, dest)
        return True
    
    @handle_network_operations("fetch data", retry_count=3, return_on_error={})
    def fetch_data(url: str) -> dict:
        import requests
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    
    @handle_csv_operations("read CSV", return_on_error=None)
    def read_csv_file(path: str):
        import pandas as pd
        return pd.read_csv(path)
    
    print("Error decorators module loaded successfully!")
    print("Available decorators:")
    print("- handle_file_operations")
    print("- handle_network_operations") 
    print("- handle_csv_operations")
    print("- handle_download_operations")
    print("- handle_validation_errors")
    print("- log_and_suppress_errors")