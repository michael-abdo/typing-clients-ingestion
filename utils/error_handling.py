#!/usr/bin/env python3
"""
Error Handling and Validation - Production-ready error management
Comprehensive error handling for the row-centric download tracking system
"""

import os
import traceback
from typing import Optional, Dict, Any, List
from datetime import datetime
from enum import Enum
from dataclasses import dataclass

try:
    from logging_config import get_logger
except ImportError:
    from .logging_config import get_logger


class ErrorSeverity(Enum):
    """Error severity levels"""
    INFO = "info"
    WARNING = "warning" 
    ERROR = "error"
    CRITICAL = "critical"


class ErrorCategory(Enum):
    """Error categories for classification"""
    NETWORK = "network"
    FILE_IO = "file_io"
    VALIDATION = "validation"
    PERMISSION = "permission"
    QUOTA = "quota"
    RATE_LIMIT = "rate_limit"
    SYSTEM = "system"
    DATA_CORRUPTION = "data_corruption"


@dataclass
class ErrorContext:
    """Complete error context for tracking and debugging"""
    error_id: str
    severity: ErrorSeverity
    category: ErrorCategory
    message: str
    details: Optional[str] = None
    row_id: Optional[str] = None
    download_type: Optional[str] = None
    url: Optional[str] = None
    file_path: Optional[str] = None
    timestamp: str = None
    stack_trace: Optional[str] = None
    
    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now().isoformat()
            
    def to_dict(self) -> Dict[str, Any]:
        """Convert to dictionary for logging/storage"""
        return {
            'error_id': self.error_id,
            'severity': self.severity.value,
            'category': self.category.value,
            'message': self.message,
            'details': self.details,
            'row_id': self.row_id,
            'download_type': self.download_type,
            'url': self.url,
            'file_path': self.file_path,
            'timestamp': self.timestamp,
            'stack_trace': self.stack_trace
        }
    
    def to_user_message(self) -> str:
        """Create user-friendly error message"""
        base_msg = f"[{self.category.value.upper()}] {self.message}"
        if self.row_id:
            base_msg += f" (Row: {self.row_id})"
        if self.details:
            base_msg += f" - {self.details}"
        return base_msg


class ErrorHandler:
    """Production-ready error handling with categorization and recovery"""
    
    def __init__(self, logger=None):
        self.logger = logger or get_logger(__name__)
        self.error_counts = {}
        self.recent_errors = []
        
    def handle_error(self, error: Exception, context: Dict[str, Any] = None) -> ErrorContext:
        """Handle and categorize errors with full context"""
        context = context or {}
        
        # Generate unique error ID
        error_id = f"ERR_{datetime.now().strftime('%Y%m%d_%H%M%S_%f')[:-3]}"
        
        # Categorize error
        category, severity = self._categorize_error(error)
        
        # Extract details
        details = self._extract_error_details(error)
        
        # Create error context
        error_context = ErrorContext(
            error_id=error_id,
            severity=severity,
            category=category,
            message=str(error),
            details=details,
            row_id=context.get('row_id'),
            download_type=context.get('download_type'),
            url=context.get('url'),
            file_path=context.get('file_path'),
            stack_trace=traceback.format_exc() if severity in [ErrorSeverity.ERROR, ErrorSeverity.CRITICAL] else None
        )
        
        # Log error
        self._log_error(error_context)
        
        # Track error patterns
        self._track_error_pattern(error_context)
        
        return error_context
    
    def _categorize_error(self, error: Exception) -> tuple[ErrorCategory, ErrorSeverity]:
        """Categorize error and determine severity"""
        error_type = type(error).__name__
        error_msg = str(error).lower()
        
        # Network errors
        if any(keyword in error_msg for keyword in ['connection', 'timeout', 'network', 'dns', 'ssl']):
            return ErrorCategory.NETWORK, ErrorSeverity.WARNING
            
        # File I/O errors
        if any(keyword in error_msg for keyword in ['no such file', 'permission denied', 'disk full']):
            if 'permission denied' in error_msg:
                return ErrorCategory.PERMISSION, ErrorSeverity.ERROR
            return ErrorCategory.FILE_IO, ErrorSeverity.ERROR
            
        # Rate limiting
        if any(keyword in error_msg for keyword in ['rate limit', 'quota', 'too many requests']):
            return ErrorCategory.RATE_LIMIT, ErrorSeverity.WARNING
            
        # Validation errors
        if 'validation' in error_msg or error_type in ['ValidationError', 'ValueError']:
            return ErrorCategory.VALIDATION, ErrorSeverity.WARNING
            
        # YouTube/Google API specific errors
        if any(keyword in error_msg for keyword in ['video unavailable', 'private video', 'deleted']):
            return ErrorCategory.VALIDATION, ErrorSeverity.INFO
            
        # Drive file errors
        if any(keyword in error_msg for keyword in ['file not found', 'access denied', 'sharing']):
            return ErrorCategory.PERMISSION, ErrorSeverity.WARNING
            
        # Data corruption
        if any(keyword in error_msg for keyword in ['corrupt', 'malformed', 'invalid format']):
            return ErrorCategory.DATA_CORRUPTION, ErrorSeverity.ERROR
            
        # System errors
        if error_type in ['OSError', 'SystemError', 'MemoryError']:
            return ErrorCategory.SYSTEM, ErrorSeverity.CRITICAL
            
        # Default classification
        return ErrorCategory.SYSTEM, ErrorSeverity.ERROR
    
    def _extract_error_details(self, error: Exception) -> Optional[str]:
        """Extract additional error details for debugging"""
        error_msg = str(error)
        
        # Extract YouTube video ID from error
        import re
        youtube_id_match = re.search(r'[a-zA-Z0-9_-]{11}', error_msg)
        if youtube_id_match:
            return f"YouTube video ID: {youtube_id_match.group()}"
            
        # Extract Google Drive file ID
        drive_id_match = re.search(r'[a-zA-Z0-9_-]{25,}', error_msg)
        if drive_id_match:
            return f"Google Drive file ID: {drive_id_match.group()}"
            
        # Extract HTTP status codes
        http_match = re.search(r'HTTP (\d{3})', error_msg)
        if http_match:
            return f"HTTP status: {http_match.group(1)}"
            
        return None
    
    def _log_error(self, error_context: ErrorContext):
        """Log error with appropriate level"""
        log_msg = error_context.to_user_message()
        
        # Use simple logging without extra parameter for compatibility
        if error_context.severity == ErrorSeverity.CRITICAL:
            self.logger.critical(log_msg)
        elif error_context.severity == ErrorSeverity.ERROR:
            self.logger.error(log_msg)
        elif error_context.severity == ErrorSeverity.WARNING:
            self.logger.warning(log_msg)
        else:
            self.logger.info(log_msg)
    
    def _track_error_pattern(self, error_context: ErrorContext):
        """Track error patterns for monitoring"""
        category_key = error_context.category.value
        self.error_counts[category_key] = self.error_counts.get(category_key, 0) + 1
        
        # Keep recent errors for analysis
        self.recent_errors.append(error_context)
        if len(self.recent_errors) > 100:  # Keep last 100 errors
            self.recent_errors.pop(0)
    
    def get_error_summary(self) -> Dict[str, Any]:
        """Get error summary for monitoring"""
        return {
            'total_errors': sum(self.error_counts.values()),
            'error_counts_by_category': self.error_counts.copy(),
            'recent_error_count': len(self.recent_errors),
            'critical_errors': len([e for e in self.recent_errors if e.severity == ErrorSeverity.CRITICAL]),
            'last_error': self.recent_errors[-1].to_dict() if self.recent_errors else None
        }
    
    def should_retry(self, error_context: ErrorContext, attempt_count: int, max_attempts: int = 3) -> bool:
        """Determine if operation should be retried based on error type"""
        if attempt_count >= max_attempts:
            return False
            
        # Retry network errors
        if error_context.category == ErrorCategory.NETWORK:
            return True
            
        # Retry rate limit errors with backoff
        if error_context.category == ErrorCategory.RATE_LIMIT:
            return True
            
        # Don't retry validation errors or permission errors
        if error_context.category in [ErrorCategory.VALIDATION, ErrorCategory.PERMISSION]:
            return False
            
        # Don't retry critical system errors
        if error_context.severity == ErrorSeverity.CRITICAL:
            return False
            
        # Retry other errors up to limit
        return True
    
    def get_retry_delay(self, error_context: ErrorContext, attempt_count: int) -> float:
        """Calculate retry delay based on error type"""
        base_delay = 2.0
        
        # Exponential backoff for rate limits
        if error_context.category == ErrorCategory.RATE_LIMIT:
            return min(base_delay * (2 ** attempt_count), 60.0)  # Max 60 seconds
            
        # Longer delay for network errors
        if error_context.category == ErrorCategory.NETWORK:
            return min(base_delay * (1.5 ** attempt_count), 30.0)  # Max 30 seconds
            
        # Standard delay for others
        return base_delay * attempt_count


# Standardized Error Handling Decorator (DRY)
import functools
from typing import Callable, Any, Union

def with_standard_error_handling(operation_name: str = None, return_on_error: Any = None, log_errors: bool = True):
    """Decorator for standardized error handling across the codebase
    
    Consolidates the common 'except Exception as e:' patterns into a reusable decorator.
    
    Args:
        operation_name: Name of the operation for error context (defaults to function name)
        return_on_error: Value to return when an error occurs (defaults to None)
        log_errors: Whether to log errors (defaults to True)
    
    Example:
        @with_standard_error_handling("Google Doc extraction", "")
        def extract_doc_text(url):
            # function implementation
            return extracted_text
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                # Use operation name or function name
                op_name = operation_name or func.__name__
                
                if log_errors:
                    # Create error handler for this operation
                    error_handler = ErrorHandler()
                    
                    # Build context from function arguments
                    context = {
                        'operation': op_name,
                        'function': func.__name__,
                        'args_count': len(args),
                        'kwargs_keys': list(kwargs.keys())
                    }
                    
                    # Add URL context if available in args/kwargs
                    for arg in args:
                        if isinstance(arg, str) and ('http' in arg or 'docs.google.com' in arg):
                            context['url'] = arg[:100]  # Truncate long URLs
                            break
                    
                    if 'url' in kwargs:
                        context['url'] = str(kwargs['url'])[:100]
                    
                    # Handle the error with full context
                    error_context = error_handler.handle_error(e, context)
                    
                    # Log with appropriate severity
                    if error_context.severity == ErrorSeverity.CRITICAL:
                        error_handler.logger.critical(error_context.to_user_message())
                    elif error_context.severity == ErrorSeverity.ERROR:
                        error_handler.logger.error(error_context.to_user_message())
                    elif error_context.severity == ErrorSeverity.WARNING:
                        error_handler.logger.warning(error_context.to_user_message())
                    else:
                        error_handler.logger.info(error_context.to_user_message())
                
                return return_on_error
                
        return wrapper
    return decorator


def validate_csv_integrity(csv_path: str) -> List[ErrorContext]:
    """Validate CSV file integrity"""
    errors = []
    error_handler = ErrorHandler()
    
    try:
        import pandas as pd
        from csv_tracker import safe_csv_read
        
        # Check if file exists
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"CSV file not found: {csv_path}")
            
        # Check if file is readable with standardized dtype specification
        df = safe_csv_read(csv_path, 'basic')
        
        # Check required columns
        required_columns = ['row_id', 'name', 'email', 'type']
        missing_columns = [col for col in required_columns if col not in df.columns]
        if missing_columns:
            raise ValueError(f"Missing required columns: {missing_columns}")
            
        # Check for duplicate row IDs
        if df['row_id'].duplicated().any():
            duplicates = df[df['row_id'].duplicated()]['row_id'].tolist()
            raise ValueError(f"Duplicate row IDs found: {duplicates}")
            
        # Check for empty critical fields
        for col in ['name', 'type']:
            empty_mask = df[col].isna() | (df[col].astype(str).str.strip() == '')
            if empty_mask.any():
                empty_rows = df[empty_mask]['row_id'].tolist()
                errors.append(error_handler.handle_error(
                    ValueError(f"Empty {col} field in rows: {empty_rows}"),
                    {'file_path': csv_path}
                ))
                
    except Exception as e:
        errors.append(error_handler.handle_error(e, {'file_path': csv_path}))
        
    return errors


def validate_download_environment() -> List[ErrorContext]:
    """Validate download environment and dependencies"""
    errors = []
    error_handler = ErrorHandler()
    
    try:
        # Check required directories
        required_dirs = ['youtube_downloads', 'drive_downloads']
        for dir_name in required_dirs:
            if not os.path.exists(dir_name):
                try:
                    os.makedirs(dir_name, exist_ok=True)
                except Exception as e:
                    errors.append(error_handler.handle_error(e, {'file_path': dir_name}))
                    
        # Check disk space (at least 1GB free)
        import shutil
        free_space = shutil.disk_usage('.').free
        if free_space < 1024 * 1024 * 1024:  # 1GB
            errors.append(error_handler.handle_error(
                RuntimeError(f"Low disk space: {free_space / (1024*1024*1024):.1f}GB free"),
                {}
            ))
            
        # Check yt-dlp availability
        try:
            import subprocess
            result = subprocess.run(['yt-dlp', '--version'], capture_output=True, text=True, timeout=10)
            if result.returncode != 0:
                raise RuntimeError("yt-dlp not working properly")
        except Exception as e:
            errors.append(error_handler.handle_error(e, {'download_type': 'youtube'}))
            
    except Exception as e:
        errors.append(error_handler.handle_error(e, {}))
        
    return errors


if __name__ == "__main__":
    """CLI interface for error handling utilities"""
    import argparse
    
    parser = argparse.ArgumentParser(description="Error Handling and Validation Utility")
    parser.add_argument('--validate-csv', type=str, metavar='CSV_PATH',
                       help='Validate CSV file integrity')
    parser.add_argument('--validate-environment', action='store_true',
                       help='Validate download environment')
    parser.add_argument('--test-error-handling', action='store_true',
                       help='Test error handling system')
    
    args = parser.parse_args()
    
    if args.validate_csv:
        errors = validate_csv_integrity(args.validate_csv)
        if errors:
            print(f"Found {len(errors)} CSV validation errors:")
            for error in errors:
                print(f"  • {error.to_user_message()}")
        else:
            print("✓ CSV validation passed")
            
    elif args.validate_environment:
        errors = validate_download_environment()
        if errors:
            print(f"Found {len(errors)} environment issues:")
            for error in errors:
                print(f"  • {error.to_user_message()}")
        else:
            print("✓ Environment validation passed")
            
    elif args.test_error_handling:
        error_handler = ErrorHandler()
        
        # Test different error types
        test_errors = [
            (ConnectionError("Network timeout"), {'url': 'https://youtube.com/test'}),
            (FileNotFoundError("Video file not found"), {'row_id': '123', 'download_type': 'youtube'}),
            (PermissionError("Access denied"), {'file_path': '/restricted/file.mp4'}),
            (ValueError("Invalid video ID"), {'row_id': '456'})
        ]
        
        print("Testing error handling:")
        for error, context in test_errors:
            error_context = error_handler.handle_error(error, context)
            print(f"  {error_context.category.value}: {error_context.message}")
            
        print(f"\nError summary: {error_handler.get_error_summary()}")
    else:
        parser.print_help()