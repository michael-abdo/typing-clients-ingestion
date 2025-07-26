#!/usr/bin/env python3
"""
DRY Error Message Formatting Utilities
Consolidates error message formatting and display patterns.
"""

from typing import Optional, Dict, Any
from datetime import datetime

def format_error_message(error: Exception, context: Optional[str] = None, include_type: bool = True) -> str:
    """
    Format error message consistently.
    
    Args:
        error: Exception object
        context: Optional context description
        include_type: Include exception type in message
        
    Returns:
        Formatted error message
    """
    error_msg = str(error)
    
    if include_type:
        error_type = type(error).__name__
        error_msg = f"{error_type}: {error_msg}"
    
    if context:
        error_msg = f"{context} - {error_msg}"
    
    return error_msg

def format_success_message(action: str, details: Optional[str] = None) -> str:
    """
    Format success message consistently.
    
    Args:
        action: Action that succeeded
        details: Optional additional details
        
    Returns:
        Formatted success message
    """
    message = f"✅ {action}"
    if details:
        message += f": {details}"
    return message

def format_warning_message(warning: str, context: Optional[str] = None) -> str:
    """
    Format warning message consistently.
    
    Args:
        warning: Warning message
        context: Optional context
        
    Returns:
        Formatted warning message
    """
    message = f"⚠️ {warning}"
    if context:
        message = f"⚠️ {context}: {warning}"
    return message

def format_failure_message(action: str, reason: Optional[str] = None) -> str:
    """
    Format failure message consistently.
    
    Args:
        action: Action that failed
        reason: Optional reason for failure
        
    Returns:
        Formatted failure message
    """
    message = f"❌ Failed: {action}"
    if reason:
        message += f" - {reason}"
    return message

def format_progress_message(current: int, total: int, item: Optional[str] = None) -> str:
    """
    Format progress message consistently.
    
    Args:
        current: Current item number
        total: Total items
        item: Optional item description
        
    Returns:
        Formatted progress message
    """
    percentage = (current / total * 100) if total > 0 else 0
    message = f"📊 Progress: {current}/{total} ({percentage:.1f}%)"
    if item:
        message += f" - {item}"
    return message

def format_processing_message(action: str, item: str) -> str:
    """
    Format processing message consistently.
    
    Args:
        action: Action being performed
        item: Item being processed
        
    Returns:
        Formatted processing message
    """
    return f"📤 {action}: {item}"

def format_completion_message(action: str, count: int, duration: Optional[float] = None) -> str:
    """
    Format completion message consistently.
    
    Args:
        action: Action that completed
        count: Number of items processed
        duration: Optional duration in seconds
        
    Returns:
        Formatted completion message
    """
    message = f"🎉 {action} completed: {count} items"
    if duration:
        message += f" in {duration:.2f}s"
    return message

def create_error_summary(errors: list, title: str = "Error Summary") -> Dict[str, Any]:
    """
    Create standardized error summary.
    
    Args:
        errors: List of error entries
        title: Summary title
        
    Returns:
        Error summary dictionary
    """
    return {
        'title': title,
        'timestamp': datetime.now().isoformat(),
        'total_errors': len(errors),
        'errors': errors
    }

def format_file_operation_error(operation: str, file_path: str, error: Exception) -> str:
    """
    Format file operation error message.
    
    Args:
        operation: File operation (read, write, delete, etc.)
        file_path: Path to file
        error: Exception that occurred
        
    Returns:
        Formatted error message
    """
    return format_error_message(error, f"File {operation} failed for {file_path}")

def format_network_error(url: str, error: Exception, context: Optional[str] = None) -> str:
    """
    Format network operation error message.
    
    Args:
        url: URL that failed
        error: Exception that occurred
        context: Optional context
        
    Returns:
        Formatted error message
    """
    context_str = f"{context} for" if context else "Network request failed for"
    return format_error_message(error, f"{context_str} {url}")

def format_validation_error(field: str, value: Any, reason: str) -> str:
    """
    Format validation error message.
    
    Args:
        field: Field that failed validation
        value: Value that was invalid
        reason: Reason for validation failure
        
    Returns:
        Formatted validation error message
    """
    return f"❌ Validation failed for {field}='{value}': {reason}"