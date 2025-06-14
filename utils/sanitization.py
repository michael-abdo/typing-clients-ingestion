#!/usr/bin/env python3
"""
Sanitization utilities to prevent CSV corruption from external web content.

This module provides functions to safely clean and validate data before
inserting it into CSV files, preventing corruption from HTML, JavaScript,
and other potentially dangerous content.

Created to address critical security vulnerability where Google Drive HTML
responses corrupted CSV data structure.
"""

import re
import html
from typing import Optional, Union, Any

# Compile regex patterns once for performance
HTML_TAG_PATTERN = re.compile(r'<[^>]*>')
JAVASCRIPT_PATTERN = re.compile(r'[\{\[\}\]"\'`]')
JSON_OBJECT_PATTERN = re.compile(r'\{[^}]*\}|\[[^\]]*\]')
CONTROL_CHAR_PATTERN = re.compile(r'[\x00-\x1f\x7f-\x9f]')
WINDOW_GLOBAL_PATTERN = re.compile(r'window\.[A-Za-z_][A-Za-z0-9_]*\s*=')
FUNCTION_CALL_PATTERN = re.compile(r'[A-Za-z_][A-Za-z0-9_]*\s*\([^)]*\)')


def sanitize_csv_field(value: Any, max_length: int = 200) -> str:
    """
    Sanitize input for safe CSV storage by removing HTML, JavaScript,
    and other content that could corrupt CSV structure.
    
    Args:
        value: Input value to sanitize (will be converted to string)
        max_length: Maximum allowed length for the sanitized field
        
    Returns:
        Sanitized string safe for CSV insertion
        
    Examples:
        >>> sanitize_csv_field('<script>alert("test")</script>')
        'alert(test)'
        
        >>> sanitize_csv_field('Error: {"config": [1,2,3]}')
        'Error: config: 1,2,3'
        
        >>> sanitize_csv_field('Long error' * 100, max_length=50)
        'Long errorLong errorLong errorLong errorLong ...'
    """
    # Handle None and non-string inputs
    if value is None:
        return ""
    
    if not isinstance(value, str):
        value = str(value)
    
    if not value.strip():
        return ""
    
    # Step 1: Remove HTML tags and decode HTML entities
    value = HTML_TAG_PATTERN.sub('', value)
    value = html.unescape(value)
    
    # Step 2: Remove specific JavaScript/Google Drive patterns
    value = WINDOW_GLOBAL_PATTERN.sub('', value)
    value = FUNCTION_CALL_PATTERN.sub('', value)
    
    # Step 3: Simplify JSON objects to basic key-value representation
    value = JSON_OBJECT_PATTERN.sub(lambda m: _simplify_json_like(m.group()), value)
    
    # Step 4: Remove remaining JavaScript/JSON syntax characters
    value = JAVASCRIPT_PATTERN.sub('', value)
    
    # Step 5: Remove control characters and non-printable characters
    value = CONTROL_CHAR_PATTERN.sub('', value)
    
    # Step 6: Escape CSV-dangerous characters
    value = _escape_csv_characters(value)
    
    # Step 7: Limit length and clean up whitespace
    value = ' '.join(value.split())  # Normalize whitespace
    
    if len(value) > max_length:
        value = value[:max_length-3] + "..."
    
    return value.strip()


def _simplify_json_like(json_str: str) -> str:
    """
    Simplify JSON-like strings to basic readable format.
    
    Converts {"key": "value", "key2": [1,2,3]} to "key: value, key2: 1,2,3"
    """
    try:
        # Remove brackets and braces
        simplified = json_str.strip('{}[]')
        
        # Replace quotes around keys/values
        simplified = re.sub(r'"([^"]*)":', r'\1:', simplified)
        simplified = re.sub(r':"([^"]*)"', r': \1', simplified)
        
        # Clean up remaining quotes and brackets
        simplified = simplified.replace('"', '').replace("'", '')
        simplified = re.sub(r'\[[^\]]*\]', lambda m: m.group().strip('[]'), simplified)
        
        # Limit length of simplified JSON
        if len(simplified) > 100:
            simplified = simplified[:97] + "..."
            
        return simplified
        
    except Exception:
        # If simplification fails, just remove the problematic content
        return ""


def _escape_csv_characters(value: str) -> str:
    """
    Escape characters that could break CSV structure.
    
    Replaces:
    - Commas with semicolons
    - Newlines and carriage returns with spaces
    - Multiple consecutive spaces with single spaces
    """
    # Replace CSV-breaking characters
    value = value.replace(',', ';')
    value = value.replace('\n', ' ')
    value = value.replace('\r', ' ')
    value = value.replace('\t', ' ')
    
    # Normalize multiple spaces
    value = re.sub(r'\s+', ' ', value)
    
    return value


def validate_csv_field_safety(value: str) -> bool:
    """
    Validate that a field is safe for CSV insertion.
    
    Returns False if the field contains patterns that could corrupt CSV:
    - HTML tags
    - JavaScript code
    - JSON objects
    - Control characters
    - Excessive length
    
    Args:
        value: String to validate
        
    Returns:
        True if safe, False if potentially dangerous
    """
    if not isinstance(value, str):
        return False
    
    # Check for dangerous patterns
    dangerous_patterns = [
        r'<[^>]*>',  # HTML tags
        r'window\.[A-Za-z_]',  # JavaScript window objects
        r'\{[^}]*\}',  # JSON objects
        r'[\x00-\x1f\x7f-\x9f]',  # Control characters
        r'.{500,}',  # Excessive length
    ]
    
    for pattern in dangerous_patterns:
        if re.search(pattern, value):
            return False
    
    return True


def sanitize_error_message(error: Union[str, Exception], max_length: int = 200) -> str:
    """
    Specialized sanitization for error messages.
    
    Extracts meaningful error information while removing dangerous content.
    
    Args:
        error: Error message or Exception object
        max_length: Maximum length for sanitized error
        
    Returns:
        Clean, safe error message
    """
    if isinstance(error, Exception):
        error_str = str(error)
    else:
        error_str = str(error) if error else "Unknown error"
    
    # Extract common error patterns and preserve meaningful parts
    error_str = _extract_meaningful_error(error_str)
    
    # Apply standard sanitization
    return sanitize_csv_field(error_str, max_length)


def _extract_meaningful_error(error_str: str) -> str:
    """
    Extract meaningful parts from error messages while removing noise.
    
    Preserves:
    - HTTP status codes
    - File paths
    - Common error types
    - User-friendly messages
    
    Removes:
    - Stack traces
    - Raw HTML/JavaScript
    - Internal system details
    """
    # Common meaningful error patterns
    meaningful_patterns = [
        r'HTTP \d{3}',  # HTTP status codes
        r'[Ff]ile not found',
        r'[Aa]ccess denied',
        r'[Pp]ermission denied',
        r'[Cc]onnection refused',
        r'[Tt]imeout',
        r'[Nn]ot found',
        r'[Uu]navailable',
        r'[Pp]rivate',
        r'[Rr]equires authentication',
    ]
    
    # Extract meaningful parts
    meaningful_parts = []
    for pattern in meaningful_patterns:
        matches = re.findall(pattern, error_str, re.IGNORECASE)
        meaningful_parts.extend(matches)
    
    if meaningful_parts:
        return " ".join(meaningful_parts[:3])  # Limit to first 3 meaningful parts
    
    # If no meaningful patterns found, return a cleaned version of the start
    words = error_str.split()[:10]  # First 10 words
    return " ".join(words)


# Exception class for safe error handling
class SafeDownloadError(Exception):
    """
    Exception class that automatically sanitizes error messages to prevent
    CSV corruption from external web content.
    """
    
    def __init__(self, message: str, error_type: str = "download_error", original_error: Optional[Exception] = None):
        """
        Initialize SafeDownloadError with automatic message sanitization.
        
        Args:
            message: Error message (will be sanitized)
            error_type: Category of error (network, permission, parsing, etc.)
            original_error: Original exception that caused this error
        """
        self.safe_message = sanitize_error_message(message)
        self.error_type = error_type
        self.original_error = original_error
        
        super().__init__(self.safe_message)
    
    def __str__(self) -> str:
        return self.safe_message
    
    def get_error_summary(self) -> dict:
        """
        Get structured error information safe for logging/CSV insertion.
        
        Returns:
            Dictionary with sanitized error details
        """
        return {
            'message': self.safe_message,
            'type': self.error_type,
            'safe': True
        }


# Quick test function
if __name__ == "__main__":
    # Test with the type of content that corrupted row 492
    test_cases = [
        '<script>window.WIZ_global_data = {"config": [1,2,3]};</script>',
        'Error: {"folders": ["test"], "files": []}',
        'HTTP 403 <html><body>Access denied</body></html>',
        'Normal error message',
        None,
        "",
        "Very " * 100 + "long error message",
    ]
    
    print("Testing sanitization function:")
    for i, test in enumerate(test_cases):
        result = sanitize_csv_field(test)
        print(f"{i+1}. Input: {repr(test)[:50]}...")
        print(f"   Output: {repr(result)}")
        print(f"   Safe: {validate_csv_field_safety(result)}")
        print()