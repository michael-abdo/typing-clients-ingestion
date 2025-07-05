#!/usr/bin/env python3
"""
Centralized Error Messages - Standardized error message templates
Consolidates all hardcoded error messages across the codebase for consistency
"""

from typing import Dict, Any


class ErrorMessages:
    """Centralized error message templates with consistent formatting"""
    
    # Network-related errors
    NETWORK = {
        'CONNECTION_TIMEOUT': "Connection timeout after {timeout}s for {resource}",
        'NETWORK_ERROR': "Network error accessing {resource}: {details}",
        'HTTP_ERROR': "HTTP {status} error: {message}",
        'DNS_ERROR': "DNS resolution failed for {domain}: {details}",
        'SSL_ERROR': "SSL/TLS error connecting to {host}: {details}",
        'RATE_LIMITED': "Rate limit exceeded for {service}: {details}"
    }
    
    # File operation errors  
    FILE_OPERATIONS = {
        'FILE_NOT_FOUND': "File not found: {path}",
        'PERMISSION_DENIED': "Permission denied accessing {path}",
        'ACCESS_DENIED': "Access denied to {resource}",
        'DISK_FULL': "Insufficient disk space for {operation}",
        'FILE_LOCKED': "File is locked and cannot be accessed: {path}",
        'INVALID_PATH': "Invalid file path: {path}",
        'COPY_FAILED': "Failed to copy {source} to {destination}: {error}",
        'DELETE_FAILED': "Failed to delete {path}: {error}",
        'WRITE_FAILED': "Failed to write to {path}: {error}",
        'READ_FAILED': "Failed to read from {path}: {error}"
    }
    
    # Validation errors
    VALIDATION = {
        'INVALID_URL': "Invalid URL format: {url}",
        'INVALID_FORMAT': "Invalid {type} format: {value}",
        'VALIDATION_FAILED': "Validation failed for {field}: {reason}",
        'MISSING_REQUIRED': "Required field missing: {field}",
        'VALUE_OUT_OF_RANGE': "{field} value {value} is out of valid range ({min_val}-{max_val})",
        'INVALID_CSV_FORMAT': "Invalid CSV format in {file}: {details}",
        'INVALID_JSON': "Invalid JSON in {file}: {details}",
        'SCHEMA_VALIDATION_FAILED': "Schema validation failed: {details}"
    }
    
    # Download-specific errors
    DOWNLOAD = {
        'DOWNLOAD_FAILED': "Download failed for {url}: {error}",
        'YOUTUBE_ERROR': "YouTube download error for {video_id}: {details}",
        'DRIVE_ERROR': "Google Drive download error for {file_id}: {details}",
        'QUOTA_EXCEEDED': "Download quota exceeded for {service}",
        'VIDEO_UNAVAILABLE': "Video unavailable: {video_id}",
        'PLAYLIST_EMPTY': "Playlist is empty or inaccessible: {playlist_url}",
        'SUBTITLE_UNAVAILABLE': "Subtitles not available for {video_id} in language {lang}",
        'FORMAT_UNAVAILABLE': "Requested format {format} not available for {video_id}"
    }
    
    # CSV processing errors
    CSV_PROCESSING = {
        'CSV_READ_ERROR': "Failed to read CSV file {path}: {error}",
        'CSV_WRITE_ERROR': "Failed to write CSV file {path}: {error}",
        'CSV_CORRUPTION': "CSV file corruption detected in {path}: {details}",
        'CSV_BACKUP_FAILED': "Failed to create CSV backup for {path}: {error}",
        'CSV_VALIDATION_FAILED': "CSV validation failed for {path}: {details}",
        'ROW_NOT_FOUND': "Row {row_id} not found in CSV",
        'COLUMN_MISSING': "Required column {column} not found in CSV",
        'DUPLICATE_ROW_ID': "Duplicate row_id {row_id} found in CSV"
    }
    
    # System and process errors
    SYSTEM = {
        'MEMORY_ERROR': "Insufficient memory for {operation}",
        'PROCESS_FAILED': "Process {process} failed with exit code {code}",
        'TIMEOUT_ERROR': "Operation {operation} timed out after {timeout}s",
        'RESOURCE_UNAVAILABLE': "Required resource {resource} is unavailable",
        'DEPENDENCY_MISSING': "Required dependency {dependency} is not installed",
        'CONFIG_ERROR': "Configuration error: {details}",
        'INITIALIZATION_FAILED': "Failed to initialize {component}: {error}"
    }
    
    # Database/persistence errors
    PERSISTENCE = {
        'DATABASE_ERROR': "Database error: {details}",
        'BACKUP_FAILED': "Backup operation failed: {details}",
        'RESTORE_FAILED': "Restore operation failed: {details}",
        'SYNC_ERROR': "Synchronization error: {details}",
        'LOCK_TIMEOUT': "Failed to acquire lock for {resource} within {timeout}s"
    }
    
    # Authentication and authorization
    AUTH = {
        'AUTH_FAILED': "Authentication failed for {service}: {details}",
        'TOKEN_EXPIRED': "Access token expired for {service}",
        'INVALID_CREDENTIALS': "Invalid credentials for {service}",
        'PERMISSION_DENIED': "Permission denied for {operation}",
        'QUOTA_EXCEEDED': "API quota exceeded for {service}"
    }
    
    @staticmethod
    def format_error(category: str, error_type: str, **kwargs) -> str:
        """
        Format error message with provided context
        
        Args:
            category: Error category (NETWORK, FILE_OPERATIONS, etc.)
            error_type: Specific error type within category
            **kwargs: Template variables for string formatting
            
        Returns:
            Formatted error message
            
        Example:
            ErrorMessages.format_error('FILE_OPERATIONS', 'FILE_NOT_FOUND', path='/tmp/missing.txt')
            # Returns: "File not found: /tmp/missing.txt"
        """
        category_dict = getattr(ErrorMessages, category.upper(), {})
        template = category_dict.get(error_type.upper())
        
        if template is None:
            return f"Unknown error: {category}.{error_type}"
        
        try:
            return template.format(**kwargs)
        except KeyError as e:
            return f"Error message template missing variable {e}: {template}"
    
    @staticmethod
    def get_all_categories() -> Dict[str, Dict[str, str]]:
        """Get all error message categories and their templates"""
        return {
            'NETWORK': ErrorMessages.NETWORK,
            'FILE_OPERATIONS': ErrorMessages.FILE_OPERATIONS,
            'VALIDATION': ErrorMessages.VALIDATION,
            'DOWNLOAD': ErrorMessages.DOWNLOAD,
            'CSV_PROCESSING': ErrorMessages.CSV_PROCESSING,
            'SYSTEM': ErrorMessages.SYSTEM,
            'PERSISTENCE': ErrorMessages.PERSISTENCE,
            'AUTH': ErrorMessages.AUTH
        }
    
    @staticmethod
    def search_messages(keyword: str) -> Dict[str, str]:
        """Search for error messages containing a keyword"""
        results = {}
        all_categories = ErrorMessages.get_all_categories()
        
        for category_name, category_messages in all_categories.items():
            for error_type, template in category_messages.items():
                if keyword.lower() in template.lower():
                    results[f"{category_name}.{error_type}"] = template
        
        return results


# Convenience functions for common error patterns
def network_error(error_type: str, **kwargs) -> str:
    """Format network-related error message"""
    return ErrorMessages.format_error('NETWORK', error_type, **kwargs)


def file_error(error_type: str, **kwargs) -> str:
    """Format file operation error message"""
    return ErrorMessages.format_error('FILE_OPERATIONS', error_type, **kwargs)


def validation_error(error_type: str, **kwargs) -> str:
    """Format validation error message"""
    return ErrorMessages.format_error('VALIDATION', error_type, **kwargs)


def download_error(error_type: str, **kwargs) -> str:
    """Format download-related error message"""
    return ErrorMessages.format_error('DOWNLOAD', error_type, **kwargs)


def csv_error(error_type: str, **kwargs) -> str:
    """Format CSV processing error message"""
    return ErrorMessages.format_error('CSV_PROCESSING', error_type, **kwargs)


def system_error(error_type: str, **kwargs) -> str:
    """Format system/process error message"""
    return ErrorMessages.format_error('SYSTEM', error_type, **kwargs)


if __name__ == "__main__":
    # Example usage and testing
    print("=== Error Message System Test ===")
    
    # Test different error types
    print("\nNetwork errors:")
    print(network_error('CONNECTION_TIMEOUT', timeout=30, resource='YouTube API'))
    print(network_error('HTTP_ERROR', status=404, message='Not Found'))
    
    print("\nFile errors:")
    print(file_error('FILE_NOT_FOUND', path='/tmp/missing.csv'))
    print(file_error('PERMISSION_DENIED', path='/restricted/file.txt'))
    
    print("\nDownload errors:")
    print(download_error('YOUTUBE_ERROR', video_id='abc123', details='Video is private'))
    print(download_error('QUOTA_EXCEEDED', service='Google Drive'))
    
    print("\nValidation errors:")
    print(validation_error('INVALID_URL', url='not-a-url'))
    print(validation_error('MISSING_REQUIRED', field='email'))
    
    print("\nCSV errors:")
    print(csv_error('ROW_NOT_FOUND', row_id='123'))
    print(csv_error('CSV_CORRUPTION', path='output.csv', details='Invalid column count'))
    
    print("\nSearch test:")
    timeout_messages = ErrorMessages.search_messages('timeout')
    for key, message in timeout_messages.items():
        print(f"{key}: {message}")