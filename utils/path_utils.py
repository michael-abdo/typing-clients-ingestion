#!/usr/bin/env python3
"""
DRY Path Utilities
Consolidates file path creation and management patterns.
"""

import os
from pathlib import Path
from typing import Union, Optional

def ensure_directory(path: Union[str, Path], exist_ok: bool = True) -> Path:
    """
    Create directory if it doesn't exist.
    
    Args:
        path: Directory path to create
        exist_ok: Don't raise error if directory already exists
    
    Returns:
        Path object of the created directory
    """
    path_obj = Path(path)
    path_obj.mkdir(parents=True, exist_ok=exist_ok)
    return path_obj

def create_download_directory(row_id: int, person_name: str, base_dir: str = "downloads") -> Path:
    """
    Create standardized download directory for a person.
    
    Args:
        row_id: Row identifier
        person_name: Person's name
        base_dir: Base downloads directory
    
    Returns:
        Path to the created directory
    """
    clean_name = person_name.replace(' ', '_').replace('/', '_').replace('\\', '_')
    dir_name = f"{row_id}_{clean_name}"
    full_path = Path(base_dir) / dir_name
    return ensure_directory(full_path)

def create_cache_directory(cache_name: str = "cache") -> Path:
    """
    Create cache directory in standard location.
    
    Args:
        cache_name: Name of the cache directory
    
    Returns:
        Path to the cache directory
    """
    return ensure_directory(cache_name)

def safe_filename(filename: str) -> str:
    """
    Clean filename to be filesystem-safe.
    
    Args:
        filename: Original filename
    
    Returns:
        Safe filename string
    """
    # Remove/replace unsafe characters
    unsafe_chars = '<>:"/\\|?*'
    safe_name = filename
    for char in unsafe_chars:
        safe_name = safe_name.replace(char, '_')
    
    # Limit length
    if len(safe_name) > 255:
        name, ext = os.path.splitext(safe_name)
        safe_name = name[:255-len(ext)] + ext
    
    return safe_name

def get_file_extension(file_path: Union[str, Path]) -> str:
    """
    Get file extension in lowercase.
    
    Args:
        file_path: Path to file
    
    Returns:
        File extension (with dot) in lowercase
    """
    return Path(file_path).suffix.lower()

def create_timestamped_filename(base_name: str, extension: str, timestamp: Optional[str] = None) -> str:
    """
    Create filename with timestamp.
    
    Args:
        base_name: Base name for file
        extension: File extension (with or without dot)
        timestamp: Optional timestamp string (defaults to current time)
    
    Returns:
        Timestamped filename
    """
    if timestamp is None:
        from datetime import datetime
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    if not extension.startswith('.'):
        extension = '.' + extension
    
    return f"{base_name}_{timestamp}{extension}"