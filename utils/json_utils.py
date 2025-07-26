#!/usr/bin/env python3
"""
DRY JSON State Management Utilities
Consolidates JSON file operations and state management patterns.
"""

import json
import os
from pathlib import Path
from typing import Any, Dict, Optional, Union
from datetime import datetime

def read_json_safe(file_path: Union[str, Path], default: Any = None) -> Any:
    """
    Safely read JSON file with error handling.
    
    Args:
        file_path: Path to JSON file
        default: Default value if file doesn't exist or is invalid
        
    Returns:
        Parsed JSON data or default value
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return default

def write_json_safe(file_path: Union[str, Path], data: Any, indent: int = 2, ensure_dir: bool = True) -> bool:
    """
    Safely write JSON file with error handling.
    
    Args:
        file_path: Path to JSON file
        data: Data to write
        indent: JSON indentation
        ensure_dir: Create parent directory if needed
        
    Returns:
        True if successful, False otherwise
    """
    try:
        file_path = Path(file_path)
        
        if ensure_dir:
            file_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(file_path, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=indent, ensure_ascii=False)
        return True
    except (OSError, TypeError):
        return False

def update_json_state(file_path: Union[str, Path], updates: Dict[str, Any], create_if_missing: bool = True) -> bool:
    """
    Update JSON state file with new data.
    
    Args:
        file_path: Path to JSON state file
        updates: Dictionary of updates to apply
        create_if_missing: Create file if it doesn't exist
        
    Returns:
        True if successful, False otherwise
    """
    current_data = read_json_safe(file_path, {} if create_if_missing else None)
    
    if current_data is None:
        return False
    
    current_data.update(updates)
    return write_json_safe(file_path, current_data)

def create_timestamped_state(base_data: Dict[str, Any], timestamp_key: str = 'timestamp') -> Dict[str, Any]:
    """
    Create state dictionary with timestamp.
    
    Args:
        base_data: Base state data
        timestamp_key: Key for timestamp field
        
    Returns:
        State dictionary with timestamp
    """
    state = base_data.copy()
    state[timestamp_key] = datetime.now().isoformat()
    return state

def load_progress_state(file_path: Union[str, Path]) -> Dict[str, Any]:
    """
    Load progress state with default structure.
    
    Args:
        file_path: Path to progress file
        
    Returns:
        Progress state dictionary
    """
    default_state = {
        'started_at': datetime.now().isoformat(),
        'completed': {},
        'failed': {},
        'skipped': {},
        'total_processed': 0,
        'last_updated': datetime.now().isoformat()
    }
    
    return read_json_safe(file_path, default_state)

def save_progress_state(file_path: Union[str, Path], state: Dict[str, Any]) -> bool:
    """
    Save progress state with updated timestamp.
    
    Args:
        file_path: Path to progress file
        state: Progress state to save
        
    Returns:
        True if successful, False otherwise
    """
    state['last_updated'] = datetime.now().isoformat()
    return write_json_safe(file_path, state)

def create_report_structure(title: str, summary: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Create standardized report structure.
    
    Args:
        title: Report title
        summary: Optional summary data
        
    Returns:
        Report structure dictionary
    """
    return {
        'title': title,
        'generated_at': datetime.now().isoformat(),
        'summary': summary or {},
        'details': [],
        'errors': [],
        'warnings': []
    }

def append_to_json_array(file_path: Union[str, Path], item: Any, max_items: Optional[int] = None) -> bool:
    """
    Append item to JSON array file.
    
    Args:
        file_path: Path to JSON array file
        item: Item to append
        max_items: Maximum items to keep (removes oldest)
        
    Returns:
        True if successful, False otherwise
    """
    current_array = read_json_safe(file_path, [])
    
    if not isinstance(current_array, list):
        current_array = []
    
    current_array.append(item)
    
    if max_items and len(current_array) > max_items:
        current_array = current_array[-max_items:]
    
    return write_json_safe(file_path, current_array)