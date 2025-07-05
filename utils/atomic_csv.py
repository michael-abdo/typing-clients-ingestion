"""
DEPRECATED: Atomic CSV operations - USE csv_manager.py INSTEAD

This module is deprecated. All functionality has been moved to csv_manager.py
for better consolidation and DRY principle adherence.

Use: from utils.csv_manager import CSVManager, atomic_csv_update
"""
import warnings
warnings.warn(
    "atomic_csv.py is deprecated. Use csv_manager.CSVManager for all CSV operations.",
    DeprecationWarning, 
    stacklevel=2
)

# Import consolidated functionality
from .csv_manager import (
    CSVManager,
    atomic_csv_update,
    safe_csv_read,
    safe_csv_write
)

# Backward compatibility aliases
AtomicCSVWriter = CSVManager


def write_csv_atomic(filename, rows, fieldnames=None, encoding='utf-8', use_lock=True, timeout=30.0):
    """DEPRECATED: Use CSVManager.atomic_write() instead"""
    warnings.warn("write_csv_atomic is deprecated. Use CSVManager.atomic_write()", DeprecationWarning)
    manager = CSVManager(csv_path=filename, use_file_lock=use_lock, timeout=timeout, encoding=encoding)
    return manager.atomic_write(rows, fieldnames)


def append_csv_atomic(filename, rows, fieldnames=None, encoding='utf-8', use_lock=True, timeout=30.0):
    """DEPRECATED: Use CSVManager.atomic_append() instead"""
    warnings.warn("append_csv_atomic is deprecated. Use CSVManager.atomic_append()", DeprecationWarning)
    manager = CSVManager(csv_path=filename, use_file_lock=use_lock, timeout=timeout, encoding=encoding)
    return manager.atomic_append(rows, fieldnames)


def prepend_csv_atomic(filename, new_rows, fieldnames=None, encoding='utf-8', use_lock=True, timeout=30.0):
    """DEPRECATED: Use CSVManager.atomic_write() with combined data instead"""
    warnings.warn("prepend_csv_atomic is deprecated. Use CSVManager.atomic_write() with combined data", DeprecationWarning)
    manager = CSVManager(csv_path=filename, use_file_lock=use_lock, timeout=timeout, encoding=encoding)
    
    # Read existing data
    import pandas as pd
    try:
        existing_df = pd.read_csv(filename)
        existing_rows = existing_df.to_dict('records')
    except:
        existing_rows = []
    
    # Combine new rows + existing rows
    all_rows = new_rows + existing_rows
    return manager.atomic_write(all_rows, fieldnames)