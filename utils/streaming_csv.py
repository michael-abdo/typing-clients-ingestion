"""
DEPRECATED: Streaming CSV operations - USE csv_manager.py INSTEAD

This module is deprecated. All functionality has been moved to csv_manager.py
for better consolidation and DRY principle adherence.

Use: from utils.csv_manager import CSVManager, streaming_csv_update
"""
import warnings
warnings.warn(
    "streaming_csv.py is deprecated. Use csv_manager.CSVManager for all CSV operations.",
    DeprecationWarning, 
    stacklevel=2
)

# Import consolidated functionality
from .csv_manager import (
    CSVManager,
    streaming_csv_update,
    safe_csv_read,
    safe_csv_write
)

# Backward compatibility aliases
StreamingCSVProcessor = CSVManager


def copy_csv_streaming(source_file, dest_file, chunk_size=1000, encoding='utf-8'):
    """DEPRECATED: Use CSVManager.stream_process() with copy function instead"""
    warnings.warn("copy_csv_streaming is deprecated. Use CSVManager.stream_process()", DeprecationWarning)
    
    def copy_func(chunk):
        return chunk  # Pass through unchanged
    
    manager = CSVManager(csv_path=source_file, chunk_size=chunk_size, encoding=encoding)
    return manager.stream_process(copy_func, output_path=dest_file)


def filter_csv_streaming(input_file, output_file, filter_func, chunk_size=1000, encoding='utf-8'):
    """DEPRECATED: Use CSVManager.stream_filter() instead"""
    warnings.warn("filter_csv_streaming is deprecated. Use CSVManager.stream_filter()", DeprecationWarning)
    manager = CSVManager(csv_path=input_file, chunk_size=chunk_size, encoding=encoding)
    return manager.stream_filter(filter_func, output_file)


def merge_csv_files_streaming(file_list, output_file, chunk_size=1000, encoding='utf-8'):
    """DEPRECATED: Use CSVManager with custom merge logic instead"""
    warnings.warn("merge_csv_files_streaming is deprecated. Use CSVManager with custom merge logic", DeprecationWarning)
    
    # Simple implementation - read all files and combine
    all_rows = []
    for file_path in file_list:
        try:
            import pandas as pd
            df = pd.read_csv(file_path, encoding=encoding)
            all_rows.extend(df.to_dict('records'))
        except Exception as e:
            print(f"Warning: Could not read {file_path}: {e}")
    
    manager = CSVManager(csv_path=output_file, chunk_size=chunk_size, encoding=encoding)
    return manager.atomic_write(all_rows)