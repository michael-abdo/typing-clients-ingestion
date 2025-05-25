"""Atomic CSV operations to prevent data corruption"""
import os
import csv
import tempfile
from pathlib import Path
from contextlib import contextmanager
try:
    from validation import sanitize_csv_value
except ImportError:
    from .validation import sanitize_csv_value
try:
    from file_lock import file_lock, atomic_write_with_lock
except ImportError:
    from .file_lock import file_lock, atomic_write_with_lock


class AtomicCSVWriter:
    """Context manager for atomic CSV writes with file locking"""
    
    def __init__(self, filename, mode='w', fieldnames=None, encoding='utf-8', use_lock=True, timeout=30.0):
        self.filename = Path(filename)
        self.mode = mode
        self.fieldnames = fieldnames
        self.encoding = encoding
        self.use_lock = use_lock
        self.timeout = timeout
        self.temp_file = None
        self.writer = None
        self.file_handle = None
        self.lock = None
    
    def __enter__(self):
        # Acquire file lock if requested
        if self.use_lock:
            self.lock = file_lock(self.filename, exclusive=True, timeout=self.timeout)
            self.lock.__enter__()
        
        if self.mode == 'w':
            # For write mode, create a temp file
            fd, temp_path = tempfile.mkstemp(
                suffix='.tmp',
                prefix=f'.{self.filename.stem}_',
                dir=self.filename.parent
            )
            os.close(fd)  # Close the file descriptor
            self.temp_file = Path(temp_path)
            self.file_handle = open(self.temp_file, 'w', newline='', encoding=self.encoding)
        elif self.mode == 'a':
            # For append mode, copy existing file to temp first
            fd, temp_path = tempfile.mkstemp(
                suffix='.tmp',
                prefix=f'.{self.filename.stem}_',
                dir=self.filename.parent
            )
            os.close(fd)
            self.temp_file = Path(temp_path)
            
            # Copy existing content if file exists
            if self.filename.exists():
                with open(self.filename, 'r', encoding=self.encoding) as src:
                    with open(self.temp_file, 'w', newline='', encoding=self.encoding) as dst:
                        dst.write(src.read())
            
            self.file_handle = open(self.temp_file, 'a', newline='', encoding=self.encoding)
        else:
            raise ValueError(f"Unsupported mode: {self.mode}")
        
        # Create CSV writer
        if self.fieldnames:
            self.writer = csv.DictWriter(self.file_handle, fieldnames=self.fieldnames)
        else:
            self.writer = csv.writer(self.file_handle)
        
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        try:
            self.file_handle.close()
            
            if exc_type is None:
                # No exception, perform atomic rename
                try:
                    # On Windows, we need to remove the target first
                    if os.name == 'nt' and self.filename.exists():
                        self.filename.unlink()
                    self.temp_file.rename(self.filename)
                except Exception:
                    # If rename fails, try os.replace (more atomic)
                    os.replace(str(self.temp_file), str(self.filename))
            else:
                # Exception occurred, clean up temp file
                if self.temp_file and self.temp_file.exists():
                    self.temp_file.unlink()
        finally:
            # Release file lock
            if self.lock:
                self.lock.__exit__(None, None, None)
        
        return False  # Don't suppress exceptions
    
    def writeheader(self):
        """Write CSV header"""
        if hasattr(self.writer, 'writeheader'):
            self.writer.writeheader()
    
    def writerow(self, row):
        """Write a single row with sanitization"""
        if isinstance(row, dict):
            # Sanitize dictionary values
            sanitized = {k: sanitize_csv_value(v) for k, v in row.items()}
            self.writer.writerow(sanitized)
        else:
            # Sanitize list values
            sanitized = [sanitize_csv_value(v) for v in row]
            self.writer.writerow(sanitized)
    
    def writerows(self, rows):
        """Write multiple rows with sanitization"""
        for row in rows:
            self.writerow(row)


@contextmanager
def atomic_csv_update(filename, fieldnames=None, encoding='utf-8', use_lock=True, timeout=30.0):
    """
    Context manager for atomic CSV updates (read and write)
    
    Usage:
        with atomic_csv_update('data.csv', fieldnames=['name', 'value']) as (reader, writer):
            # Read existing data
            existing_data = list(reader)
            
            # Write header
            writer.writeheader()
            
            # Write modified data
            for row in existing_data:
                row['value'] = int(row['value']) + 1
                writer.writerow(row)
    """
    filename = Path(filename)
    
    if use_lock:
        # Acquire exclusive lock for the entire operation
        lock = file_lock(filename, exclusive=True, timeout=timeout)
        lock.__enter__()
    else:
        lock = None
    
    try:
        # Read existing data while holding the lock
        rows = []
        if filename.exists():
            with open(filename, 'r', encoding=encoding) as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                if not fieldnames and reader.fieldnames:
                    fieldnames = reader.fieldnames
        
        # Create temp file for writing
        fd, temp_path = tempfile.mkstemp(
            suffix='.tmp',
            prefix=f'.{filename.stem}_',
            dir=filename.parent
        )
        os.close(fd)
        temp_file = Path(temp_path)
        
        try:
            # Open temp file for writing
            with open(temp_file, 'w', newline='', encoding=encoding) as f:
                if fieldnames:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                else:
                    writer = csv.writer(f)
                
                # Yield rows and writer
                yield rows, writer
            
            # If we get here, no exception occurred - do atomic rename
            os.replace(str(temp_file), str(filename))
            
        except Exception:
            # Clean up temp file on error
            if temp_file.exists():
                temp_file.unlink()
            raise
            
    finally:
        # Release lock
        if lock:
            lock.__exit__(None, None, None)


def write_csv_atomic(filename, rows, fieldnames=None, encoding='utf-8', use_lock=True, timeout=30.0):
    """
    Write CSV file atomically
    
    Args:
        filename: Path to CSV file
        rows: List of rows (dicts or lists)
        fieldnames: Field names for DictWriter (required for dict rows)
        encoding: File encoding
        use_lock: Whether to use file locking
        timeout: Lock acquisition timeout
    """
    with AtomicCSVWriter(filename, mode='w', fieldnames=fieldnames, encoding=encoding, use_lock=use_lock, timeout=timeout) as writer:
        if fieldnames:
            writer.writeheader()
        writer.writerows(rows)


def append_csv_atomic(filename, rows, fieldnames=None, encoding='utf-8', use_lock=True, timeout=30.0):
    """
    Append to CSV file atomically
    
    Args:
        filename: Path to CSV file
        rows: List of rows to append
        fieldnames: Field names for DictWriter
        encoding: File encoding
        use_lock: Whether to use file locking
        timeout: Lock acquisition timeout
    """
    # For append, we need to check if file exists and has content
    write_header = not Path(filename).exists() or Path(filename).stat().st_size == 0
    
    with AtomicCSVWriter(filename, mode='a', fieldnames=fieldnames, encoding=encoding, use_lock=use_lock, timeout=timeout) as writer:
        if write_header and fieldnames:
            writer.writeheader()
        writer.writerows(rows)


