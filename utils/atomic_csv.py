"""Atomic CSV operations to prevent data corruption"""
import os
import csv
import tempfile
from pathlib import Path
from contextlib import contextmanager
from validation import sanitize_csv_value


class AtomicCSVWriter:
    """Context manager for atomic CSV writes"""
    
    def __init__(self, filename, mode='w', fieldnames=None, encoding='utf-8'):
        self.filename = Path(filename)
        self.mode = mode
        self.fieldnames = fieldnames
        self.encoding = encoding
        self.temp_file = None
        self.writer = None
        self.file_handle = None
    
    def __enter__(self):
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
def atomic_csv_update(filename, fieldnames=None, encoding='utf-8'):
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
    # Read existing data first
    rows = []
    if Path(filename).exists():
        with open(filename, 'r', encoding=encoding) as f:
            reader = csv.DictReader(f)
            rows = list(reader)
            if not fieldnames and reader.fieldnames:
                fieldnames = reader.fieldnames
    
    # Create atomic writer
    with AtomicCSVWriter(filename, mode='w', fieldnames=fieldnames, encoding=encoding) as writer:
        yield rows, writer


def write_csv_atomic(filename, rows, fieldnames=None, encoding='utf-8'):
    """
    Write CSV file atomically
    
    Args:
        filename: Path to CSV file
        rows: List of rows (dicts or lists)
        fieldnames: Field names for DictWriter (required for dict rows)
        encoding: File encoding
    """
    with AtomicCSVWriter(filename, mode='w', fieldnames=fieldnames, encoding=encoding) as writer:
        if fieldnames:
            writer.writeheader()
        writer.writerows(rows)


def append_csv_atomic(filename, rows, fieldnames=None, encoding='utf-8'):
    """
    Append to CSV file atomically
    
    Args:
        filename: Path to CSV file
        rows: List of rows to append
        fieldnames: Field names for DictWriter
        encoding: File encoding
    """
    # For append, we need to check if file exists and has content
    write_header = not Path(filename).exists() or Path(filename).stat().st_size == 0
    
    with AtomicCSVWriter(filename, mode='a', fieldnames=fieldnames, encoding=encoding) as writer:
        if write_header and fieldnames:
            writer.writeheader()
        writer.writerows(rows)


# Example usage
if __name__ == "__main__":
    # Test atomic write
    test_data = [
        {"name": "Alice", "age": "30", "email": "alice@example.com"},
        {"name": "Bob", "age": "25", "email": "bob@example.com"},
        {"name": "=cmd.exe", "age": "99", "email": "'; DROP TABLE users;--"}  # Test sanitization
    ]
    
    print("Testing atomic CSV write...")
    write_csv_atomic("test_atomic.csv", test_data, fieldnames=["name", "age", "email"])
    print("Write complete. Check test_atomic.csv")
    
    # Test atomic update
    print("\nTesting atomic CSV update...")
    with atomic_csv_update("test_atomic.csv") as (existing, writer):
        writer.writeheader()
        for row in existing:
            row['age'] = str(int(row['age']) + 1)  # Increment age
            writer.writerow(row)
        # Add a new row
        writer.writerow({"name": "Charlie", "age": "35", "email": "charlie@example.com"})
    
    print("Update complete. Check test_atomic.csv")