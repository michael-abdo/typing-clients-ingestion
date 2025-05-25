"""Streaming CSV operations for handling large files efficiently"""
import os
import csv
import tempfile
from pathlib import Path
from contextlib import contextmanager
try:
    from file_lock import file_lock
    from validation import sanitize_csv_value
except ImportError:
    from .file_lock import file_lock  
    from .validation import sanitize_csv_value


class StreamingCSVProcessor:
    """Process CSV files in chunks to avoid loading entire file into memory"""
    
    def __init__(self, chunk_size=1000):
        """
        Initialize streaming processor.
        
        Args:
            chunk_size: Number of rows to process at a time
        """
        self.chunk_size = chunk_size
    
    def process_csv_in_chunks(self, input_file, output_file, process_func, 
                            fieldnames=None, has_header=True, encoding='utf-8'):
        """
        Process a CSV file in chunks.
        
        Args:
            input_file: Path to input CSV
            output_file: Path to output CSV
            process_func: Function that takes a list of rows and returns processed rows
            fieldnames: CSV field names (if None, will be read from file)
            has_header: Whether the CSV has a header row
            encoding: File encoding
        
        Returns:
            Total number of rows processed
        """
        rows_processed = 0
        
        with open(input_file, 'r', encoding=encoding) as infile:
            reader = csv.DictReader(infile) if has_header else csv.reader(infile)
            
            # Get fieldnames from input
            original_fieldnames = reader.fieldnames if has_header else fieldnames
            
            # Process first chunk to determine output fieldnames
            first_chunk = []
            for i, row in enumerate(reader):
                first_chunk.append(row)
                if i >= self.chunk_size - 1:
                    break
            
            if first_chunk:
                # Process first chunk to get output fieldnames
                processed_first = process_func(first_chunk)
                if processed_first and isinstance(processed_first[0], dict):
                    output_fieldnames = list(processed_first[0].keys())
                else:
                    output_fieldnames = original_fieldnames
            else:
                output_fieldnames = original_fieldnames
            
            with open(output_file, 'w', newline='', encoding=encoding) as outfile:
                if has_header and output_fieldnames:
                    writer = csv.DictWriter(outfile, fieldnames=output_fieldnames)
                    writer.writeheader()
                    
                    # Write first processed chunk
                    for processed_row in processed_first:
                        writer.writerow(processed_row)
                    rows_processed += len(processed_first)
                else:
                    writer = csv.writer(outfile)
                
                # Process remaining chunks
                chunk = []
                for row in reader:
                    chunk.append(row)
                    
                    if len(chunk) >= self.chunk_size:
                        # Process chunk
                        processed_chunk = process_func(chunk)
                        for processed_row in processed_chunk:
                            writer.writerow(processed_row)
                        rows_processed += len(processed_chunk)
                        chunk = []
                
                # Process remaining rows
                if chunk:
                    processed_chunk = process_func(chunk)
                    for processed_row in processed_chunk:
                        writer.writerow(processed_row)
                    rows_processed += len(processed_chunk)
        
        return rows_processed


@contextmanager
def streaming_csv_update(filename, process_func, chunk_size=1000, 
                        fieldnames=None, encoding='utf-8', use_lock=True, timeout=30.0):
    """
    Update a CSV file using streaming to handle large files.
    
    Args:
        filename: Path to CSV file
        process_func: Function that processes a chunk of rows
        chunk_size: Number of rows to process at a time
        fieldnames: Field names (if None, read from file)
        encoding: File encoding
        use_lock: Whether to use file locking
        timeout: Lock timeout
    
    Yields:
        StreamingCSVProcessor instance
    
    Example:
        def add_processed_flag(rows):
            for row in rows:
                row['processed'] = 'yes'
            return rows
        
        with streaming_csv_update('data.csv', add_processed_flag) as processor:
            rows_processed = processor.process()
    """
    filename = Path(filename)
    
    if use_lock:
        lock = file_lock(filename, exclusive=True, timeout=timeout)
        lock.__enter__()
    else:
        lock = None
    
    try:
        # Create temp file
        fd, temp_path = tempfile.mkstemp(
            suffix='.tmp',
            prefix=f'.{filename.stem}_',
            dir=filename.parent
        )
        os.close(fd)
        temp_file = Path(temp_path)
        
        try:
            # Process the file
            processor = StreamingCSVProcessor(chunk_size)
            
            class ProcessorContext:
                def process(self):
                    return processor.process_csv_in_chunks(
                        filename, temp_file, process_func, 
                        fieldnames=fieldnames, encoding=encoding
                    )
            
            context = ProcessorContext()
            yield context
            
            # Atomic rename
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


def copy_csv_streaming(source_file, dest_file, chunk_size=1000, encoding='utf-8'):
    """
    Copy a CSV file using streaming to avoid loading entire file into memory.
    
    Args:
        source_file: Source CSV path
        dest_file: Destination CSV path
        chunk_size: Number of rows to copy at a time
        encoding: File encoding
    """
    with open(source_file, 'r', encoding=encoding) as src:
        reader = csv.DictReader(src)
        fieldnames = reader.fieldnames
        
        with open(dest_file, 'w', newline='', encoding=encoding) as dst:
            writer = csv.DictWriter(dst, fieldnames=fieldnames)
            writer.writeheader()
            
            # Copy in chunks
            chunk = []
            for row in reader:
                chunk.append(row)
                
                if len(chunk) >= chunk_size:
                    writer.writerows(chunk)
                    chunk = []
            
            # Write remaining rows
            if chunk:
                writer.writerows(chunk)


def filter_csv_streaming(input_file, output_file, filter_func, 
                        chunk_size=1000, encoding='utf-8'):
    """
    Filter a CSV file using streaming.
    
    Args:
        input_file: Input CSV path
        output_file: Output CSV path
        filter_func: Function that takes a row and returns True to keep it
        chunk_size: Number of rows to process at a time
        encoding: File encoding
    
    Returns:
        Tuple of (total_rows, kept_rows)
    """
    total_rows = 0
    kept_rows = 0
    
    with open(input_file, 'r', encoding=encoding) as src:
        reader = csv.DictReader(src)
        fieldnames = reader.fieldnames
        
        with open(output_file, 'w', newline='', encoding=encoding) as dst:
            writer = csv.DictWriter(dst, fieldnames=fieldnames)
            writer.writeheader()
            
            chunk = []
            for row in reader:
                total_rows += 1
                
                if filter_func(row):
                    chunk.append(row)
                    kept_rows += 1
                
                if len(chunk) >= chunk_size:
                    writer.writerows(chunk)
                    chunk = []
            
            # Write remaining rows
            if chunk:
                writer.writerows(chunk)
    
    return total_rows, kept_rows


def merge_csv_files_streaming(file_list, output_file, chunk_size=1000, encoding='utf-8'):
    """
    Merge multiple CSV files into one using streaming.
    
    Args:
        file_list: List of CSV file paths to merge
        output_file: Output file path
        chunk_size: Number of rows to process at a time
        encoding: File encoding
    
    Returns:
        Total number of rows merged
    """
    if not file_list:
        return 0
    
    total_rows = 0
    
    # Get fieldnames from first file
    with open(file_list[0], 'r', encoding=encoding) as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
    
    with open(output_file, 'w', newline='', encoding=encoding) as dst:
        writer = csv.DictWriter(dst, fieldnames=fieldnames)
        writer.writeheader()
        
        # Process each file
        for file_path in file_list:
            with open(file_path, 'r', encoding=encoding) as src:
                reader = csv.DictReader(src)
                
                chunk = []
                for row in reader:
                    # Ensure all fieldnames are present
                    clean_row = {field: row.get(field, '') for field in fieldnames}
                    chunk.append(clean_row)
                    total_rows += 1
                    
                    if len(chunk) >= chunk_size:
                        writer.writerows(chunk)
                        chunk = []
                
                # Write remaining rows
                if chunk:
                    writer.writerows(chunk)
    
    return total_rows


