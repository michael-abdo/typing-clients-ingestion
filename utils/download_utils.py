#\!/usr/bin/env python3
"""
Centralized Download Utilities (DRY Consolidation)

Eliminates duplicate download patterns across multiple files.
"""

import sys
import time
import logging
from pathlib import Path
from typing import Union, Optional, Any

try:
    from config import get_download_chunk_size, Constants
except ImportError:
    from .config import get_download_chunk_size, Constants


def download_file_with_progress(response, output_path: Union[str, Path], 
                               total_size: int = 0, logger: Optional[Any] = None,
                               show_progress: bool = True) -> bool:
    """
    Centralized file download function with progress tracking (DRY consolidation).
    
    Args:
        response: HTTP response object with iter_content method
        output_path: Path where file should be saved
        total_size: Total file size in bytes (0 if unknown)
        logger: Optional logger instance
        show_progress: Whether to display progress bar
        
    Returns:
        bool: True if download succeeded, False otherwise
    """
    if logger is None:
        logger = logging.getLogger(__name__)
    
    output_path = Path(output_path)
    temp_path = Path(f"{output_path}.tmp")
    
    try:
        # Get adaptive chunk size (DRY: using centralized function)
        chunk_size = get_download_chunk_size(total_size) if total_size > 0 else 1048576
        
        with open(temp_path, 'wb') as f:
            downloaded = 0
            start_time = time.time()
            last_progress_time = start_time
            
            for chunk in response.iter_content(chunk_size=chunk_size):
                if chunk:
                    f.write(chunk)
                    downloaded += len(chunk)
                    
                    # Show progress (consolidated display logic)
                    if show_progress and total_size > 0:
                        current_time = time.time()
                        if (current_time - last_progress_time) >= 1.0:
                            progress_percent = (downloaded / total_size) * 100
                            speed_mbps = downloaded / (current_time - start_time) / Constants.BYTES_PER_MB if (current_time - start_time) > 0 else 0
                            
                            # Progress bar format
                            progress_bar_width = Constants.DEFAULT_PROGRESS_BAR_WIDTH
                            filled_length = int(progress_bar_width * downloaded // total_size)
                            bar = '=' * filled_length + '-' * (progress_bar_width - filled_length)
                            
                            sys.stdout.write(f'\r[{bar}] {progress_percent:.1f}%  < /dev/null |  {downloaded // Constants.BYTES_PER_MB}/{total_size // Constants.BYTES_PER_MB} MB | {speed_mbps:.1f} MB/s')
                            sys.stdout.flush()
                            last_progress_time = current_time
        
        # Atomic move to final location
        temp_path.rename(output_path)
        
        if show_progress and total_size > 0:
            sys.stdout.write('\n')  # New line after progress bar
            
        if total_size > 0:
            logger.info(f"Download completed: {output_path} ({total_size // (1024*1024)} MB)")
        else:
            logger.info(f"Download completed: {output_path}")
            
        return True
        
    except Exception as e:
        logger.error(f"Download failed: {e}")
        if temp_path.exists():
            temp_path.unlink()
        return False


def get_adaptive_chunk_size(file_size: int) -> int:
    """Get adaptive chunk size for file downloads (DRY wrapper)."""
    return get_download_chunk_size(file_size)
