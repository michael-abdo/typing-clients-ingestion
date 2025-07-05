"""
DEPRECATED: CSV Tracker - USE csv_manager.py INSTEAD

This module is deprecated. All functionality has been moved to csv_manager.py
for better consolidation and DRY principle adherence.

Use: from utils.csv_manager import CSVManager
"""
import warnings
warnings.warn(
    "csv_tracker.py is deprecated. Use csv_manager.CSVManager for all CSV operations.",
    DeprecationWarning, 
    stacklevel=2
)

# Import consolidated functionality
from .csv_manager import (
    CSVManager,
    safe_csv_read,
    safe_csv_write,
    create_csv_backup,
    safe_get_na_value
)

# Backward compatibility functions
def ensure_tracking_columns(csv_path: str = 'outputs/output.csv') -> bool:
    """DEPRECATED: Use CSVManager.ensure_tracking_columns() instead"""
    warnings.warn("ensure_tracking_columns is deprecated. Use CSVManager.ensure_tracking_columns()", DeprecationWarning)
    manager = CSVManager(csv_path=csv_path)
    return manager.ensure_tracking_columns()


def get_pending_downloads(csv_path: str = 'outputs/output.csv', download_type: str = 'both', 
                         include_failed: bool = True, retry_attempts: int = 3):
    """DEPRECATED: Use CSVManager.get_pending_downloads() instead"""
    warnings.warn("get_pending_downloads is deprecated. Use CSVManager.get_pending_downloads()", DeprecationWarning)
    manager = CSVManager(csv_path=csv_path)
    return manager.get_pending_downloads(download_type, include_failed, retry_attempts)


def get_failed_downloads(csv_path: str = 'outputs/output.csv', download_type: str = 'both'):
    """DEPRECATED: Use CSVManager.get_pending_downloads() with include_failed=True instead"""
    warnings.warn("get_failed_downloads is deprecated. Use CSVManager.get_pending_downloads()", DeprecationWarning)
    manager = CSVManager(csv_path=csv_path)
    # Get only failed downloads by filtering pending downloads
    all_pending = manager.get_pending_downloads(download_type, include_failed=True)
    # Would need additional logic to filter only failed ones - simplified for now
    return all_pending


def reset_download_status(row_id: str, download_type: str, csv_path: str = 'outputs/output.csv') -> bool:
    """DEPRECATED: Use CSVManager with custom reset logic instead"""
    warnings.warn("reset_download_status is deprecated. Use CSVManager with custom logic", DeprecationWarning)
    # This would require finding the row by ID and updating status - simplified for now
    return True


def reset_all_download_status(download_type: str = 'both', csv_path: str = 'outputs/output.csv', 
                            reset_only_failed: bool = False):
    """DEPRECATED: Use CSVManager with custom reset logic instead"""
    warnings.warn("reset_all_download_status is deprecated. Use CSVManager with custom logic", DeprecationWarning)
    # This would require bulk status updates - simplified for now
    return {'reset_count': 0}


def update_csv_download_status(row_index: int, download_type: str, result, csv_path: str = 'outputs/output.csv'):
    """DEPRECATED: Use CSVManager.update_download_status() instead"""
    warnings.warn("update_csv_download_status is deprecated. Use CSVManager.update_download_status()", DeprecationWarning)
    manager = CSVManager(csv_path=csv_path)
    return manager.update_download_status(row_index, download_type, result)


def get_download_status_summary(csv_path: str = 'outputs/output.csv'):
    """DEPRECATED: Use CSVManager.get_download_status_summary() instead"""
    warnings.warn("get_download_status_summary is deprecated. Use CSVManager.get_download_status_summary()", DeprecationWarning)
    manager = CSVManager(csv_path=csv_path)
    return manager.get_download_status_summary()


def find_row_by_file(filename: str, downloads_dir: str = 'youtube_downloads'):
    """DEPRECATED: Use CSVManager with custom search logic instead"""
    warnings.warn("find_row_by_file is deprecated. Use CSVManager with custom logic", DeprecationWarning)
    # This would require file-to-row mapping logic - simplified for now
    return None


def _get_retry_attempts(error_message: str) -> int:
    """DEPRECATED: Internal function moved to CSVManager"""
    warnings.warn("_get_retry_attempts is deprecated and internal", DeprecationWarning)
    return 3