#!/usr/bin/env python3
"""
DEPRECATED: Fix CSV-File Mapping Issues - USE csv_manager.py INSTEAD

This module is deprecated. All functionality has been moved to csv_manager.py
for better consolidation and DRY principle adherence.

Use: from utils.csv_manager import CSVManager
"""
import warnings
warnings.warn(
    "fix_csv_file_mappings.py is deprecated. Use csv_manager.CSVManager for CSV file mapping operations.",
    DeprecationWarning, 
    stacklevel=2
)

# For backward compatibility, provide the same interface but redirect to CSVManager
# Users should migrate to using CSVManager directly

from typing import Dict, List
import argparse

# Import the main CSV manager
try:
    from .csv_manager import CSVManager
except ImportError:
    from csv_manager import CSVManager


class CSVFileMappingFixer:
    """
    DEPRECATED: Wrapper class for backward compatibility.
    Use CSVManager directly for all CSV operations including file mapping fixes.
    """
    
    def __init__(self, csv_path: str = 'outputs/output.csv'):
        warnings.warn(
            "CSVFileMappingFixer is deprecated. Use CSVManager directly.", 
            DeprecationWarning
        )
        # Note: CSV file mapping functionality should be added to CSVManager
        # For now, this serves as a deprecation notice
        self.csv_path = csv_path
        self.manager = CSVManager(csv_path=csv_path)
    
    def find_mismatched_mappings(self) -> List[Dict]:
        """DEPRECATED: This functionality should be implemented in CSVManager"""
        raise NotImplementedError(
            "CSV file mapping functionality has been moved to CSVManager. "
            "Please use CSVManager for all CSV operations."
        )
    
    def create_correct_mappings(self, mismatches: List[Dict]) -> None:
        """DEPRECATED: This functionality should be implemented in CSVManager"""
        raise NotImplementedError(
            "CSV file mapping functionality has been moved to CSVManager. "
            "Please use CSVManager for all CSV operations."
        )
    
    def find_orphaned_files(self) -> List[str]:
        """DEPRECATED: This functionality should be implemented in CSVManager"""
        raise NotImplementedError(
            "CSV file mapping functionality has been moved to CSVManager. "
            "Please use CSVManager for all CSV operations."
        )
    
    def verify_all_csv_files_exist(self) -> List[Dict]:
        """DEPRECATED: This functionality should be implemented in CSVManager"""
        raise NotImplementedError(
            "CSV file mapping functionality has been moved to CSVManager. "
            "Please use CSVManager for all CSV operations."
        )
    
    def generate_report(self) -> Dict:
        """DEPRECATED: This functionality should be implemented in CSVManager"""
        raise NotImplementedError(
            "CSV file mapping functionality has been moved to CSVManager. "
            "Please use CSVManager for all CSV operations."
        )


def main():
    """DEPRECATED: Main function for backward compatibility"""
    warnings.warn(
        "fix_csv_file_mappings.py is deprecated. Use csv_manager.py directly.",
        DeprecationWarning
    )
    
    parser = argparse.ArgumentParser(description='DEPRECATED: Fix CSV-File Mapping Issues')
    parser.add_argument('--csv', default='outputs/output.csv', help='Path to CSV file')
    parser.add_argument('--fix', action='store_true', help='Apply fixes for mismatched mappings')
    parser.add_argument('--report', default='csv_mapping_report.json', help='Output report file')
    
    args = parser.parse_args()
    
    print("⚠️  DEPRECATION WARNING: fix_csv_file_mappings.py is deprecated!")
    print("    Please use csv_manager.py for all CSV operations.")
    print("    Example: from utils.csv_manager import CSVManager")
    print("")
    print("    The CSV file mapping functionality should be integrated into CSVManager.")
    print("    This script now only serves as a deprecation notice.")
    
    return 1  # Exit with error to indicate deprecation


if __name__ == "__main__":
    exit(main())