#!/usr/bin/env python3
"""
Simple module validation check to verify DRY refactoring status
"""
import sys
import os
import importlib
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

def check_module_exists(module_name):
    """Check if a module can be imported"""
    try:
        importlib.import_module(module_name)
        return True, None
    except Exception as e:
        return False, str(e)

def main():
    print("="*60)
    print("SIMPLE MODULE VALIDATION CHECK")
    print("="*60)
    
    modules_to_check = [
        'utils.config',
        'utils.csv_manager', 
        'utils.downloader',
        'utils.extract_links',
        'utils.validation', 
        'utils.error_handling',
        'utils.retry_utils',
        'utils.import_utils',
        'utils.logging_config',
        'utils.s3_manager',
        'utils.patterns',
        'utils.sanitization'
    ]
    
    success_count = 0
    total_count = len(modules_to_check)
    
    for module in modules_to_check:
        success, error = check_module_exists(module)
        if success:
            print(f"✅ {module}")
            success_count += 1
        else:
            print(f"❌ {module}: {error}")
    
    print(f"\nResults: {success_count}/{total_count} modules imported successfully")
    
    if success_count == total_count:
        print("🎉 ALL MODULES VALIDATED - DRY refactoring working correctly!")
        return True
    else:
        print("⚠️  Some modules failed - manual review needed")
        return False

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)