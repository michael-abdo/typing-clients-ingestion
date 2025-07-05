#!/usr/bin/env python3
"""
Final cleanup - remove all temporary files
"""

import os
import subprocess
import sys
from pathlib import Path

def main():
    # Change to the correct directory
    os.chdir('/Users/Mike/ops_typing_log/ongoing_clients')
    
    # List of temporary files to remove
    temp_files = [
        'make_repo_private.py',
        'run_git_setup.py', 
        'GITHUB_SETUP_COMMANDS.md',
        'cleanup_temp_files.py',
        'temp_cleanup.py',
        'execute_cleanup.py'
    ]
    
    removed_files = []
    
    print('🧹 Final cleanup of temporary files...')
    print('=' * 40)
    
    # Remove individual files
    for file_name in temp_files:
        file_path = Path(file_name)
        if file_path.exists():
            try:
                file_path.unlink()
                removed_files.append(file_name)
                print(f'✅ Removed: {file_name}')
            except Exception as e:
                print(f'❌ Failed to remove {file_name}: {e}')
        else:
            print(f'⚪ Not found: {file_name}')
    
    print(f'\n🎯 Summary:')
    print(f'   Files removed: {len(removed_files)}')
    
    if removed_files:
        print(f'   Removed files:')
        for file in removed_files:
            print(f'   - {file}')
    
    print(f'\n✨ Final cleanup complete!')
    
    # Clean up run_cleanup.py if it exists
    run_cleanup = Path('run_cleanup.py')
    if run_cleanup.exists():
        try:
            run_cleanup.unlink()
            print(f'✅ Removed: run_cleanup.py')
        except Exception as e:
            print(f'❌ Failed to remove run_cleanup.py: {e}')

if __name__ == "__main__":
    main()