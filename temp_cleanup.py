#!/usr/bin/env python3
"""
Temporary cleanup script to remove temporary files
"""

import os
from pathlib import Path

def cleanup():
    # List of temporary files to remove
    temp_files = [
        'make_repo_private.py',
        'run_git_setup.py', 
        'GITHUB_SETUP_COMMANDS.md'
    ]
    
    removed_files = []
    
    print('🧹 Cleaning up temporary files...')
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
    
    # Remove cleanup_temp_files.py if it exists
    cleanup_script = Path('cleanup_temp_files.py')
    if cleanup_script.exists():
        try:
            cleanup_script.unlink()
            removed_files.append('cleanup_temp_files.py')
            print(f'✅ Removed: cleanup_temp_files.py')
        except Exception as e:
            print(f'❌ Failed to remove cleanup_temp_files.py: {e}')
    
    print(f'\n🎯 Summary:')
    print(f'   Files removed: {len(removed_files)}')
    
    if removed_files:
        print(f'   Removed files:')
        for file in removed_files:
            print(f'   - {file}')
    
    print(f'\n✨ Cleanup complete!')
    
    # Remove this script last
    print(f'\n🗑️  Removing cleanup script itself...')
    try:
        Path(__file__).unlink()
        print(f'✅ Removed: temp_cleanup.py')
    except Exception as e:
        print(f'❌ Failed to remove cleanup script: {e}')

if __name__ == "__main__":
    cleanup()