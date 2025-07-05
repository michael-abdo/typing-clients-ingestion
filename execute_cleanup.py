#!/usr/bin/env python3
"""
Execute cleanup of temporary files
"""

import os
from pathlib import Path

# Change to the correct directory
os.chdir('/Users/Mike/ops_typing_log/ongoing_clients')

# List of temporary files to remove
temp_files = [
    'make_repo_private.py',
    'run_git_setup.py', 
    'GITHUB_SETUP_COMMANDS.md',
    'cleanup_temp_files.py'
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

print(f'\n🎯 Summary:')
print(f'   Files removed: {len(removed_files)}')

if removed_files:
    print(f'   Removed files:')
    for file in removed_files:
        print(f'   - {file}')

print(f'\n✨ Cleanup complete!')

# Now remove this script itself
script_path = Path(__file__)
try:
    script_path.unlink()
    print(f'✅ Removed: execute_cleanup.py')
except Exception as e:
    print(f'❌ Failed to remove execute_cleanup.py: {e}')