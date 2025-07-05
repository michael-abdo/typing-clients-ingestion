import os
import sys
from pathlib import Path

print("🗑️ Final removal of empty temporary files...")
print("=" * 50)

# Change to the project directory  
os.chdir('/Users/Mike/ops_typing_log/ongoing_clients')

# List of empty temporary files to remove
temp_files = [
    'make_repo_private.py',
    'run_git_setup.py', 
    'GITHUB_SETUP_COMMANDS.md',
    'cleanup_temp_files.py',
    'temp_cleanup.py',
    'execute_cleanup.py',
    'final_cleanup.py',
    'simple_cleanup.py',
    'run_cleanup.py'
]

removed_count = 0
failed_count = 0

for file_name in temp_files:
    file_path = Path(file_name)
    if file_path.exists():
        try:
            # Check if file is empty or very small
            if file_path.stat().st_size <= 10:  # Less than 10 bytes (essentially empty)
                file_path.unlink()
                print(f"✅ Removed empty file: {file_name}")
                removed_count += 1
            else:
                print(f"⚠️  Skipped non-empty file: {file_name} ({file_path.stat().st_size} bytes)")
        except Exception as e:
            print(f"❌ Failed to remove {file_name}: {e}")
            failed_count += 1
    else:
        print(f"⚪ Not found: {file_name}")

print(f"\n📊 Cleanup Results:")
print(f"   ✅ Files removed: {removed_count}")
print(f"   ❌ Failed removals: {failed_count}")
print(f"\n🎉 Cleanup completed!")

# Don't remove this script itself since it provides the output
print(f"\n💡 Note: This script (final_remove.py) can be removed manually after verification.")