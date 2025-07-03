#!/usr/bin/env python3
"""Run minimal test to verify system functionality after duplicate removal"""

import subprocess
import sys

# First run our duplicate removal verification
print("=" * 60)
print("RUNNING DUPLICATE REMOVAL VERIFICATION")
print("=" * 60)
result = subprocess.run([sys.executable, "test_duplicate_removal.py"], capture_output=True, text=True)
print(result.stdout)
if result.stderr:
    print("STDERR:", result.stderr)
if result.returncode != 0:
    print("❌ Duplicate removal test failed!")
    sys.exit(1)

# Now run a minimal version of the comprehensive tests
print("\n" + "=" * 60)
print("RUNNING MINIMAL FUNCTIONALITY TESTS")
print("=" * 60)

# Test critical imports
print("\nTesting critical imports...")
critical_imports = [
    "from utils.csv_tracker import safe_get_na_value, update_csv_download_status",
    "from utils.download_drive import download_drive_with_context", 
    "from utils.download_youtube import download_youtube_with_context",
    "from utils.config import get_config, create_download_dir",
]

all_passed = True
for imp in critical_imports:
    result = subprocess.run([sys.executable, "-c", f"{imp}; print('OK')"], capture_output=True, text=True)
    if result.returncode == 0:
        print(f"✅ {imp.split('import')[1].strip()}")
    else:
        print(f"❌ Failed: {imp}")
        print(f"   Error: {result.stderr}")
        all_passed = False

# Test CSV tracker status
print("\nTesting CSV tracker...")
result = subprocess.run([sys.executable, "utils/csv_tracker.py", "--status"], capture_output=True, text=True)
if result.returncode == 0:
    print("✅ CSV tracker status works")
else:
    print("❌ CSV tracker status failed")
    all_passed = False

# Test monitoring status
print("\nTesting monitoring...")
result = subprocess.run([sys.executable, "utils/monitoring.py", "--status"], capture_output=True, text=True)
if result.returncode == 0:
    print("✅ Monitoring status works")
else:
    print("❌ Monitoring status failed")
    all_passed = False

if all_passed:
    print("\n✅ All critical tests passed! The system is functional after duplicate removal.")
else:
    print("\n❌ Some tests failed. Please review the errors above.")
    sys.exit(1)