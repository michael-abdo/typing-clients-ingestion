import subprocess
import sys

# Run the test
result = subprocess.run([sys.executable, "test_create_download_dir.py"], capture_output=True, text=True)
print(result.stdout)
if result.stderr:
    print("STDERR:", result.stderr)
print(f"Exit code: {result.returncode}")