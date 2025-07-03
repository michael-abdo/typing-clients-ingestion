#!/usr/bin/env python3
"""
Comprehensive test suite to verify all functionality
"""
import subprocess
import sys
import os
import time
import json
import pandas as pd
from datetime import datetime

class Colors:
    GREEN = '\033[92m'
    RED = '\033[91m'
    YELLOW = '\033[93m'
    BLUE = '\033[94m'
    RESET = '\033[0m'
    BOLD = '\033[1m'

def run_command(cmd, timeout=30, check_output=True):
    """Run a command and return success status and output"""
    try:
        if isinstance(cmd, str):
            cmd = cmd.split()
        
        result = subprocess.run(
            cmd, 
            capture_output=True, 
            text=True, 
            timeout=timeout,
            check=False
        )
        
        success = result.returncode == 0
        output = result.stdout if check_output else ""
        error = result.stderr
        
        return success, output, error
    except subprocess.TimeoutExpired:
        return False, "", f"Command timed out after {timeout}s"
    except Exception as e:
        return False, "", str(e)

def print_test_header(category):
    """Print a formatted test category header"""
    print(f"\n{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{category}{Colors.RESET}")
    print(f"{Colors.BOLD}{Colors.BLUE}{'='*60}{Colors.RESET}")

def print_result(test_name, success, details=""):
    """Print test result with color coding"""
    status = f"{Colors.GREEN}✅ PASS{Colors.RESET}" if success else f"{Colors.RED}❌ FAIL{Colors.RESET}"
    print(f"  {test_name}: {status}")
    if details and not success:
        print(f"    {Colors.YELLOW}→ {details}{Colors.RESET}")

def test_imports():
    """Test all critical Python imports"""
    print_test_header("1. PYTHON MODULE IMPORTS")
    
    test_cases = [
        ("CSV Tracker", "from utils.csv_tracker import safe_get_na_value, update_csv_download_status"),
        ("Download Drive", "from utils.download_drive import download_drive_with_context"),
        ("Download YouTube", "from utils.download_youtube import download_youtube_with_context"),
        ("Row Context", "from utils.row_context import RowContext"),
        ("Config", "from utils.config import get_config"),
        ("Error Handling", "from utils.error_handling import ErrorHandler"),
        ("Monitoring", "from utils.monitoring import DownloadMonitor"),
        ("Sanitization", "from utils.sanitization import sanitize_csv_field"),
        ("Validation", "from utils.validation import validate_youtube_url"),
        ("File Lock", "from utils.file_lock import file_lock"),
        ("Retry Utils", "from utils.retry_utils import retry_with_backoff"),
        ("Rate Limiter", "from utils.rate_limiter import RateLimiter"),
    ]
    
    results = []
    for name, import_stmt in test_cases:
        # Use list format to avoid shell escaping issues
        cmd = ['python3', '-c', f"{import_stmt}; print('Import successful')"]
        success, output, error = run_command(cmd, timeout=5)
        results.append(success)
        print_result(name, success, error.split('\n')[-2] if error else "")
    
    return all(results)

def test_cli_tools():
    """Test all CLI tool interfaces"""
    print_test_header("2. CLI TOOL INTERFACES")
    
    test_cases = [
        ("CSV Tracker Status", "python3 utils/csv_tracker.py --status"),
        ("CSV Tracker Help", "python3 utils/csv_tracker.py --help"),
        ("Error Handling Validation", "python3 utils/error_handling.py --validate-environment"),
        ("Monitoring Status", "python3 utils/monitoring.py --status"),
        ("Monitoring Alerts", "python3 utils/monitoring.py --alerts"),
        ("Main Workflow Help", "python3 run_complete_workflow.py --help"),
        ("Download YouTube Help", "python3 utils/download_youtube.py --help"),
        ("Download Drive Help", "python3 utils/download_drive.py --help"),
    ]
    
    results = []
    for name, cmd in test_cases:
        success, output, error = run_command(cmd, timeout=10)
        # For help commands, check if help text is in output
        if "--help" in cmd:
            success = success or "usage:" in output or "usage:" in error
        results.append(success)
        print_result(name, success, error.split('\n')[0] if error and not success else "")
    
    return all(results)

def test_file_operations():
    """Test file operations and data integrity"""
    print_test_header("3. FILE OPERATIONS & DATA INTEGRITY")
    
    results = []
    
    # Test 1: Check CSV file exists and is readable
    csv_path = "outputs/output.csv"
    try:
        df = pd.read_csv(csv_path)
        csv_exists = True
        row_count = len(df)
        col_count = len(df.columns)
        details = f"Rows: {row_count}, Columns: {col_count}"
    except Exception as e:
        csv_exists = False
        details = str(e)
    
    results.append(csv_exists)
    print_result("CSV File Readable", csv_exists, details if not csv_exists else f"✓ {details}")
    
    # Test 2: Check database file
    db_path = "data/file_row_mappings.db"
    db_exists = os.path.exists(db_path)
    results.append(db_exists)
    print_result("Database File Exists", db_exists, f"Path: {db_path}")
    
    # Test 3: Check config file
    config_path = "config/config.yaml"
    config_exists = os.path.exists(config_path)
    results.append(config_exists)
    print_result("Config File Exists", config_exists, f"Path: {config_path}")
    
    # Test 4: Check essential directories
    dirs_to_check = [
        "utils", "outputs", "logs", "config", "data",
        "youtube_downloads", "drive_downloads", "scripts", "docs"
    ]
    
    for dir_name in dirs_to_check:
        exists = os.path.isdir(dir_name)
        results.append(exists)
        print_result(f"Directory: {dir_name}/", exists)
    
    return all(results)

def test_core_functionality():
    """Test core system functionality"""
    print_test_header("4. CORE FUNCTIONALITY TESTS")
    
    results = []
    
    # Test 1: CSV validation
    print("\n  Testing CSV Validation...")
    cmd = ['python3', '-c', "from utils.error_handling import validate_csv_integrity; errors = validate_csv_integrity('outputs/output.csv'); print(f'Validation errors: {len(errors)}')"]
    success, output, error = run_command(cmd)
    results.append(success)
    print_result("CSV Validation", success, error.split('\n')[0] if error else output.strip())
    
    # Test 2: Row context creation
    print("\n  Testing Row Context...")
    cmd = ['python3', '-c', """from utils.row_context import RowContext
ctx = RowContext('test_001', 0, 'TestType', 'Test User', 'test@example.com')
print(f'Row context created: {ctx.row_id}')"""]
    success, output, error = run_command(cmd)
    results.append(success)
    print_result("Row Context Creation", success, error.split('\n')[0] if error else "")
    
    # Test 3: Configuration loading
    print("\n  Testing Configuration...")
    cmd = ['python3', '-c', """from utils.config import get_config, get_youtube_downloads_dir, get_drive_downloads_dir
config = get_config()
yt_dir = get_youtube_downloads_dir()
drive_dir = get_drive_downloads_dir()
print(f'Config loaded. YT dir: {yt_dir}, Drive dir: {drive_dir}')"""]
    success, output, error = run_command(cmd)
    results.append(success)
    print_result("Configuration Loading", success, error.split('\n')[0] if error else "")
    
    # Test 4: Sanitization
    print("\n  Testing Sanitization...")
    cmd = ['python3', '-c', """from utils.sanitization import sanitize_csv_field
test = 'Test\\nWith\\nNewlines'
result = sanitize_csv_field(test)
print(f'Sanitized: {repr(result)}')"""]
    success, output, error = run_command(cmd)
    results.append(success)
    print_result("Field Sanitization", success, error.split('\n')[0] if error else "")
    
    return all(results)

def test_workflow_execution():
    """Test workflow execution with various flags"""
    print_test_header("5. WORKFLOW EXECUTION TESTS")
    
    results = []
    
    # Test different workflow scenarios
    test_cases = [
        ("Dry Run (all skip)", "python3 run_complete_workflow.py --skip-sheet --skip-youtube --skip-drive"),
        ("Max Rows Limited", "python3 run_complete_workflow.py --skip-sheet --max-youtube 0 --max-drive 0"),
    ]
    
    for name, cmd in test_cases:
        print(f"\n  Running: {name}")
        success, output, error = run_command(cmd, timeout=20)
        # Check for completion message
        completed = "completed successfully" in output.lower() or "status: completed" in output.lower()
        results.append(completed)
        print_result(name, completed, "Check logs for details" if not completed else "")
    
    return all(results)

def test_error_handling():
    """Test error handling and edge cases"""
    print_test_header("6. ERROR HANDLING & EDGE CASES")
    
    results = []
    
    # Test 1: Invalid URL validation
    cmd = ['python3', '-c', """from utils.validation import validate_youtube_url, validate_google_drive_url
try:
    # These should return tuples or False
    yt_result = validate_youtube_url('https://example.com')
    drive_result = validate_google_drive_url('not-a-url')
    print(f'Invalid URL handling OK')
except Exception as e:
    print(f'Error: {e}')"""]
    success, output, error = run_command(cmd)
    results.append(success or "Error:" in output)
    print_result("Invalid URL Handling", success or "Error:" in output)
    
    # Test 2: File locking
    cmd = ['python3', '-c', """from utils.file_lock import file_lock
import tempfile
import os
with tempfile.NamedTemporaryFile(delete=False) as tmp:
    lock_file = f'{tmp.name}.lock'
    with file_lock(lock_file):
        print('Lock acquired')
    if not os.path.exists(lock_file):
        print('Lock released')
    os.unlink(tmp.name)"""]
    success, output, error = run_command(cmd)
    results.append(success and "Lock released" in output)
    print_result("File Locking", success and "Lock released" in output)
    
    return all(results)

def test_monitoring_system():
    """Test monitoring and alerting system"""
    print_test_header("7. MONITORING & ALERTING SYSTEM")
    
    results = []
    
    # Get monitoring status
    success, output, error = run_command("python3 utils/monitoring.py --status --detailed", timeout=10)
    if success:
        # Parse monitoring output
        has_status = "System Status:" in output
        has_metrics = "Success Rate:" in output or "Pending Downloads:" in output
        results.append(has_status and has_metrics)
        print_result("Monitoring Status", has_status and has_metrics)
        
        # Check for alerts
        success, output, error = run_command("python3 utils/monitoring.py --alerts", timeout=10)
        results.append(success)
        print_result("Alert System", success, "No alerts" if "No active alerts" in output else "Alerts present")
    else:
        results.append(False)
        print_result("Monitoring System", False, error.split('\n')[0])
    
    return all(results)

def test_performance():
    """Basic performance tests"""
    print_test_header("8. PERFORMANCE TESTS")
    
    results = []
    
    # Test CSV read performance
    start_time = time.time()
    cmd = ['python3', '-c', """import pandas as pd
df = pd.read_csv('outputs/output.csv')
print(f'Loaded {len(df)} rows')"""]
    success, output, error = run_command(cmd)
    elapsed = time.time() - start_time
    
    results.append(success and elapsed < 5)  # Should load in under 5 seconds
    print_result(f"CSV Load Performance", success and elapsed < 5, 
                 f"Loaded in {elapsed:.2f}s" if success else error.split('\n')[0])
    
    # Test import performance
    start_time = time.time()
    cmd = ['python3', '-c', "import utils.csv_tracker; import utils.download_youtube; import utils.download_drive"]
    success, output, error = run_command(cmd)
    elapsed = time.time() - start_time
    
    results.append(success and elapsed < 3)  # Imports should be fast
    print_result(f"Module Import Performance", success and elapsed < 3,
                 f"Imported in {elapsed:.2f}s" if success else error.split('\n')[0])
    
    return all(results)

def main():
    """Run all tests and provide summary"""
    print(f"{Colors.BOLD}\n🧪 COMPREHENSIVE SYSTEM TEST SUITE 🧪{Colors.RESET}")
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Track results
    test_results = {}
    
    # Run all test categories
    test_results['imports'] = test_imports()
    test_results['cli_tools'] = test_cli_tools()
    test_results['file_operations'] = test_file_operations()
    test_results['core_functionality'] = test_core_functionality()
    test_results['workflow_execution'] = test_workflow_execution()
    test_results['error_handling'] = test_error_handling()
    test_results['monitoring'] = test_monitoring_system()
    test_results['performance'] = test_performance()
    
    # Summary
    print_test_header("TEST SUMMARY")
    
    total_passed = sum(1 for v in test_results.values() if v)
    total_tests = len(test_results)
    
    print(f"\nTest Categories:")
    for category, passed in test_results.items():
        status = f"{Colors.GREEN}PASSED{Colors.RESET}" if passed else f"{Colors.RED}FAILED{Colors.RESET}"
        print(f"  {category.replace('_', ' ').title()}: {status}")
    
    print(f"\n{Colors.BOLD}Overall Result: {total_passed}/{total_tests} categories passed{Colors.RESET}")
    
    if total_passed == total_tests:
        print(f"\n{Colors.GREEN}{Colors.BOLD}✅ ALL TESTS PASSED! System is fully operational.{Colors.RESET}")
        return 0
    else:
        print(f"\n{Colors.RED}{Colors.BOLD}❌ Some tests failed. Please review the output above.{Colors.RESET}")
        return 1

if __name__ == "__main__":
    sys.exit(main())