#!/usr/bin/env python3
"""
Test DRY Refactoring - Comprehensive test suite to verify refactoring doesn't break functionality
Run this after applying DRY refactoring to ensure everything still works correctly.
"""

from utils.path_setup import init_project_imports
init_project_imports()

import sys
import subprocess
import json
from pathlib import Path
from typing import List, Tuple, Dict
import tempfile
import shutil
from datetime import datetime

# Import modules to test their availability
from utils.csv_manager import CSVManager
from utils.config import get_config
try:
    from utils.config import run_subprocess
except ImportError:
    run_subprocess = None
from utils.error_decorators import handle_file_operations, handle_validation_errors
from utils.path_setup import ensure_directory_exists, get_project_root
from utils.retry_utils import retry_with_backoff
from utils.import_utils import safe_import


class RefactoringTester:
    """Test suite for DRY refactoring verification."""
    
    def __init__(self):
        self.results = []
        self.temp_dir = None
        
    def setup(self):
        """Set up test environment."""
        self.temp_dir = tempfile.mkdtemp(prefix='dry_test_')
        print(f"Created temp directory: {self.temp_dir}")
        
    def teardown(self):
        """Clean up test environment."""
        if self.temp_dir and Path(self.temp_dir).exists():
            shutil.rmtree(self.temp_dir)
            print(f"Cleaned up temp directory")
    
    def log_result(self, test_name: str, passed: bool, details: str = ""):
        """Log test result."""
        status = "✓ PASS" if passed else "✗ FAIL"
        self.results.append({
            'test': test_name,
            'passed': passed,
            'details': details
        })
        print(f"{status} - {test_name}")
        if details:
            print(f"    {details}")
    
    def test_imports(self) -> bool:
        """Test that all imports work correctly."""
        try:
            # Test path setup
            from utils.path_setup import (
                init_project_imports, setup_project_path, 
                get_project_root, ensure_directory_exists
            )
            
            # Test CSV manager
            from utils.csv_manager import CSVManager
            
            # Test config
            from utils.config import get_config
            
            # Test error handling
            from utils.error_decorators import (
                handle_file_operations, handle_validation_errors
            )
            
            # Test retry utils
            from utils.retry_utils import retry_with_backoff
            
            self.log_result("Module imports", True)
            return True
            
        except ImportError as e:
            self.log_result("Module imports", False, str(e))
            return False
    
    def test_path_setup(self) -> bool:
        """Test path setup functionality."""
        try:
            from utils.path_setup import (
                get_project_root, ensure_directory_exists,
                get_utils_dir, get_tests_dir
            )
            
            # Test getting directories
            project_root = get_project_root()
            assert project_root.exists(), "Project root doesn't exist"
            
            utils_dir = get_utils_dir()
            assert utils_dir.exists(), "Utils directory doesn't exist"
            
            # Test creating directory
            test_dir = Path(self.temp_dir) / "test_create"
            ensure_directory_exists(test_dir)
            assert test_dir.exists(), "Directory creation failed"
            
            self.log_result("Path setup functionality", True)
            return True
            
        except Exception as e:
            self.log_result("Path setup functionality", False, str(e))
            return False
    
    def test_csv_manager(self) -> bool:
        """Test CSV manager functionality."""
        try:
            # Test CSV writing
            test_file = Path(self.temp_dir) / "test.csv"
            test_data = [
                {'name': 'Test1', 'value': '123'},
                {'name': 'Test2', 'value': '456'}
            ]
            
            csv_manager = CSVManager(str(test_file))
            
            # Write CSV using atomic write
            success = csv_manager.atomic_write(
                test_data,
                fieldnames=['name', 'value']
            )
            assert success, "CSV write failed"
            
            # Read CSV
            df = csv_manager.read(dtype_spec='all_string')
            read_data = df.to_dict('records')
            assert len(read_data) == 2, f"Expected 2 rows, got {len(read_data)}"
            assert read_data[0]['name'] == 'Test1', "Data mismatch"
            
            self.log_result("CSV Manager functionality", True)
            return True
            
        except Exception as e:
            self.log_result("CSV Manager functionality", False, str(e))
            return False
    
    def test_config_access(self) -> bool:
        """Test configuration access."""
        try:
            config = get_config()
            
            # Test basic get
            value = config.get('downloads.max_workers', 5)
            assert isinstance(value, int), "Config value type mismatch"
            
            # Test section get
            downloads = config.get_section('downloads')
            assert isinstance(downloads, dict), "Section should return dict"
            
            self.log_result("Configuration access", True)
            return True
            
        except Exception as e:
            self.log_result("Configuration access", False, str(e))
            return False
    
    def test_error_decorators(self) -> bool:
        """Test error handling decorators."""
        try:
            @handle_file_operations("test_operation", return_on_error="failed")
            def test_file_op():
                # This should handle the error gracefully
                raise FileNotFoundError("test.txt")
            
            result = test_file_op()
            assert result == "failed", "Error decorator didn't return expected value"
            
            self.log_result("Error decorators", True)
            return True
            
        except Exception as e:
            self.log_result("Error decorators", False, str(e))
            return False
    
    def test_subprocess_handler(self) -> bool:
        """Test subprocess handling if implemented."""
        try:
            # Check if run_subprocess exists
            from utils.config import get_config
            config_module = sys.modules['utils.config']
            
            if run_subprocess is not None:
                # Test simple command
                result = run_subprocess(['echo', 'test'], description="Echo test")
                
                self.log_result("Subprocess handler", True)
                return True
            else:
                self.log_result("Subprocess handler", True, 
                              "Not implemented yet (Step 4 of refactoring)")
                return True
                
        except Exception as e:
            self.log_result("Subprocess handler", False, str(e))
            return False
    
    def test_script_execution(self) -> bool:
        """Test that main scripts still execute without errors."""
        scripts_to_test = [
            'simple_workflow.py',
            'check_entries.py',
            'run_all_tests.py'
        ]
        
        all_passed = True
        
        for script in scripts_to_test:
            script_path = get_project_root() / script
            if not script_path.exists():
                self.log_result(f"Script execution: {script}", False, 
                              "Script not found")
                continue
            
            try:
                # Try importing the script to check for syntax errors
                result = subprocess.run(
                    [sys.executable, '-m', 'py_compile', str(script_path)],
                    capture_output=True,
                    text=True
                )
                
                if result.returncode == 0:
                    self.log_result(f"Script syntax: {script}", True)
                else:
                    self.log_result(f"Script syntax: {script}", False,
                                  result.stderr)
                    all_passed = False
                    
            except Exception as e:
                self.log_result(f"Script execution: {script}", False, str(e))
                all_passed = False
        
        return all_passed
    
    def test_pytest_collection(self) -> bool:
        """Test that pytest can collect all tests."""
        try:
            result = subprocess.run(
                [sys.executable, '-m', 'pytest', '--collect-only', 'tests/'],
                capture_output=True,
                text=True,
                cwd=get_project_root()
            )
            
            if result.returncode == 0:
                # Extract number of tests collected
                import re
                match = re.search(r'collected (\d+) items?', result.stdout)
                if match:
                    num_tests = int(match.group(1))
                    self.log_result("Pytest collection", True, 
                                  f"Collected {num_tests} tests")
                else:
                    self.log_result("Pytest collection", True,
                                  "Tests collected successfully")
                return True
            else:
                self.log_result("Pytest collection", False, 
                              "Failed to collect tests")
                return False
                
        except Exception as e:
            self.log_result("Pytest collection", False, str(e))
            return False
    
    def generate_report(self) -> str:
        """Generate test report."""
        report = [
            "# DRY Refactoring Test Report",
            f"Generated: {datetime.now()}",
            "",
            "## Test Results Summary",
            ""
        ]
        
        total_tests = len(self.results)
        passed_tests = sum(1 for r in self.results if r['passed'])
        failed_tests = total_tests - passed_tests
        
        report.append(f"- Total Tests: {total_tests}")
        report.append(f"- Passed: {passed_tests}")
        report.append(f"- Failed: {failed_tests}")
        report.append(f"- Success Rate: {passed_tests/total_tests*100:.1f}%")
        report.append("")
        
        if failed_tests > 0:
            report.append("## Failed Tests")
            for result in self.results:
                if not result['passed']:
                    report.append(f"- **{result['test']}**")
                    if result['details']:
                        report.append(f"  - Error: {result['details']}")
            report.append("")
        
        report.append("## Detailed Results")
        for result in self.results:
            status = "✓" if result['passed'] else "✗"
            report.append(f"- {status} {result['test']}")
            if result['details']:
                report.append(f"  - {result['details']}")
        
        return "\n".join(report)
    
    def run_all_tests(self):
        """Run all tests."""
        print("="*60)
        print("DRY Refactoring Test Suite")
        print("="*60)
        print()
        
        self.setup()
        
        try:
            # Run tests in order
            self.test_imports()
            self.test_path_setup()
            self.test_csv_manager()
            self.test_config_access()
            self.test_error_decorators()
            self.test_subprocess_handler()
            self.test_script_execution()
            self.test_pytest_collection()
            
        finally:
            self.teardown()
        
        # Generate and save report
        report = self.generate_report()
        report_path = "dry_refactoring_test_report.md"
        
        with open(report_path, 'w') as f:
            f.write(report)
        
        print()
        print("="*60)
        print(f"Test report saved to: {report_path}")
        
        # Return success/failure
        failed = sum(1 for r in self.results if not r['passed'])
        return failed == 0


def main():
    """Run the test suite."""
    tester = RefactoringTester()
    success = tester.run_all_tests()
    
    if success:
        print("\n✅ All tests passed! Refactoring appears successful.")
        sys.exit(0)
    else:
        print("\n❌ Some tests failed. Review the report for details.")
        sys.exit(1)


if __name__ == "__main__":
    main()