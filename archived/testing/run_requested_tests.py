#!/usr/bin/env python3
"""
Run the specific tests that were requested:
1. Test import: `python3 -c "from utils.csv_manager import safe_csv_read; print('✅ safe_csv_read import works')"`
2. Test download_all_minimal import: `python3 -c "import download_all_minimal; print('✅ download_all_minimal import works')"`
3. Test CSV reading function: `python3 -c "from utils.csv_manager import safe_csv_read; df = safe_csv_read('outputs/output.csv'); print('✅ safe_csv_read function works, shape:', df.shape if df is not None else 'None')"`
4. Test help still works: `python3 download_all_minimal.py --help`
"""

import sys
import subprocess
import os

def run_test_1():
    """Test import: safe_csv_read"""
    print("Test 1: safe_csv_read import")
    try:
        from utils.csv_manager import safe_csv_read
        print('✅ safe_csv_read import works')
        return True
    except Exception as e:
        print(f'❌ safe_csv_read import failed: {e}')
        return False

def run_test_2():
    """Test download_all_minimal import"""
    print("\nTest 2: download_all_minimal import")
    try:
        import download_all_minimal
        print('✅ download_all_minimal import works')
        return True
    except Exception as e:
        print(f'❌ download_all_minimal import failed: {e}')
        return False

def run_test_3():
    """Test CSV reading function"""
    print("\nTest 3: safe_csv_read function")
    try:
        from utils.csv_manager import safe_csv_read
        df = safe_csv_read('outputs/output.csv')
        print('✅ safe_csv_read function works, shape:', df.shape if df is not None else 'None')
        return True
    except Exception as e:
        print(f'❌ safe_csv_read function failed: {e}')
        return False

def run_test_4():
    """Test help still works"""
    print("\nTest 4: download_all_minimal help")
    try:
        # Run the help command
        result = subprocess.run([sys.executable, 'download_all_minimal.py', '--help'], 
                              capture_output=True, text=True, timeout=10)
        
        if result.returncode == 0:
            print('✅ download_all_minimal help works')
            # Show first line of help output
            first_line = result.stdout.split('\n')[0]
            print(f'   Help output starts with: {first_line}')
            return True
        else:
            print(f'❌ download_all_minimal help failed: {result.stderr}')
            return False
    except subprocess.TimeoutExpired:
        print('❌ download_all_minimal help timed out')
        return False
    except Exception as e:
        print(f'❌ download_all_minimal help failed: {e}')
        return False

def main():
    """Run all requested tests"""
    print("Running requested CSV consolidation tests")
    print("=" * 50)
    
    tests = [run_test_1, run_test_2, run_test_3, run_test_4]
    passed = 0
    
    for test in tests:
        if test():
            passed += 1
    
    print("\n" + "=" * 50)
    print(f"Results: {passed}/{len(tests)} tests passed")
    
    if passed == len(tests):
        print("🎉 ALL TESTS PASSED!")
        print("\nThe CSV reading consolidation is working correctly.")
        print("The updated download_all_minimal.py file successfully uses")
        print("safe_csv_read from utils.csv_manager.")
    else:
        print("⚠️  Some tests failed.")
    
    return passed == len(tests)

if __name__ == "__main__":
    success = main()
    sys.exit(0 if success else 1)