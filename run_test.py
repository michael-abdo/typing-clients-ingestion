#!/usr/bin/env python3
"""
Simple test runner
"""
import subprocess
import sys

try:
    result = subprocess.run([sys.executable, "test_sheet_download.py"], 
                          capture_output=True, text=True, timeout=30)
    
    print("STDOUT:")
    print(result.stdout)
    
    if result.stderr:
        print("STDERR:")
        print(result.stderr)
    
    print(f"Return code: {result.returncode}")
    
except Exception as e:
    print(f"Error running test: {e}")