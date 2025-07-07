#!/usr/bin/env python3
"""Simple test runner"""
import subprocess
import sys

try:
    result = subprocess.run([sys.executable, "minimal/test_enhanced_workflow.py"], capture_output=True, text=True)
    print(result.stdout)
    if result.stderr:
        print("STDERR:", result.stderr)
except Exception as e:
    print(f"Error running test: {e}")