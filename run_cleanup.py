#!/usr/bin/env python3
"""
Execute the cleanup script using subprocess
"""

import subprocess
import sys

def main():
    try:
        result = subprocess.run([sys.executable, "cleanup_temp_files.py"], 
                              capture_output=True, text=True, check=False)
        
        print("STDOUT:")
        print(result.stdout)
        
        if result.stderr:
            print("STDERR:")
            print(result.stderr)
            
        print(f"Exit code: {result.returncode}")
        
    except Exception as e:
        print(f"Error running cleanup: {e}")

if __name__ == "__main__":
    main()