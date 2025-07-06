#!/usr/bin/env python3
"""
Execute text extraction workflow
"""
import os
import sys
import subprocess

def main():
    # Change to minimal directory
    minimal_dir = "/home/Mike/projects/xenodex/typing-clients-ingestion/minimal"
    os.chdir(minimal_dir)
    
    # Execute the command
    cmd = [sys.executable, "simple_workflow.py", "--text", "--batch-size", "20"]
    
    print(f"Executing: {' '.join(cmd)}")
    print(f"Working directory: {os.getcwd()}")
    
    # Run the command
    result = subprocess.run(cmd, capture_output=False, text=True)
    
    return result.returncode

if __name__ == "__main__":
    sys.exit(main())