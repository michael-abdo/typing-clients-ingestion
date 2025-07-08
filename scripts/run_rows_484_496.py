#!/usr/bin/env python3
"""
Run text extraction for rows 484-496
"""
import os
import sys

# Change to the minimal directory
minimal_dir = "/home/Mike/projects/xenodex/typing-clients-ingestion/minimal"
os.chdir(minimal_dir)
sys.path.insert(0, minimal_dir)

# Set up command line arguments
sys.argv = [
    "simple_workflow.py", 
    "--text", 
    "--batch-size", "5", 
    "--start-row", "484", 
    "--end-row", "496"
]

# Import and run
from simple_workflow import main

if __name__ == "__main__":
    try:
        main()
    except SystemExit:
        pass
    except Exception as e:
        print(f"Error: {e}")
        import traceback
        traceback.print_exc()