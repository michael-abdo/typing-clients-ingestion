#!/usr/bin/env python3
"""
Direct execution of the text extraction workflow without bash dependencies
"""
import os
import sys

# Add the minimal directory to Python path
minimal_dir = "/home/Mike/projects/xenodex/typing-clients-ingestion/minimal"
sys.path.insert(0, minimal_dir)

# Change working directory
os.chdir(minimal_dir)

# Import and configure the workflow
sys.argv = ["simple_workflow.py", "--text", "--batch-size", "20"]

# Import and run the main function
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