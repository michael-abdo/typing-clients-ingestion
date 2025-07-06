#!/usr/bin/env python3
"""
Text extraction runner - bypasses shell issues
"""
import os
import sys
import json
import time
from pathlib import Path

# Set up the environment
MINIMAL_DIR = "/home/Mike/projects/xenodex/typing-clients-ingestion/minimal"
os.chdir(MINIMAL_DIR)
sys.path.insert(0, MINIMAL_DIR)

# Import the workflow modules
from simple_workflow import main
from extraction_utils import download_google_sheet, extract_people_from_sheet_html, extract_text_with_retry

def run_text_extraction():
    """Run the text extraction workflow"""
    print("🚀 STARTING TEXT EXTRACTION WORKFLOW")
    print("=" * 80)
    
    # Configure for text extraction mode
    sys.argv = ["simple_workflow.py", "--text", "--batch-size", "20"]
    
    # Run the main workflow
    try:
        main()
        print("\n✅ TEXT EXTRACTION COMPLETE!")
    except Exception as e:
        print(f"\n❌ ERROR: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    run_text_extraction()