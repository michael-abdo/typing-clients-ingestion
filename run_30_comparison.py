#!/usr/bin/env python3
"""
Quick script to extract 30 most recent people for comparison with operator data
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

from simple_workflow import main

if __name__ == "__main__":
    # Simulate command line arguments for basic mode with 30 people
    sys.argv = ["simple_workflow.py", "--basic", "--test-limit", "30"]
    main()