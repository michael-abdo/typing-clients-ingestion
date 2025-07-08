#!/usr/bin/env python3
"""
Direct test execution without subprocess
"""
import sys
import os

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    # Import and run the test
    from test_direct_links_fix import test_direct_links
    
    print("🚀 Running Direct Links Test")
    print("=" * 60)
    
    success = test_direct_links()
    
    if success:
        print("\n✅ TEST PASSED: Direct links extraction is working!")
    else:
        print("\n❌ TEST FAILED: Direct links extraction has issues")
        
except Exception as e:
    print(f"❌ Error running test: {e}")
    import traceback
    traceback.print_exc()