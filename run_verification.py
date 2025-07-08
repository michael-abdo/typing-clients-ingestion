#!/usr/bin/env python3
"""
Execute verification against operator data
"""

try:
    print("🚀 EXECUTING VERIFICATION AGAINST OPERATOR DATA")
    print("=" * 60)
    
    # Import and run verification
    import verify_operator_data
    
    print("\n✅ Verification completed successfully")
    
except Exception as e:
    print(f"❌ Verification failed: {e}")
    import traceback
    traceback.print_exc()