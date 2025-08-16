#!/usr/bin/env python3
'''Emergency rollback verification'''
import os
import sys

def verify_csv_mode():
    checks = []
    
    # Check environment variable
    checks.append(os.environ.get('EMERGENCY_CSV_ONLY') == 'true')
    
    # Check flag file
    checks.append(os.path.exists('EMERGENCY_CSV_ONLY.flag'))
    
    # Check CSV file
    checks.append(os.path.exists('outputs/output.csv'))
    
    if all(checks):
        print("✅ Emergency CSV-only mode verified")
        return True
    else:
        print("❌ Emergency rollback verification failed")
        return False

if __name__ == "__main__":
    success = verify_csv_mode()
    sys.exit(0 if success else 1)
