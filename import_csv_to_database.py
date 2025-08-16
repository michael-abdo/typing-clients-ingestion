#!/usr/bin/env python3
"""
Import existing CSV data to database with comprehensive validation
"""

import os
import sys
from datetime import datetime

# Set up environment
os.environ['DB_PASSWORD'] = 'migration_pass_2025'
sys.path.append('.')

from utils.database_crud import import_csv_to_database
from utils.database_logging import db_logger

def main():
    """Import CSV data to database with validation."""
    
    print("🚀 Starting CSV to Database Import with Row-by-Row Validation")
    print("="*70)
    
    csv_file = "outputs/output.csv"
    start_time = datetime.now()
    
    try:
        # Import with comprehensive validation
        summary = import_csv_to_database(
            csv_file=csv_file,
            table_name='typing_clients_data',
            batch_size=50,  # Smaller batches for better error tracking
            validate_all=True  # Full validation after import
        )
        
        print("\n📊 IMPORT SUMMARY")
        print("="*50)
        print(f"CSV File: {summary['csv_file']}")
        print(f"Table: {summary['table_name']}")
        print(f"Total Records: {summary['total_records']}")
        print(f"Successful Inserts: {summary['successful_inserts']}")
        print(f"Failed Inserts: {summary['failed_inserts']}")
        print(f"Success Rate: {summary['success_rate']:.1f}%")
        print(f"Duration: {summary['duration_seconds']:.1f} seconds")
        print(f"Rate: {summary['records_per_second']:.1f} records/sec")
        
        if 'validation_passed' in summary:
            if summary['validation_passed']:
                print("✅ Full validation PASSED")
            else:
                print("❌ Full validation FAILED")
                return False
        
        if summary['failed_inserts'] == 0:
            print("\n🎉 CSV import completed successfully!")
            print("✅ All records imported and validated")
            return True
        else:
            print(f"\n⚠️ Import completed with {summary['failed_inserts']} failures")
            failure_rate = summary['failed_inserts'] / summary['total_records'] * 100
            if failure_rate > 5:  # More than 5% failure
                print(f"❌ High failure rate: {failure_rate:.1f}%")
                return False
            else:
                print(f"✅ Acceptable failure rate: {failure_rate:.1f}%")
                return True
                
    except Exception as e:
        print(f"\n💥 Import failed: {e}")
        db_logger.error(f"CSV import failed: {e}")
        return False
    
    finally:
        end_time = datetime.now()
        total_duration = (end_time - start_time).total_seconds()
        print(f"\nTotal execution time: {total_duration:.1f} seconds")

if __name__ == "__main__":
    success = main()
    if success:
        print("\n✅ Ready to proceed to next migration phase!")
        exit(0)
    else:
        print("\n❌ Import failed - check logs and fix issues before proceeding")
        exit(1)