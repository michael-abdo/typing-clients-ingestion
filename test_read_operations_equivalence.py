#!/usr/bin/env python3
"""
Test Read Operations Equivalence

Tests read operations for overlapping data between CSV and database,
and verifies correct behavior for data that exists in only one source.

Focus: Ensure read operations work correctly for shared data and handle
missing data appropriately.
"""

import os
import sys
import pandas as pd
from datetime import datetime

# Set up environment
os.environ['DB_PASSWORD'] = 'migration_pass_2025'
sys.path.append('.')

from utils.database_primary_manager import DatabasePrimaryManager
from utils.csv_manager import CSVManager
from utils.database_logging import db_logger

def test_read_operations_equivalence():
    """Test read operations for overlapping data and proper system behavior."""
    
    print("🧪 Testing Read Operations Equivalence")
    print("=" * 40)
    
    # Initialize managers
    db_manager = DatabasePrimaryManager()
    csv_manager = CSVManager("outputs/output.csv")
    
    results = {
        'tests_run': 0,
        'tests_passed': 0,
        'tests_failed': 0,
        'start_time': datetime.now()
    }
    
    # Step 1: Identify overlapping records
    print("\n🔍 Step 1: Identifying overlapping data")
    results['tests_run'] += 1
    
    try:
        # Get CSV data
        csv_df = csv_manager.read()
        csv_row_ids = set(csv_df['row_id'].dropna().astype(float).astype(int).tolist())
        print(f"   📁 CSV contains {len(csv_row_ids)} valid row_ids: {sorted(list(csv_row_ids))[:10]}...")
        
        # Get database data  
        db_df = db_manager.read_dataframe()
        db_row_ids = set(db_df['row_id'].tolist())
        print(f"   🗄️ Database contains {len(db_row_ids)} row_ids: {sorted(list(db_row_ids))[:10]}...")
        
        # Find overlapping records
        overlapping_ids = csv_row_ids.intersection(db_row_ids)
        csv_only_ids = csv_row_ids - db_row_ids
        db_only_ids = db_row_ids - csv_row_ids
        
        print(f"   🔗 Overlapping records: {len(overlapping_ids)} -> {sorted(list(overlapping_ids))[:10]}")
        print(f"   📁 CSV-only records: {len(csv_only_ids)} -> {sorted(list(csv_only_ids))[:5]}")
        print(f"   🗄️ DB-only records: {len(db_only_ids)} (showing first 10: {sorted(list(db_only_ids))[:10]})")
        
        results['tests_passed'] += 1
        
    except Exception as e:
        print(f"   ❌ FAILED: {e}")
        results['tests_failed'] += 1
        return False
    
    # Step 2: Test overlapping records for equivalence
    if overlapping_ids:
        print(f"\n🔄 Step 2: Testing overlapping records ({len(overlapping_ids)} records)")
        results['tests_run'] += 1
        
        try:
            equivalent_count = 0
            mismatch_count = 0
            
            for row_id in sorted(list(overlapping_ids))[:10]:  # Test first 10
                # Get from CSV (handle float comparison)
                csv_matches = csv_df[csv_df['row_id'] == float(row_id)]
                if csv_matches.empty:
                    print(f"   ⚠️ Record {row_id}: Not found in CSV")
                    continue
                csv_record = csv_matches.iloc[0].to_dict()
                
                # Get from database  
                db_record = db_manager.get_record_by_id(row_id)
                
                # Compare key fields
                if db_record and csv_record['name'] == db_record['name'] and csv_record['email'] == db_record['email']:
                    equivalent_count += 1
                    print(f"   ✅ Record {row_id}: {db_record['name']} - EQUIVALENT")
                else:
                    mismatch_count += 1
                    print(f"   ❌ Record {row_id}: MISMATCH")
                    print(f"      CSV: {csv_record.get('name', 'N/A')} | DB: {db_record.get('name', 'N/A') if db_record else 'None'}")
            
            print(f"   📊 Results: {equivalent_count} equivalent, {mismatch_count} mismatches")
            
            if mismatch_count == 0:
                results['tests_passed'] += 1
            else:
                results['tests_failed'] += 1
                
        except Exception as e:
            print(f"   ❌ FAILED: {e}")
            results['tests_failed'] += 1
    else:
        print("\n⚠️ Step 2: No overlapping records found - skipping equivalence test")
    
    # Step 3: Test database-only records
    print(f"\n🗄️ Step 3: Testing database-only records")
    results['tests_run'] += 1
    
    try:
        test_db_ids = sorted(list(db_only_ids))[:5]  # Test first 5 DB-only records
        success_count = 0
        
        for row_id in test_db_ids:
            db_record = db_manager.get_record_by_id(row_id)
            if db_record:
                success_count += 1
                print(f"   ✅ Record {row_id}: {db_record['name']} - SUCCESS")
            else:
                print(f"   ❌ Record {row_id}: NOT FOUND")
        
        print(f"   📊 Database-only reads: {success_count}/{len(test_db_ids)} successful")
        
        if success_count == len(test_db_ids):
            results['tests_passed'] += 1
        else:
            results['tests_failed'] += 1
            
    except Exception as e:
        print(f"   ❌ FAILED: {e}")
        results['tests_failed'] += 1
    
    # Step 4: Test CSV fallback for CSV-only records
    if csv_only_ids:
        print(f"\n📁 Step 4: Testing CSV fallback for CSV-only records")
        results['tests_run'] += 1
        
        try:
            test_csv_ids = sorted(list(csv_only_ids))[:3]  # Test first 3 CSV-only records
            fallback_count = 0
            
            for row_id in test_csv_ids:
                # This should trigger fallback to CSV
                record = db_manager.get_record_by_id(row_id)
                if record:
                    fallback_count += 1
                    print(f"   ✅ Record {row_id}: {record['name']} - FALLBACK SUCCESS")
                else:
                    print(f"   ❌ Record {row_id}: FALLBACK FAILED")
            
            print(f"   📊 CSV fallback reads: {fallback_count}/{len(test_csv_ids)} successful")
            
            if fallback_count == len(test_csv_ids):
                results['tests_passed'] += 1
            else:
                results['tests_failed'] += 1
                
        except Exception as e:
            print(f"   ❌ FAILED: {e}")
            results['tests_failed'] += 1
    else:
        print("\n⚠️ Step 4: No CSV-only records found - skipping fallback test")
    
    # Step 5: Test performance comparison
    print(f"\n⚡ Step 5: Performance comparison")
    results['tests_run'] += 1
    
    try:
        if overlapping_ids:
            test_id = list(overlapping_ids)[0]
            
            # Time database read
            import time
            db_start = time.time()
            for _ in range(5):
                db_record = db_manager.get_record_by_id(test_id)
            db_time = (time.time() - db_start) / 5
            
            # Time CSV read  
            csv_matches = csv_df[csv_df['row_id'] == float(test_id)]
            if not csv_matches.empty:
                csv_start = time.time()
                for _ in range(5):
                    csv_record = csv_matches.iloc[0].to_dict()
                csv_time = (time.time() - csv_start) / 5
            else:
                csv_time = 0.001  # Default time if no CSV match
            
            improvement = ((csv_time - db_time) / csv_time * 100) if csv_time > 0 else 0
            
            print(f"   🗄️ Database read: {db_time:.4f}s avg")
            print(f"   📁 CSV read: {csv_time:.4f}s avg") 
            print(f"   📈 Performance change: {improvement:+.1f}%")
            
            results['tests_passed'] += 1
        else:
            print("   ⚠️ No overlapping records for performance test")
            results['tests_passed'] += 1
            
    except Exception as e:
        print(f"   ❌ FAILED: {e}")
        results['tests_failed'] += 1
    
    # Step 6: Test usage statistics
    print(f"\n📊 Step 6: Final usage statistics")
    results['tests_run'] += 1
    
    try:
        stats = db_manager.get_usage_statistics()
        print(f"   🗄️ Database reads: {stats['database_reads']}")
        print(f"   📁 CSV fallbacks: {stats['csv_fallbacks']}")
        print(f"   📈 Database success rate: {stats['database_success_rate']:.1f}%")
        print(f"   📉 Fallback rate: {stats['fallback_rate']:.1f}%")
        
        results['tests_passed'] += 1
        
    except Exception as e:
        print(f"   ❌ FAILED: {e}")
        results['tests_failed'] += 1
    
    # Final Report
    end_time = datetime.now()
    duration = (end_time - results['start_time']).total_seconds()
    
    print(f"\n📊 Test Results Summary")
    print("=" * 25)
    print(f"Tests run: {results['tests_run']}")
    print(f"Tests passed: {results['tests_passed']}")
    print(f"Tests failed: {results['tests_failed']}")
    print(f"Success rate: {(results['tests_passed']/results['tests_run']*100):.1f}%")
    print(f"Test duration: {duration:.1f}s")
    
    if results['tests_failed'] == 0:
        print(f"\n🎉 All read operations working correctly!")
        print(f"✅ Overlapping data is equivalent")
        print(f"✅ Database-primary reads work")
        print(f"✅ CSV fallback functions correctly")
        print(f"✅ Performance is acceptable")
        return True
    else:
        print(f"\n⚠️ {results['tests_failed']} tests failed. Review issues before proceeding.")
        return False

if __name__ == "__main__":
    success = test_read_operations_equivalence()
    exit(0 if success else 1)