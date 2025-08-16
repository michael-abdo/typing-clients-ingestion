#!/usr/bin/env python3
"""
Test Database-Primary Operations

Tests that database-primary read operations work correctly without
requiring CSV/DB data equivalence (since they contain different datasets).

Focus: Operational capability, performance, and fallback functionality.
"""

import os
import sys
import time
from datetime import datetime

# Set up environment
os.environ['DB_PASSWORD'] = 'migration_pass_2025'
sys.path.append('.')

from utils.database_primary_manager import DatabasePrimaryManager
from utils.database_logging import db_logger

def test_database_primary_operations():
    """Test core database-primary operations for functionality."""
    
    print("🧪 Testing Database-Primary Operations")
    print("=" * 40)
    
    manager = DatabasePrimaryManager()
    
    results = {
        'tests_run': 0,
        'tests_passed': 0,
        'tests_failed': 0,
        'start_time': datetime.now()
    }
    
    # Test 1: Database DataFrame Read
    print("\n📊 Test 1: Database DataFrame Read")
    results['tests_run'] += 1
    try:
        df = manager.read_dataframe()
        print(f"   ✅ SUCCESS: Read {len(df)} records from database")
        print(f"   📋 Columns: {list(df.columns)}")
        print(f"   🔢 Row ID range: {df['row_id'].min()} to {df['row_id'].max()}")
        results['tests_passed'] += 1
    except Exception as e:
        print(f"   ❌ FAILED: {e}")
        results['tests_failed'] += 1
    
    # Test 2: Single Record Read
    print("\n📖 Test 2: Single Record Read")
    results['tests_run'] += 1
    try:
        record = manager.get_record_by_id(1)  # Try first record
        if record:
            print(f"   ✅ SUCCESS: Found record 1: {record['name']}")
            print(f"   📧 Email: {record['email']}")
            results['tests_passed'] += 1
        else:
            print(f"   ❌ FAILED: Record 1 not found")
            results['tests_failed'] += 1
    except Exception as e:
        print(f"   ❌ FAILED: {e}")
        results['tests_failed'] += 1
    
    # Test 3: Record Count
    print("\n🔢 Test 3: Record Count")
    results['tests_run'] += 1
    try:
        count = manager.count_records()
        print(f"   ✅ SUCCESS: Database contains {count} records")
        results['tests_passed'] += 1
    except Exception as e:
        print(f"   ❌ FAILED: {e}")
        results['tests_failed'] += 1
    
    # Test 4: Filtered Records
    print("\n🔍 Test 4: Filtered Records (processed=True)")
    results['tests_run'] += 1
    try:
        processed_records = manager.get_records_by_criteria(processed=True)
        print(f"   ✅ SUCCESS: Found {len(processed_records)} processed records")
        results['tests_passed'] += 1
    except Exception as e:
        print(f"   ❌ FAILED: {e}")
        results['tests_failed'] += 1
    
    # Test 5: Performance Test
    print("\n⚡ Test 5: Performance Test (10 record reads)")
    results['tests_run'] += 1
    try:
        start_time = time.time()
        for i in range(1, 11):
            record = manager.get_record_by_id(i)
        end_time = time.time()
        
        duration = end_time - start_time
        avg_per_read = duration / 10
        print(f"   ✅ SUCCESS: 10 reads in {duration:.3f}s (avg: {avg_per_read:.3f}s per read)")
        results['tests_passed'] += 1
    except Exception as e:
        print(f"   ❌ FAILED: {e}")
        results['tests_failed'] += 1
    
    # Test 6: Fallback Capability (force CSV read)
    print("\n🔄 Test 6: CSV Fallback Capability")
    results['tests_run'] += 1
    try:
        csv_df = manager.read_dataframe(force_csv=True)
        print(f"   ✅ SUCCESS: CSV fallback read {len(csv_df)} records")
        print(f"   📋 CSV row ID range: {csv_df['row_id'].min()} to {csv_df['row_id'].max()}")
        results['tests_passed'] += 1
    except Exception as e:
        print(f"   ❌ FAILED: {e}")
        results['tests_failed'] += 1
    
    # Test 7: Usage Statistics
    print("\n📊 Test 7: Usage Statistics")
    results['tests_run'] += 1
    try:
        stats = manager.get_usage_statistics()
        print(f"   ✅ SUCCESS: Statistics collected")
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
        print(f"\n🎉 All tests passed! Database-primary operations working correctly.")
        return True
    else:
        print(f"\n⚠️ {results['tests_failed']} tests failed. Review issues before proceeding.")
        return False

if __name__ == "__main__":
    success = test_database_primary_operations()
    exit(0 if success else 1)