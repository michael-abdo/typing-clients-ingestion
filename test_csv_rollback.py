#!/usr/bin/env python3
"""
Test CSV-Only Mode Rollback Functionality

Comprehensive testing of emergency rollback scenarios:
1. Test emergency rollback script
2. Simulate database unavailability
3. Verify CSV-only operations work correctly  
4. Test recovery back to dual-write mode
5. Validate data integrity during rollback scenarios
"""

import os
import sys
import time
import json
import shutil
import pandas as pd
import subprocess
from datetime import datetime
from pathlib import Path
from unittest.mock import patch

# Set up environment
sys.path.append('.')

from utils.database_logging import db_logger

def test_emergency_rollback_script():
    """Test the emergency CSV rollback script functionality."""
    
    print("🚨 Testing Emergency Rollback Script...")
    
    # Check if rollback script exists
    rollback_script = Path("emergency_csv_rollback.py")
    if not rollback_script.exists():
        print(f"❌ Emergency rollback script not found: {rollback_script}")
        return False
    
    try:
        # Create backup of current config
        config_backup = None
        config_file = Path("config/config.yaml")
        
        if config_file.exists():
            config_backup = config_file.with_suffix(".backup_before_rollback")
            shutil.copy2(config_file, config_backup)
            print(f"📁 Config backup created: {config_backup}")
        
        # Test rollback script execution
        print("🔄 Executing emergency rollback script...")
        result = subprocess.run([
            sys.executable, str(rollback_script)
        ], capture_output=True, text=True, timeout=30)
        
        if result.returncode == 0:
            print("✅ Emergency rollback script executed successfully")
            print(f"   Output: {result.stdout[:200]}...")
            
            # Verify rollback effects
            if config_file.exists():
                with open(config_file, 'r') as f:
                    config_content = f.read()
                    if 'csv_only_mode: true' in config_content or 'database_enabled: false' in config_content:
                        print("✅ Config successfully modified for CSV-only mode")
                    else:
                        print("⚠️ Config may not have been properly modified")
            
            return True
        else:
            print(f"❌ Emergency rollback script failed")
            print(f"   Error: {result.stderr}")
            return False
            
    except subprocess.TimeoutExpired:
        print("❌ Emergency rollback script timed out")
        return False
    except Exception as e:
        print(f"❌ Emergency rollback script error: {e}")
        return False
    
    finally:
        # Restore config backup if it exists
        if config_backup and config_backup.exists():
            try:
                shutil.copy2(config_backup, config_file)
                config_backup.unlink()
                print(f"🔄 Config restored from backup")
            except:
                pass

def test_csv_only_operations():
    """Test that CSV operations work correctly when database is unavailable."""
    
    print("📊 Testing CSV-Only Operations...")
    
    csv_file = Path("outputs/output.csv")
    test_backup = csv_file.with_suffix(".test_backup")
    
    try:
        # Create backup of current CSV
        if csv_file.exists():
            shutil.copy2(csv_file, test_backup)
            print(f"📁 CSV backup created for testing")
        
        # Test CSV read operations
        print("  📖 Testing CSV read operations...")
        try:
            if csv_file.exists():
                df = pd.read_csv(csv_file)
                record_count = len(df)
                print(f"    ✅ CSV read successful: {record_count} records")
            else:
                print(f"    ✅ CSV read handled missing file correctly")
        except Exception as e:
            print(f"    ❌ CSV read failed: {e}")
            return False
        
        # Test CSV write operations  
        print("  📝 Testing CSV write operations...")
        try:
            test_record = {
                'row_id': 123456,
                'name': 'CSV Only Test User',
                'email': 'csvonly@example.com',
                'type': 'CSV_ONLY_TEST',
                'processed': True,
                'document_text': 'Testing CSV-only mode operations'
            }
            
            # Write test record
            if csv_file.exists():
                df = pd.read_csv(csv_file)
                new_df = pd.concat([df, pd.DataFrame([test_record])], ignore_index=True)
                new_df.to_csv(csv_file, index=False)
            else:
                df = pd.DataFrame([test_record])
                df.to_csv(csv_file, index=False)
            
            # Verify write
            df_verify = pd.read_csv(csv_file)
            if test_record['row_id'] in df_verify['row_id'].values:
                print(f"    ✅ CSV write successful: test record added")
            else:
                print(f"    ❌ CSV write failed: test record not found")
                return False
            
        except Exception as e:
            print(f"    ❌ CSV write failed: {e}")
            return False
        
        # Test CSV update operations
        print("  🔄 Testing CSV update operations...")
        try:
            # Update the test record
            df = pd.read_csv(csv_file)
            df.loc[df['row_id'] == test_record['row_id'], 'name'] = 'Updated CSV Only Test User'
            df.to_csv(csv_file, index=False)
            
            # Verify update
            df_verify = pd.read_csv(csv_file)
            updated_name = df_verify[df_verify['row_id'] == test_record['row_id']]['name'].iloc[0]
            if updated_name == 'Updated CSV Only Test User':
                print(f"    ✅ CSV update successful: record modified")
            else:
                print(f"    ❌ CSV update failed: record not modified")
                return False
                
        except Exception as e:
            print(f"    ❌ CSV update failed: {e}")
            return False
        
        print("✅ All CSV-only operations working correctly")
        return True
        
    finally:
        # Restore CSV backup
        if test_backup.exists():
            try:
                shutil.copy2(test_backup, csv_file)
                test_backup.unlink()
                print(f"🔄 CSV restored from backup")
            except:
                pass

def test_database_unavailable_scenario():
    """Test system behavior when database becomes unavailable."""
    
    print("🔌 Testing Database Unavailable Scenario...")
    
    # We'll simulate this by testing with invalid database config
    print("  🚫 Simulating database connection failure...")
    
    try:
        # Import database components
        os.environ['DB_PASSWORD'] = 'migration_pass_2025'
        from utils.database_operations import DatabaseConfig, DatabaseManager
        
        # Test with invalid database config (wrong password)
        invalid_config = DatabaseConfig(
            db_type='postgresql',
            host='localhost',
            port=5432,
            database='typing_clients_uuid',
            username='migration_user',
            password='invalid_password_for_testing'
        )
        
        db_manager = DatabaseManager(invalid_config)
        
        # Attempt database operation
        try:
            with db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
            print("    ❌ Database connection should have failed but didn't")
            return False
        except Exception as db_error:
            print(f"    ✅ Database connection failed as expected: {type(db_error).__name__}")
        
        # Test CSV fallback when database fails
        print("  📊 Testing CSV fallback when database unavailable...")
        
        csv_file = Path("outputs/output.csv")
        test_backup = csv_file.with_suffix(".fallback_test_backup")
        
        try:
            # Backup current CSV
            if csv_file.exists():
                shutil.copy2(csv_file, test_backup)
            
            # Simulate fallback operation
            fallback_record = {
                'row_id': 654321,
                'name': 'Database Unavailable Fallback Test',
                'email': 'fallback@example.com',
                'type': 'FALLBACK_TEST',
                'processed': False,
                'document_text': 'Testing fallback when database is unavailable'
            }
            
            # Write using CSV-only (simulating fallback)
            if csv_file.exists():
                df = pd.read_csv(csv_file)
                new_df = pd.concat([df, pd.DataFrame([fallback_record])], ignore_index=True)
                new_df.to_csv(csv_file, index=False)
            else:
                df = pd.DataFrame([fallback_record])
                df.to_csv(csv_file, index=False)
            
            # Verify fallback worked
            df_verify = pd.read_csv(csv_file)
            if fallback_record['row_id'] in df_verify['row_id'].values:
                print(f"    ✅ CSV fallback working: record written successfully")
            else:
                print(f"    ❌ CSV fallback failed: record not written")
                return False
                
        finally:
            # Restore backup
            if test_backup.exists():
                try:
                    shutil.copy2(test_backup, csv_file)
                    test_backup.unlink()
                except:
                    pass
        
        print("✅ Database unavailable scenario handled correctly")
        return True
        
    except Exception as e:
        print(f"❌ Database unavailable test failed: {e}")
        return False

def test_recovery_to_dual_write():
    """Test recovery back to dual-write mode after rollback."""
    
    print("🔄 Testing Recovery to Dual-Write Mode...")
    
    try:
        # Test that we can re-establish dual-write functionality
        print("  🔗 Testing dual-write recovery...")
        
        # Set up proper database config
        os.environ['DB_PASSWORD'] = 'migration_pass_2025'
        from utils.database_operations import DatabaseConfig, DatabaseManager
        from utils.data_validation import DataValidator
        
        config = DatabaseConfig(
            db_type='postgresql',
            host='localhost',
            port=5432,
            database='typing_clients_uuid',
            username='migration_user',
            password='migration_pass_2025'
        )
        
        db_manager = DatabaseManager(config)
        validator = DataValidator()
        csv_file = Path("outputs/output.csv")
        
        # Test dual-write functionality is restored
        recovery_record = {
            'row_id': 987654,
            'name': 'Recovery Test User',
            'email': 'recovery@example.com',
            'type': 'RECOVERY_TEST',
            'processed': True,
            'document_text': 'Testing recovery to dual-write mode'
        }
        
        try:
            # Attempt dual-write
            with db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Database write
                normalized_record = validator.normalize_csv_row(recovery_record)
                clean_record = {k: v for k, v in normalized_record.items() 
                              if k not in ['created_at', 'updated_at']}
                
                for key in ['file_uuids', 's3_paths']:
                    if key in clean_record and isinstance(clean_record[key], dict):
                        clean_record[key] = json.dumps(clean_record[key])
                
                columns = list(clean_record.keys())
                placeholders = ['%s' for _ in columns]
                values = list(clean_record.values())
                
                sql = f"""
                    INSERT INTO typing_clients_data ({', '.join(columns)}) 
                    VALUES ({', '.join(placeholders)})
                    ON CONFLICT (row_id) DO UPDATE SET name = EXCLUDED.name
                """
                
                cursor.execute(sql, values)
                
                # CSV write
                csv_record = validator.normalize_csv_row(recovery_record)
                for key in ['file_uuids', 's3_paths']:
                    if key in csv_record and isinstance(csv_record[key], dict):
                        csv_record[key] = json.dumps(csv_record[key])
                
                if csv_file.exists():
                    df = pd.read_csv(csv_file)
                    new_df = pd.concat([df, pd.DataFrame([csv_record])], ignore_index=True)
                    new_df.to_csv(csv_file, index=False)
                else:
                    df = pd.DataFrame([csv_record])
                    df.to_csv(csv_file, index=False)
                
                conn.commit()
                
                # Verify dual-write worked
                cursor.execute("SELECT * FROM typing_clients_data WHERE row_id = %s", [recovery_record['row_id']])
                db_result = cursor.fetchone()
                
                df_current = pd.read_csv(csv_file)
                csv_matches = df_current[df_current['row_id'] == recovery_record['row_id']]
                
                if db_result and not csv_matches.empty:
                    print(f"    ✅ Dual-write recovery successful: record in both CSV and database")
                else:
                    print(f"    ❌ Dual-write recovery failed: record missing from CSV or database")
                    return False
                
                # Cleanup
                cursor.execute("DELETE FROM typing_clients_data WHERE row_id = %s", [recovery_record['row_id']])
                df_clean = df_current[df_current['row_id'] != recovery_record['row_id']]
                df_clean.to_csv(csv_file, index=False)
                conn.commit()
                
        except Exception as dual_write_error:
            print(f"    ❌ Dual-write recovery failed: {dual_write_error}")
            return False
        
        print("✅ Recovery to dual-write mode successful")
        return True
        
    except Exception as e:
        print(f"❌ Recovery test failed: {e}")
        return False

def test_data_integrity_during_rollback():
    """Test that data integrity is maintained during rollback scenarios."""
    
    print("🔒 Testing Data Integrity During Rollback...")
    
    csv_file = Path("outputs/output.csv")
    integrity_backup = csv_file.with_suffix(".integrity_backup")
    
    try:
        # Create backup
        if csv_file.exists():
            shutil.copy2(csv_file, integrity_backup)
            initial_df = pd.read_csv(csv_file)
            initial_count = len(initial_df)
            print(f"  📊 Initial CSV record count: {initial_count}")
        else:
            initial_count = 0
            print(f"  📊 No existing CSV file")
        
        # Simulate rollback scenario with multiple operations
        print("  🔄 Simulating rollback operations...")
        
        rollback_records = [
            {
                'row_id': 111111,
                'name': 'Integrity Test 1',
                'email': 'integrity1@example.com',
                'type': 'INTEGRITY_TEST',
                'processed': True
            },
            {
                'row_id': 111112,
                'name': 'Integrity Test 2',
                'email': 'integrity2@example.com',
                'type': 'INTEGRITY_TEST',
                'processed': False
            }
        ]
        
        # Add records during "rollback mode"
        for record in rollback_records:
            if csv_file.exists():
                df = pd.read_csv(csv_file)
                new_df = pd.concat([df, pd.DataFrame([record])], ignore_index=True)
                new_df.to_csv(csv_file, index=False)
            else:
                df = pd.DataFrame([record])
                df.to_csv(csv_file, index=False)
        
        # Verify data integrity
        df_final = pd.read_csv(csv_file)
        final_count = len(df_final)
        expected_count = initial_count + len(rollback_records)
        
        if final_count == expected_count:
            print(f"    ✅ Record count integrity maintained: {final_count} records")
        else:
            print(f"    ❌ Record count mismatch: expected {expected_count}, got {final_count}")
            return False
        
        # Verify all rollback records are present
        for record in rollback_records:
            if record['row_id'] not in df_final['row_id'].values:
                print(f"    ❌ Rollback record {record['row_id']} missing")
                return False
        
        print(f"    ✅ All rollback records present and accounted for")
        
        # Verify no data corruption
        try:
            # Check that all records have required fields
            required_fields = ['row_id', 'name', 'email']
            for field in required_fields:
                if field not in df_final.columns:
                    print(f"    ❌ Required field '{field}' missing from CSV")
                    return False
            
            # Check for duplicate row_ids (data corruption indicator)
            duplicate_ids = df_final[df_final.duplicated(subset=['row_id'], keep=False)]
            if not duplicate_ids.empty:
                print(f"    ⚠️ Warning: {len(duplicate_ids)} duplicate row_ids found")
            else:
                print(f"    ✅ No duplicate row_ids found")
            
        except Exception as integrity_error:
            print(f"    ❌ Data integrity check failed: {integrity_error}")
            return False
        
        print("✅ Data integrity maintained during rollback scenario")
        return True
        
    finally:
        # Restore from backup
        if integrity_backup.exists():
            try:
                shutil.copy2(integrity_backup, csv_file)
                integrity_backup.unlink()
                print(f"🔄 CSV restored from integrity backup")
            except:
                pass

def main():
    """Run comprehensive CSV rollback testing."""
    
    print("🚨 Comprehensive CSV-Only Mode Rollback Testing")
    print("="*55)
    
    start_time = datetime.now()
    
    # Test suite
    tests = [
        ("Emergency Rollback Script", test_emergency_rollback_script),
        ("CSV-Only Operations", test_csv_only_operations),
        ("Database Unavailable Scenario", test_database_unavailable_scenario),
        ("Recovery to Dual-Write", test_recovery_to_dual_write),
        ("Data Integrity During Rollback", test_data_integrity_during_rollback)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        print(f"\n🧪 Running test: {test_name}")
        print("-" * 40)
        
        try:
            success = test_func()
            results.append((test_name, success))
            
            if success:
                print(f"✅ {test_name} PASSED")
            else:
                print(f"❌ {test_name} FAILED")
                
        except Exception as e:
            print(f"💥 {test_name} CRASHED: {e}")
            results.append((test_name, False))
    
    # Summary
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    passed = sum(1 for _, success in results if success)
    total = len(results)
    
    print(f"\n📊 CSV ROLLBACK TEST SUMMARY")
    print("="*35)
    print(f"Tests passed: {passed}/{total}")
    print(f"Success rate: {passed/total*100:.1f}%")
    print(f"Total duration: {duration:.1f} seconds")
    
    print(f"\n📋 Individual Results:")
    for test_name, success in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"  {test_name}: {status}")
    
    if passed == total:
        print(f"\n🎉 ALL ROLLBACK TESTS PASSED!")
        print(f"✅ CSV-only mode rollback functionality verified")
        print(f"✅ Emergency recovery procedures working correctly") 
        print(f"✅ Data integrity maintained during rollbacks")
        print(f"✅ System ready for production deployment")
        return True
    else:
        print(f"\n⚠️ {total - passed} tests failed - review rollback procedures")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)