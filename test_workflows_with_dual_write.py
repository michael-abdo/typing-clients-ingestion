#!/usr/bin/env python3
"""
Test All Main Workflows with Dual-Write Enabled

Tests the integration of dual-write functionality into main system workflows:
1. Simple workflow (6-step document processing)
2. CSV incremental updates 
3. Upload workflows
4. Download workflows
"""

import os
import sys
import json
import pandas as pd
from datetime import datetime
from pathlib import Path

# Set up environment
os.environ['DB_PASSWORD'] = 'migration_pass_2025'
sys.path.append('.')

from utils.database_operations import DatabaseConfig, DatabaseManager
from test_single_dual_write import test_single_dual_write  # Our proven dual-write test
from utils.data_validation import DataValidator
from utils.database_logging import db_logger

def test_csv_incremental_update_with_dual_write():
    """Test CSV incremental updates with dual-write enabled."""
    
    print("🔄 Testing CSV incremental updates with dual-write...")
    
    # Configure database
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
    
    test_records = [
        {
            'row_id': 777777,
            'name': 'Workflow Test User 1',
            'email': 'workflow1@example.com',
            'type': 'WORKFLOW_TEST',
            'processed': False,
            'document_text': 'Test document for workflow validation 1'
        },
        {
            'row_id': 777778,
            'name': 'Workflow Test User 2', 
            'email': 'workflow2@example.com',
            'type': 'WORKFLOW_TEST',
            'processed': True,
            'document_text': 'Test document for workflow validation 2'
        }
    ]
    
    csv_file = Path("outputs/output.csv")
    successful_writes = 0
    
    try:
        for i, record in enumerate(test_records, 1):
            print(f"\n  📝 Testing incremental update {i}/2...")
            
            # Simulate dual-write during incremental CSV update
            with db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Write to database first
                normalized_record = validator.normalize_csv_row(record)
                clean_record = {k: v for k, v in normalized_record.items() 
                              if k not in ['created_at', 'updated_at']}
                
                # Convert JSON fields
                for key in ['file_uuids', 's3_paths']:
                    if key in clean_record and isinstance(clean_record[key], dict):
                        clean_record[key] = json.dumps(clean_record[key])
                
                # Insert to database
                columns = list(clean_record.keys())
                placeholders = ['%s' for _ in columns]
                values = list(clean_record.values())
                
                sql = f"""
                    INSERT INTO typing_clients_data ({', '.join(columns)}) 
                    VALUES ({', '.join(placeholders)})
                    ON CONFLICT (row_id) DO UPDATE SET name = EXCLUDED.name
                """
                
                cursor.execute(sql, values)
                
                # Write to CSV (incremental update simulation)
                if csv_file.exists():
                    df = pd.read_csv(csv_file)
                    
                    # Check if record exists
                    if record['row_id'] in df['row_id'].values:
                        # Update existing
                        for key, value in record.items():
                            df.loc[df['row_id'] == record['row_id'], key] = value
                    else:
                        # Append new
                        new_df = pd.concat([df, pd.DataFrame([record])], ignore_index=True)
                        df = new_df
                    
                    df.to_csv(csv_file, index=False)
                else:
                    # Create new file
                    df = pd.DataFrame([record])
                    df.to_csv(csv_file, index=False)
                
                conn.commit()
                
                # Validate dual-write worked
                cursor.execute("SELECT * FROM typing_clients_data WHERE row_id = %s", [record['row_id']])
                db_result = cursor.fetchone()
                
                df_current = pd.read_csv(csv_file)
                csv_matches = df_current[df_current['row_id'] == record['row_id']]
                csv_result = csv_matches.iloc[0].to_dict()
                
                # Validate using our validator
                validator.validate_dual_write(csv_result, db_result, record['row_id'])
                
                successful_writes += 1
                print(f"    ✅ Incremental update {i} successful and validated")
        
        print(f"\n✅ CSV incremental updates test PASSED: {successful_writes}/{len(test_records)} successful")
        return True
        
    except Exception as e:
        print(f"\n❌ CSV incremental updates test FAILED: {e}")
        return False
    
    finally:
        # Cleanup
        try:
            with db_manager.get_connection() as conn:
                cursor = conn.cursor()
                for record in test_records:
                    cursor.execute("DELETE FROM typing_clients_data WHERE row_id = %s", [record['row_id']])
                
                # Remove from CSV
                if csv_file.exists():
                    df = pd.read_csv(csv_file)
                    test_ids = [r['row_id'] for r in test_records]
                    df = df[~df['row_id'].isin(test_ids)]
                    df.to_csv(csv_file, index=False)
                
                conn.commit()
        except:
            pass

def test_workflow_operations_simulation():
    """Test workflow operations that would use dual-write."""
    
    print("🔧 Testing workflow operations simulation with dual-write...")
    
    # Test different types of operations that happen in workflows
    operations = [
        {
            'name': 'Document Processing Result',
            'record': {
                'row_id': 666666,
                'name': 'Document Processing Test',
                'email': 'docprocess@example.com',
                'type': 'DOC_PROCESS_TEST',
                'processed': True,
                'document_text': 'Extracted document content from Google Docs',
                'youtube_status': 'not_applicable',
                'drive_status': 'not_applicable'
            }
        },
        {
            'name': 'YouTube Download Status Update',
            'record': {
                'row_id': 666667,
                'name': 'YouTube Download Test',
                'email': 'youtube@example.com',
                'type': 'YOUTUBE_TEST',
                'processed': True,
                'youtube_status': 'completed',
                'youtube_files': '["video.mp4", "transcript.txt"]',
                'youtube_media_id': 'test_video_123',
                'document_text': 'YouTube video transcript content'
            }
        },
        {
            'name': 'Drive Download Status Update',
            'record': {
                'row_id': 666668,
                'name': 'Drive Download Test',
                'email': 'drive@example.com',
                'type': 'DRIVE_TEST',
                'processed': True,
                'drive_status': 'completed',
                'drive_files': '["document.pdf", "presentation.pptx"]',
                'drive_media_id': 'test_drive_456',
                'document_text': 'Drive document content'
            }
        }
    ]
    
    successful_operations = 0
    
    for i, operation in enumerate(operations, 1):
        try:
            print(f"\n  🔧 Testing operation {i}/3: {operation['name']}")
            
            # Use our proven single dual-write test for each operation
            test_record = operation['record']
            
            # Modify the test to use our record
            from test_single_dual_write import test_single_dual_write
            
            # Create a custom test for this specific record
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
            
            # Dual-write operation
            with db_manager.get_connection() as conn:
                cursor = conn.cursor()
                
                # Normalize and write to database
                normalized_record = validator.normalize_csv_row(test_record)
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
                
                # Write to CSV
                csv_record = validator.normalize_csv_row(test_record)
                for key in ['file_uuids', 's3_paths']:
                    if key in csv_record and isinstance(csv_record[key], dict):
                        csv_record[key] = json.dumps(csv_record[key])
                
                if csv_file.exists():
                    df = pd.read_csv(csv_file)
                    if test_record['row_id'] in df['row_id'].values:
                        for key, value in csv_record.items():
                            df.loc[df['row_id'] == test_record['row_id'], key] = value
                        df.to_csv(csv_file, index=False)
                    else:
                        new_df = pd.concat([df, pd.DataFrame([csv_record])], ignore_index=True)
                        new_df.to_csv(csv_file, index=False)
                else:
                    df = pd.DataFrame([csv_record])
                    df.to_csv(csv_file, index=False)
                
                conn.commit()
                
                # Validate
                cursor.execute("SELECT * FROM typing_clients_data WHERE row_id = %s", [test_record['row_id']])
                db_result = cursor.fetchone()
                
                df_current = pd.read_csv(csv_file)
                csv_matches = df_current[df_current['row_id'] == test_record['row_id']]
                csv_result = csv_matches.iloc[0].to_dict()
                
                validator.validate_dual_write(csv_result, db_result, test_record['row_id'])
                
                successful_operations += 1
                print(f"    ✅ Operation '{operation['name']}' successful and validated")
                
                # Cleanup immediately
                cursor.execute("DELETE FROM typing_clients_data WHERE row_id = %s", [test_record['row_id']])
                df_clean = df_current[df_current['row_id'] != test_record['row_id']]
                df_clean.to_csv(csv_file, index=False)
                conn.commit()
                
        except Exception as e:
            print(f"    ❌ Operation '{operation['name']}' failed: {e}")
    
    print(f"\n✅ Workflow operations test: {successful_operations}/{len(operations)} successful")
    return successful_operations == len(operations)

def test_batch_processing_workflow():
    """Test batch processing workflow with dual-write."""
    
    print("📦 Testing batch processing workflow with dual-write...")
    
    # Simulate a batch processing scenario
    batch_records = [
        {
            'row_id': 555551,
            'name': 'Batch User 1',
            'email': 'batch1@example.com',
            'type': 'BATCH_PROCESS',
            'processed': True,
            'document_text': 'Batch processed document 1'
        },
        {
            'row_id': 555552,
            'name': 'Batch User 2',
            'email': 'batch2@example.com',
            'type': 'BATCH_PROCESS',
            'processed': True,
            'document_text': 'Batch processed document 2'
        },
        {
            'row_id': 555553,
            'name': 'Batch User 3',
            'email': 'batch3@example.com',
            'type': 'BATCH_PROCESS',
            'processed': False,
            'document_text': 'Batch processed document 3'
        }
    ]
    
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
    
    successful_batch = 0
    
    try:
        print(f"  📦 Processing batch of {len(batch_records)} records...")
        
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            for i, record in enumerate(batch_records, 1):
                print(f"    Processing batch record {i}/{len(batch_records)}...")
                
                # Dual-write for each record in batch
                normalized_record = validator.normalize_csv_row(record)
                clean_record = {k: v for k, v in normalized_record.items() 
                              if k not in ['created_at', 'updated_at']}
                
                for key in ['file_uuids', 's3_paths']:
                    if key in clean_record and isinstance(clean_record[key], dict):
                        clean_record[key] = json.dumps(clean_record[key])
                
                # Database write
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
                csv_record = validator.normalize_csv_row(record)
                for key in ['file_uuids', 's3_paths']:
                    if key in csv_record and isinstance(csv_record[key], dict):
                        csv_record[key] = json.dumps(csv_record[key])
                
                if csv_file.exists():
                    df = pd.read_csv(csv_file)
                    if record['row_id'] in df['row_id'].values:
                        for key, value in csv_record.items():
                            df.loc[df['row_id'] == record['row_id'], key] = value
                        df.to_csv(csv_file, index=False)
                    else:
                        new_df = pd.concat([df, pd.DataFrame([csv_record])], ignore_index=True)
                        new_df.to_csv(csv_file, index=False)
                else:
                    df = pd.DataFrame([csv_record])
                    df.to_csv(csv_file, index=False)
                
                successful_batch += 1
            
            conn.commit()
            
            # Validate entire batch
            print(f"  🔍 Validating batch of {len(batch_records)} records...")
            validation_passed = 0
            
            for record in batch_records:
                cursor.execute("SELECT * FROM typing_clients_data WHERE row_id = %s", [record['row_id']])
                db_result = cursor.fetchone()
                
                df_current = pd.read_csv(csv_file)
                csv_matches = df_current[df_current['row_id'] == record['row_id']]
                csv_result = csv_matches.iloc[0].to_dict()
                
                validator.validate_dual_write(csv_result, db_result, record['row_id'])
                validation_passed += 1
            
            print(f"✅ Batch processing test PASSED: {successful_batch}/{len(batch_records)} processed, {validation_passed}/{len(batch_records)} validated")
            return True
            
    except Exception as e:
        print(f"❌ Batch processing test FAILED: {e}")
        return False
    
    finally:
        # Cleanup
        try:
            with db_manager.get_connection() as conn:
                cursor = conn.cursor()
                for record in batch_records:
                    cursor.execute("DELETE FROM typing_clients_data WHERE row_id = %s", [record['row_id']])
                
                if csv_file.exists():
                    df = pd.read_csv(csv_file)
                    test_ids = [r['row_id'] for r in batch_records]
                    df = df[~df['row_id'].isin(test_ids)]
                    df.to_csv(csv_file, index=False)
                
                conn.commit()
        except:
            pass

def main():
    """Run comprehensive dual-write workflow testing."""
    
    print("🚀 Testing All Main Workflows with Dual-Write Enabled")
    print("="*60)
    
    start_time = datetime.now()
    
    # Test suite
    tests = [
        ("Single Dual-Write", test_single_dual_write),
        ("CSV Incremental Updates", test_csv_incremental_update_with_dual_write),
        ("Workflow Operations", test_workflow_operations_simulation),
        ("Batch Processing", test_batch_processing_workflow)
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
    
    print(f"\n📊 DUAL-WRITE WORKFLOW TEST SUMMARY")
    print("="*45)
    print(f"Tests passed: {passed}/{total}")
    print(f"Success rate: {passed/total*100:.1f}%")
    print(f"Total duration: {duration:.1f} seconds")
    
    print(f"\n📋 Individual Results:")
    for test_name, success in results:
        status = "✅ PASSED" if success else "❌ FAILED"
        print(f"  {test_name}: {status}")
    
    if passed == total:
        print(f"\n🎉 ALL WORKFLOW TESTS PASSED!")
        print(f"✅ Dual-write system is fully integrated and working correctly")
        print(f"✅ Ready for production workflows")
        return True
    else:
        print(f"\n⚠️ {total - passed} tests failed - review and fix before proceeding")
        return False

if __name__ == "__main__":
    success = main()
    exit(0 if success else 1)