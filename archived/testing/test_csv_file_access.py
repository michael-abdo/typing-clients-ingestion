#!/usr/bin/env python3
"""
Test that all files can be accessed through CSV mappings
"""

import boto3
import csv
import json
import random

def test_csv_file_access():
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'

    print('=== TESTING CSV FILE ACCESS ===')

    # Get all people with files from CSV
    people_with_files = []
    
    with open('outputs/output.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            person_id = row.get('row_id', '').strip()
            person_name = row.get('name', '').strip()
            
            s3_paths_str = row.get('s3_paths', '{}')
            if s3_paths_str and s3_paths_str != '{}':
                try:
                    s3_paths = json.loads(s3_paths_str)
                    if s3_paths:
                        people_with_files.append({
                            'id': person_id,
                            'name': person_name,
                            'files': s3_paths
                        })
                except:
                    pass

    print(f'Found {len(people_with_files)} people with files')

    # Test a sample of files
    total_tests = 0
    successful_tests = 0
    
    # Test all people, but sample files for people with many files
    for person in people_with_files:
        person_id = person['id']
        person_name = person['name']
        files = person['files']
        
        # Sample up to 3 files per person for testing
        files_to_test = list(files.items())
        if len(files_to_test) > 3:
            files_to_test = random.sample(files_to_test, 3)
        
        for uuid_key, s3_path in files_to_test:
            total_tests += 1
            
            try:
                # Test if file exists and is accessible
                response = s3.head_object(Bucket=bucket, Key=s3_path)
                file_size = response['ContentLength']
                successful_tests += 1
                
                print(f'✅ {person_id} ({person_name}): {uuid_key} -> {s3_path} ({file_size:,} bytes)')
                
            except Exception as e:
                print(f'❌ {person_id} ({person_name}): {uuid_key} -> {s3_path} - ERROR: {e}')

    print(f'\n=== TEST RESULTS ===')
    print(f'📊 Total files tested: {total_tests}')
    print(f'✅ Successful accesses: {successful_tests}')
    print(f'❌ Failed accesses: {total_tests - successful_tests}')
    
    if successful_tests == total_tests:
        print(f'\n🎯 PERFECT: All tested files are accessible through CSV mappings!')
        return True
    else:
        success_rate = (successful_tests / total_tests) * 100 if total_tests > 0 else 0
        print(f'\n⚠️  SUCCESS RATE: {success_rate:.1f}% ({successful_tests}/{total_tests})')
        return False

if __name__ == "__main__":
    success = test_csv_file_access()
    exit(0 if success else 1)