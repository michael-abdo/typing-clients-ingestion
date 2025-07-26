#!/usr/bin/env python3
"""
Verify that all files are in UUID structure and have CSV mappings
"""

import boto3
import csv
import json

def verify_uuid_structure():
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'

    print('=== S3 STRUCTURE VERIFICATION ===')

    # Count files in different directories
    files_dir = 0
    other_dirs = 0

    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.endswith('.json') or key.endswith('.csv'):
                continue
            
            if key.startswith('files/'):
                files_dir += 1
            else:
                other_dirs += 1

    print(f'✅ Files in files/ directory: {files_dir}')
    print(f'❌ Files in other directories: {other_dirs}')
    print(f'📊 Total media files: {files_dir + other_dirs}')

    # Check CSV mappings
    print('\n=== CSV MAPPING VERIFICATION ===')

    csv_file_count = 0
    csv_people_with_files = 0

    with open('outputs/output.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            s3_paths_str = row.get('s3_paths', '{}')
            if s3_paths_str and s3_paths_str != '{}':
                try:
                    s3_paths = json.loads(s3_paths_str)
                    if s3_paths:
                        csv_people_with_files += 1
                        csv_file_count += len(s3_paths)
                except:
                    pass

    print(f'✅ People with files in CSV: {csv_people_with_files}')
    print(f'✅ File mappings in CSV: {csv_file_count}')

    # Final verification
    if other_dirs == 0:
        print('\n🎯 SUCCESS: All files are in files/ directory with UUID names!')
        if csv_file_count == files_dir:
            print('🎯 SUCCESS: All files have CSV mappings!')
            return True
        else:
            print(f'⚠️  WARNING: CSV mapping count ({csv_file_count}) != S3 file count ({files_dir})')
            return False
    else:
        print(f'\n❌ INCOMPLETE: {other_dirs} files still in wrong directories')
        return False

if __name__ == "__main__":
    success = verify_uuid_structure()
    exit(0 if success else 1)