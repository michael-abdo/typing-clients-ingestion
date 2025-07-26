#!/usr/bin/env python3
"""
Fix CSV mappings to include proper file extensions for files in S3
"""

import boto3
import csv
import json
import shutil
from datetime import datetime

def fix_csv_file_extensions():
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'

    print('=== FIXING CSV FILE EXTENSIONS ===')

    # Get all existing S3 files with their full paths
    s3_files = {}  # uuid -> full_path
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.startswith('files/') and not key.endswith('.json') and not key.endswith('.csv'):
                # Extract UUID (remove files/ prefix and extension)
                filename = key.split('/')[-1]
                uuid_part = filename.split('.')[0] if '.' in filename else filename
                s3_files[uuid_part] = key

    print(f'Found {len(s3_files)} files in S3 files/ directory')

    # Backup current CSV
    backup_file = f'outputs/output.csv.backup_extensions_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
    shutil.copy('outputs/output.csv', backup_file)
    print(f'Created backup: {backup_file}')

    # Fix CSV mappings
    fixed_rows = []
    total_fixes = 0
    people_fixed = 0

    with open('outputs/output.csv', 'r') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        
        for row in reader:
            person_id = row.get('row_id', '').strip()
            person_name = row.get('name', '').strip()
            
            s3_paths_str = row.get('s3_paths', '{}')
            
            if s3_paths_str and s3_paths_str != '{}':
                try:
                    s3_paths = json.loads(s3_paths_str)
                    
                    fixed_s3_paths = {}
                    person_fixes = 0
                    
                    for uuid_key, path in s3_paths.items():
                        # Check if this UUID exists in S3 with proper extension
                        if uuid_key in s3_files:
                            correct_path = s3_files[uuid_key]
                            fixed_s3_paths[uuid_key] = correct_path
                            
                            if path != correct_path:
                                person_fixes += 1
                                print(f'Fixed: {person_id} ({person_name}) {uuid_key}: {path} -> {correct_path}')
                        else:
                            print(f'WARNING: {person_id} ({person_name}) UUID {uuid_key} not found in S3')
                    
                    if person_fixes > 0:
                        people_fixed += 1
                        total_fixes += person_fixes
                    
                    # Update row with fixed mappings
                    row['s3_paths'] = json.dumps(fixed_s3_paths)
                        
                except Exception as e:
                    print(f"Error processing person {person_id}: {e}")
            
            fixed_rows.append(row)

    # Write fixed CSV
    with open('outputs/output.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(fixed_rows)

    print(f'\n=== FIX SUMMARY ===')
    print(f'✅ Total path fixes: {total_fixes}')
    print(f'👥 People affected: {people_fixed}')
    print(f'💾 Backup saved as: {backup_file}')
    print(f'🎯 CSV now references files with correct extensions!')

if __name__ == "__main__":
    fix_csv_file_extensions()