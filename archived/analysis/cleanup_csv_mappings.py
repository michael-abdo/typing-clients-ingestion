#!/usr/bin/env python3
"""
Clean up CSV mappings to only reference files that actually exist in S3
"""

import boto3
import csv
import json
import shutil
from datetime import datetime

def cleanup_csv_mappings():
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'

    print('=== CLEANING UP CSV MAPPINGS ===')

    # Get all existing S3 files
    existing_files = set()
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.startswith('files/') and not key.endswith('.json') and not key.endswith('.csv'):
                # Extract UUID (remove files/ prefix and extension)
                filename = key.split('/')[-1]
                uuid_part = filename.split('.')[0] if '.' in filename else filename
                existing_files.add(uuid_part)

    print(f'Found {len(existing_files)} existing files in S3')

    # Backup current CSV
    backup_file = f'outputs/output.csv.backup_cleanup_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
    shutil.copy('outputs/output.csv', backup_file)
    print(f'Created backup: {backup_file}')

    # Clean up CSV
    cleaned_rows = []
    total_mappings_before = 0
    total_mappings_after = 0
    people_affected = 0

    with open('outputs/output.csv', 'r') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        
        for row in reader:
            person_id = row.get('row_id', '').strip()
            person_name = row.get('name', '').strip()
            
            s3_paths_str = row.get('s3_paths', '{}')
            file_uuids_str = row.get('file_uuids', '{}')
            
            if s3_paths_str and s3_paths_str != '{}':
                try:
                    s3_paths = json.loads(s3_paths_str)
                    file_uuids = json.loads(file_uuids_str) if file_uuids_str and file_uuids_str != '{}' else {}
                    
                    original_count = len(s3_paths)
                    total_mappings_before += original_count
                    
                    # Filter to only include existing files
                    cleaned_s3_paths = {}
                    cleaned_file_uuids = {}
                    
                    for uuid_key, path in s3_paths.items():
                        if uuid_key in existing_files:
                            cleaned_s3_paths[uuid_key] = f'files/{uuid_key}'
                            # Find corresponding filename in file_uuids
                            for filename, uuid_val in file_uuids.items():
                                if uuid_val == uuid_key:
                                    cleaned_file_uuids[filename] = uuid_val
                                    break
                    
                    # Update row with cleaned mappings
                    if cleaned_s3_paths:
                        row['s3_paths'] = json.dumps(cleaned_s3_paths)
                        row['file_uuids'] = json.dumps(cleaned_file_uuids)
                        total_mappings_after += len(cleaned_s3_paths)
                    else:
                        # No valid files for this person
                        row['s3_paths'] = '{}'
                        row['file_uuids'] = '{}'
                    
                    if original_count != len(cleaned_s3_paths):
                        people_affected += 1
                        removed = original_count - len(cleaned_s3_paths)
                        print(f'Person {person_id} ({person_name}): Removed {removed} invalid mappings, {len(cleaned_s3_paths)} remaining')
                        
                except Exception as e:
                    print(f"Error processing person {person_id}: {e}")
            
            cleaned_rows.append(row)

    # Write cleaned CSV
    with open('outputs/output.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(cleaned_rows)

    print(f'\n=== CLEANUP SUMMARY ===')
    print(f'✅ Mappings before cleanup: {total_mappings_before}')
    print(f'✅ Mappings after cleanup: {total_mappings_after}')
    print(f'🗑️  Removed invalid mappings: {total_mappings_before - total_mappings_after}')
    print(f'👥 People affected: {people_affected}')
    print(f'💾 Backup saved as: {backup_file}')

    if total_mappings_after == len(existing_files):
        print(f'\n🎯 SUCCESS: CSV now has perfect 1:1 mapping with S3 files!')
        return True
    else:
        print(f'\n⚠️  WARNING: CSV mappings ({total_mappings_after}) != S3 files ({len(existing_files)})')
        return False

if __name__ == "__main__":
    success = cleanup_csv_mappings()
    exit(0 if success else 1)