#!/usr/bin/env python3
"""
Detailed verification of UUID structure and mappings
"""

import boto3
import csv
import json
from collections import defaultdict

def detailed_verification():
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'

    print('=== DETAILED S3 AND CSV VERIFICATION ===')

    # Get all S3 files
    s3_files = set()
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if key.startswith('files/') and not key.endswith('.json') and not key.endswith('.csv'):
                # Extract UUID (remove files/ prefix and extension)
                filename = key.split('/')[-1]
                uuid_part = filename.split('.')[0] if '.' in filename else filename
                s3_files.add(uuid_part)

    print(f'S3 files in files/ directory: {len(s3_files)}')

    # Get all CSV mappings
    csv_mappings = defaultdict(list)
    csv_uuids = set()
    
    with open('outputs/output.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            person_id = row.get('row_id', '').strip()
            person_name = row.get('name', '').strip()
            
            s3_paths_str = row.get('s3_paths', '{}')
            if s3_paths_str and s3_paths_str != '{}':
                try:
                    s3_paths = json.loads(s3_paths_str)
                    for uuid_key, path in s3_paths.items():
                        csv_uuids.add(uuid_key)
                        csv_mappings[person_id].append({
                            'uuid': uuid_key,
                            'path': path,
                            'name': person_name
                        })
                except Exception as e:
                    print(f"Error parsing s3_paths for person {person_id}: {e}")

    print(f'CSV UUID mappings: {len(csv_uuids)}')
    print(f'People with files: {len(csv_mappings)}')

    # Find discrepancies
    s3_only = s3_files - csv_uuids
    csv_only = csv_uuids - s3_files
    common = s3_files & csv_uuids

    print(f'\n=== MAPPING VERIFICATION ===')
    print(f'✅ Files with correct CSV mapping: {len(common)}')
    print(f'❌ S3 files without CSV mapping: {len(s3_only)}')
    print(f'❌ CSV mappings without S3 file: {len(csv_only)}')

    if s3_only:
        print(f'\nS3 files without CSV mapping (first 10):')
        for uuid in list(s3_only)[:10]:
            print(f'  {uuid}')

    if csv_only:
        print(f'\nCSV mappings without S3 file (first 10):')
        for uuid in list(csv_only)[:10]:
            # Find which person this belongs to
            for person_id, mappings in csv_mappings.items():
                for mapping in mappings:
                    if mapping['uuid'] == uuid:
                        print(f'  {uuid} - Person {person_id} ({mapping["name"]}): {mapping["path"]}')
                        break

    # Final result
    orphaned_files = len(s3_only)
    missing_files = len(csv_only)
    
    if orphaned_files == 0 and missing_files == 0:
        print(f'\n🎯 PERFECT: All {len(common)} files have correct UUID structure and CSV mappings!')
        return True
    else:
        print(f'\n⚠️  ISSUES FOUND:')
        if orphaned_files > 0:
            print(f'   - {orphaned_files} orphaned files (in S3 but not in CSV)')
        if missing_files > 0:
            print(f'   - {missing_files} missing files (in CSV but not in S3)')
        return False

if __name__ == "__main__":
    success = detailed_verification()
    exit(0 if success else 1)