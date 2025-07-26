#!/usr/bin/env python3
"""
Verify that all S3 files are mapped to CSV entries
"""

import csv
import json
import boto3

def verify_mapping():
    # Get all S3 files
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    s3_files = set()
    
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.startswith('files/'):  # Skip orphaned directory
                s3_files.add(key)
    
    print(f"Total files in S3 (excluding orphaned): {len(s3_files)}")
    
    # Get all paths from CSV
    csv_paths = set()
    csv_rows_with_files = 0
    
    with open('outputs/output.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            s3_paths_str = row.get('s3_paths', '')
            if s3_paths_str and s3_paths_str != '{}':
                try:
                    s3_paths = json.loads(s3_paths_str)
                    csv_rows_with_files += 1
                    for path in s3_paths.values():
                        csv_paths.add(path)
                except:
                    pass
    
    print(f"Total paths in CSV: {len(csv_paths)}")
    print(f"CSV rows with S3 files: {csv_rows_with_files}")
    
    # Compare
    in_s3_not_csv = s3_files - csv_paths
    in_csv_not_s3 = csv_paths - s3_files
    
    print(f"\nFiles in S3 but not in CSV: {len(in_s3_not_csv)}")
    if in_s3_not_csv:
        for f in list(in_s3_not_csv)[:5]:
            print(f"  - {f}")
    
    print(f"\nPaths in CSV but not in S3: {len(in_csv_not_s3)}")
    if in_csv_not_s3:
        for f in list(in_csv_not_s3)[:5]:
            print(f"  - {f}")
    
    # Check orphaned directory
    orphaned_count = 0
    for page in paginator.paginate(Bucket=bucket, Prefix='files/'):
        orphaned_count += len(page.get('Contents', []))
    
    print(f"\nOrphaned files remaining: {orphaned_count}")
    
    if len(in_s3_not_csv) == 0 and orphaned_count == 0:
        print("\n✅ SUCCESS: All S3 files are mapped to CSV entries!")
        print("✅ All UUID files have been recovered and mapped!")
    else:
        print("\n❌ Some files are not properly mapped")

if __name__ == "__main__":
    verify_mapping()