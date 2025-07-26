#!/usr/bin/env python3

import pandas as pd
import json

df = pd.read_csv('outputs/output.csv')

# The 4 people we just processed (excluding 496 James Kirton)
target_rows = [476, 477, 483, 484]

print('S3 FILE LINKS FOR PROCESSED METADATA:')
print('=' * 60)

for row_id in target_rows:
    row = df[df['row_id'] == row_id]
    if not row.empty:
        row_data = row.iloc[0]
        name = row_data.get('name', 'Unknown')
        s3_paths = row_data.get('s3_paths', '[]')
        file_uuids = row_data.get('file_uuids', '[]')
        
        print(f'\n{row_id} - {name}:')
        print('-' * 40)
        
        # Show S3 paths
        if s3_paths and s3_paths != '[]':
            try:
                paths = json.loads(s3_paths) if isinstance(s3_paths, str) else s3_paths
                for i, path in enumerate(paths, 1):
                    print(f'  File {i}: s3://typing-clients-temp/{path}')
            except Exception as e:
                print(f'  Error parsing s3_paths: {e}')
                print(f'  Raw s3_paths: {s3_paths}')
        
        # Show UUIDs for reference
        if file_uuids and file_uuids != '[]':
            try:
                uuids = json.loads(file_uuids) if isinstance(file_uuids, str) else file_uuids
                print(f'  UUIDs: {uuids}')
            except:
                print(f'  Raw file_uuids: {file_uuids}')
        
        if not s3_paths or s3_paths == '[]':
            print('  No S3 paths found')