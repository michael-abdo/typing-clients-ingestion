#!/usr/bin/env python3

import pandas as pd
import json

df = pd.read_csv('outputs/output.csv')

# The 4 people we just processed (excluding 496 James Kirton)
target_rows = [476, 477, 483, 484]

print('ACTUAL S3 UUID FILE LINKS FOR PROCESSED METADATA:')
print('=' * 70)

for row_id in target_rows:
    row = df[df['row_id'] == row_id]
    if not row.empty:
        row_data = row.iloc[0]
        name = row_data.get('name', 'Unknown')
        file_uuids = row_data.get('file_uuids', '[]')
        
        print(f'\n{row_id} - {name}:')
        print('-' * 50)
        
        if file_uuids and file_uuids != '[]':
            try:
                uuids = json.loads(file_uuids) if isinstance(file_uuids, str) else file_uuids
                if isinstance(uuids, dict):
                    for i, (desc, uuid_name) in enumerate(uuids.items(), 1):
                        # Show both the S3 link and description
                        print(f'  File {i}: s3://typing-clients-uuid-system/files/{uuid_name}')
                        print(f'    Description: {desc}')
                elif isinstance(uuids, list):
                    for i, uuid_name in enumerate(uuids, 1):
                        print(f'  File {i}: s3://typing-clients-uuid-system/files/{uuid_name}')
                else:
                    print(f'  Raw file_uuids: {uuids}')
            except Exception as e:
                print(f'  Error parsing file_uuids: {e}')
                print(f'  Raw file_uuids: {file_uuids}')
        else:
            print('  No file UUIDs found')
            
print(f'\n📁 All files are stored in: s3://typing-clients-uuid-system/files/')
print(f'🔒 Files use secure UUID names (non-enumerable)')
print(f'🗂️  CSV contains the UUID→description mappings')