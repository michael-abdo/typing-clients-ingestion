#!/usr/bin/env python3

import pandas as pd
import json

# Read CSV
df = pd.read_csv('outputs/output.csv')

# Fix the s3_paths mappings
fixes = {
    477: {
        'uuid': '0198dc12-bc51-4d99-bae5-b50954bf3e35',
        's3_key': 'files/0198dc12-bc51-4d99-bae5-b50954bf3e35.mp4'
    },
    483: {
        'uuid': '9647c50b-3d92-4614-8eec-6a11a902861b', 
        's3_key': 'files/9647c50b-3d92-4614-8eec-6a11a902861b.mp4'
    }
}

for row_id, fix in fixes.items():
    mask = df['row_id'] == row_id
    if mask.any():
        # Fix s3_paths mapping
        s3_paths = {fix['uuid']: fix['s3_key']}
        df.loc[mask, 's3_paths'] = json.dumps(s3_paths)
        print(f"✅ Fixed s3_paths for row {row_id}")

# Save updated CSV
df.to_csv('outputs/output.csv', index=False)
print("✅ CSV paths corrected")