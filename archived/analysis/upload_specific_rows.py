#!/usr/bin/env python3
"""
Upload specific rows to S3
"""
import boto3
import os
from pathlib import Path
import json
from datetime import datetime

# Initialize S3 client
s3 = boto3.client('s3')
bucket_name = 'typing-clients-uuid-system'

# Specific directories to upload
target_dirs = {
    503: 'downloads/503_Augusto_Evangelista',
    504: 'downloads/504_Navid_Ghahremani', 
    506: 'downloads/506_Ryan_Madera'
}

results = []

for row_id, dir_path in target_dirs.items():
    if not os.path.exists(dir_path):
        print(f"❌ Directory not found: {dir_path}")
        continue
    
    print(f"\n📤 Uploading files for row {row_id}")
    dir_path = Path(dir_path)
    
    # Upload all files in directory
    for file_path in dir_path.iterdir():
        if file_path.is_file():
            # Skip JSON metadata files
            if file_path.suffix == '.json':
                continue
                
            # Create S3 key
            s3_key = f"{row_id}/{file_path.parent.name}/{file_path.name}"
            
            try:
                print(f"  ⬆️  Uploading {file_path.name} to s3://{bucket_name}/{s3_key}")
                
                # Upload file
                s3.upload_file(
                    str(file_path),
                    bucket_name,
                    s3_key,
                    ExtraArgs={'ContentType': 'video/mp4' if file_path.suffix == '.mp4' else 'video/x-matroska'}
                )
                
                print(f"  ✅ Success: {file_path.name}")
                results.append({
                    'row_id': row_id,
                    'file': file_path.name,
                    's3_key': s3_key,
                    'status': 'success'
                })
                
            except Exception as e:
                print(f"  ❌ Failed: {file_path.name} - {str(e)}")
                results.append({
                    'row_id': row_id,
                    'file': file_path.name,
                    'error': str(e),
                    'status': 'failed'
                })

# Save report
report = {
    'timestamp': datetime.now().isoformat(),
    'bucket': bucket_name,
    'results': results
}

with open('s3_upload_specific_rows_report.json', 'w') as f:
    json.dump(report, f, indent=2)

print(f"\n📊 Upload complete. Report saved to s3_upload_specific_rows_report.json")
print(f"✅ Successful uploads: {len([r for r in results if r['status'] == 'success'])}")
print(f"❌ Failed uploads: {len([r for r in results if r['status'] == 'failed'])}")