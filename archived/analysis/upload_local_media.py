#!/usr/bin/env python3

import os
import json
import uuid
import boto3
import pandas as pd
from pathlib import Path
from datetime import datetime

def upload_local_media_to_s3():
    """Upload locally downloaded media files to S3 with proper UUIDs and update CSV."""
    
    # S3 setup
    s3_client = boto3.client('s3')
    bucket_name = 'typing-clients-uuid-system'
    
    # Media files to upload
    media_files = [
        {
            'local_path': './downloads/477_Shelly_Chen/250424.mp4',
            'row_id': 477,
            'person_name': 'Shelly Chen',
            'description': 'Downloaded: 250424.mp4'
        },
        {
            'local_path': './downloads/483_Taro/taro official vedio.mp4', 
            'row_id': 483,
            'person_name': 'Taro',
            'description': 'Downloaded: taro official vedio.mp4'
        }
    ]
    
    # Create backup of CSV
    backup_path = f"outputs/output.csv.backup_upload_media_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    os.system(f"cp outputs/output.csv {backup_path}")
    print(f"✅ Created CSV backup: {backup_path}")
    
    # Read CSV
    df = pd.read_csv('outputs/output.csv')
    
    uploaded_files = {}
    
    for media in media_files:
        local_path = Path(media['local_path'])
        
        if not local_path.exists():
            print(f"❌ File not found: {local_path}")
            continue
            
        # Generate UUID for S3 file
        file_uuid = str(uuid.uuid4())
        file_ext = local_path.suffix
        s3_key = f"files/{file_uuid}{file_ext}"
        
        # Get file size
        file_size_mb = local_path.stat().st_size / (1024 * 1024)
        
        print(f"📤 Uploading {media['person_name']}: {local_path.name} ({file_size_mb:.1f} MB)")
        print(f"   UUID: {file_uuid}")
        print(f"   S3 Path: s3://{bucket_name}/{s3_key}")
        
        try:
            # Upload to S3
            s3_client.upload_file(str(local_path), bucket_name, s3_key)
            
            # Store mapping
            uploaded_files[media['row_id']] = {
                'file_uuid': file_uuid,
                's3_key': s3_key,
                'description': f"{media['description']} ({file_size_mb:.1f} MB)",
                'original_filename': local_path.name
            }
            
            print(f"✅ Upload successful: {s3_key}")
            
        except Exception as e:
            print(f"❌ Upload failed for {media['person_name']}: {e}")
            continue
    
    # Update CSV with real S3 paths
    print(f"\n📝 Updating CSV with real S3 UUID paths...")
    
    for row_id, file_info in uploaded_files.items():
        # Find row in DataFrame
        mask = df['row_id'] == row_id
        if not mask.any():
            print(f"❌ Row {row_id} not found in CSV")
            continue
            
        # Create proper JSON mappings
        file_uuids = {file_info['description']: file_info['file_uuid']}
        s3_paths = {file_info['file_uuid']: s3_key}
        
        # Update DataFrame
        df.loc[mask, 'file_uuids'] = json.dumps(file_uuids)
        df.loc[mask, 's3_paths'] = json.dumps(s3_paths)
        
        print(f"✅ Updated CSV row {row_id} with UUID: {file_info['file_uuid']}")
    
    # Save updated CSV
    df.to_csv('outputs/output.csv', index=False)
    print(f"✅ CSV updated with real S3 UUID paths")
    
    # Summary
    print(f"\n{'='*60}")
    print(f"UPLOAD SUMMARY:")
    print(f"{'='*60}")
    for row_id, file_info in uploaded_files.items():
        print(f"Row {row_id}: s3://{bucket_name}/{file_info['s3_key']}")
        print(f"  UUID: {file_info['file_uuid']}")
        print(f"  Size: {file_info['description']}")
        print()
    
    return uploaded_files

if __name__ == "__main__":
    upload_local_media_to_s3()