#!/usr/bin/env python3

import os
import json
import uuid
import boto3
import pandas as pd
from pathlib import Path
from datetime import datetime

def upload_folder_files_to_s3():
    """Upload the downloaded folder files to S3 with proper UUIDs and update CSV."""
    
    # S3 setup
    s3_client = boto3.client('s3')
    bucket_name = 'typing-clients-uuid-system'
    
    # Downloaded files mapping
    patryk_files = [
        'Blooper 1.mp4',
        'Blooper 2.mp4', 
        'Lifting weight.mp4',
        'Optional - Old Typing Video worse quality.mp4',
        'Random guitar faces.mp4'
    ]
    
    emilie_files = [
        '1.Please read me.docx',
        '2.Questions I answered.docx'
    ]
    
    file_mappings = [
        {
            'row_id': 476,
            'person_name': 'Patryk Makara',
            'files': patryk_files
        },
        {
            'row_id': 484,
            'person_name': 'Emilie',
            'files': emilie_files
        }
    ]
    
    # Create backup of CSV
    backup_path = f"outputs/output.csv.backup_folder_upload_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    os.system(f"cp outputs/output.csv {backup_path}")
    print(f"✅ Created CSV backup: {backup_path}")
    
    # Read CSV
    df = pd.read_csv('outputs/output.csv')
    
    all_uploaded_files = {}
    
    for mapping in file_mappings:
        row_id = mapping['row_id']
        person_name = mapping['person_name']
        files = mapping['files']
        
        print(f"\\n{'='*60}")
        print(f"Uploading files for {person_name} (Row {row_id})")
        print(f"{'='*60}")
        
        uploaded_files = []
        
        for filename in files:
            local_path = Path(f"drive_downloads/{filename}")
            
            if not local_path.exists():
                print(f"❌ File not found: {local_path}")
                continue
            
            # Generate UUID for S3 file
            file_uuid = str(uuid.uuid4())
            file_ext = local_path.suffix
            s3_key = f"files/{file_uuid}{file_ext}"
            
            # Get file size
            file_size_bytes = local_path.stat().st_size
            file_size_mb = file_size_bytes / (1024 * 1024)
            
            print(f"📤 Uploading: {filename} ({file_size_mb:.1f} MB)")
            print(f"   UUID: {file_uuid}")
            print(f"   S3 Path: s3://{bucket_name}/{s3_key}")
            
            try:
                # Upload to S3
                s3_client.upload_file(str(local_path), bucket_name, s3_key)
                
                uploaded_files.append({
                    'uuid': file_uuid,
                    's3_key': s3_key,
                    'description': f"From folder: {filename} ({file_size_mb:.1f} MB)",
                    'original_filename': filename,
                    'file_size_bytes': file_size_bytes
                })
                
                print(f"✅ Upload successful: {s3_key}")
                
            except Exception as e:
                print(f"❌ Upload failed for {filename}: {e}")
                continue
        
        if uploaded_files:
            all_uploaded_files[row_id] = uploaded_files
            
            # Update CSV with uploaded file UUIDs
            print(f"\\n📝 Updating CSV for {person_name}...")
            
            # Create JSON mappings
            file_uuids = {}
            s3_paths = {}
            
            for file_info in uploaded_files:
                file_uuids[file_info['description']] = file_info['uuid']
                s3_paths[file_info['uuid']] = file_info['s3_key']
            
            # Find row in DataFrame and update
            mask = df['row_id'] == row_id
            if mask.any():
                # Get existing mappings and merge
                existing_uuids = df.loc[mask, 'file_uuids'].iloc[0]
                existing_paths = df.loc[mask, 's3_paths'].iloc[0]
                
                if existing_uuids and existing_uuids not in ['[]', '{}', None]:
                    try:
                        existing_uuids_dict = json.loads(existing_uuids)
                        if isinstance(existing_uuids_dict, dict):
                            file_uuids.update(existing_uuids_dict)
                    except:
                        pass
                
                if existing_paths and existing_paths not in ['[]', '{}', None]:
                    try:
                        existing_paths_dict = json.loads(existing_paths)
                        if isinstance(existing_paths_dict, dict):
                            s3_paths.update(existing_paths_dict)
                    except:
                        pass
                
                # Update DataFrame
                df.loc[mask, 'file_uuids'] = json.dumps(file_uuids)
                df.loc[mask, 's3_paths'] = json.dumps(s3_paths)
                
                print(f"✅ Updated CSV with {len(uploaded_files)} files for {person_name}")
            else:
                print(f"❌ Row {row_id} not found in CSV")
        else:
            print(f"❌ No files uploaded for {person_name}")
    
    # Save updated CSV
    df.to_csv('outputs/output.csv', index=False)
    print(f"\\n✅ CSV updated with folder file mappings")
    
    # Summary
    print(f"\\n{'='*70}")
    print(f"FOLDER FILE UPLOAD SUMMARY:")
    print(f"{'='*70}")
    
    total_files = 0
    total_size = 0
    
    for row_id, files in all_uploaded_files.items():
        mapping = next(m for m in file_mappings if m['row_id'] == row_id)
        print(f"\\n{mapping['person_name']} (Row {row_id}): {len(files)} files")
        
        for file_info in files:
            size_mb = file_info['file_size_bytes'] / (1024 * 1024)
            print(f"  ✅ s3://{bucket_name}/{file_info['s3_key']}")
            print(f"     UUID: {file_info['uuid']}")
            print(f"     File: {file_info['original_filename']} ({size_mb:.1f} MB)")
            total_size += file_info['file_size_bytes']
        
        total_files += len(files)
    
    total_size_mb = total_size / (1024 * 1024)
    total_size_gb = total_size / (1024 * 1024 * 1024)
    
    print(f"\\n📊 TOTALS:")
    print(f"   Files uploaded: {total_files}")
    print(f"   Total size: {total_size_mb:.1f} MB ({total_size_gb:.2f} GB)")
    print(f"\\n🎯 ALL FOLDER CONTENTS NOW IN S3 WITH UUID REFERENCES!")
    
    return all_uploaded_files

if __name__ == "__main__":
    upload_folder_files_to_s3()