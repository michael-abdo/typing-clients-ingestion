#!/usr/bin/env python3

import os
import json
import uuid
import boto3
import pandas as pd
from pathlib import Path
from datetime import datetime
import sys

# Add project root to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.download_drive import download_folder_files
from utils.row_context import RowContext

def process_drive_folders():
    """Process Google Drive folders for Patryk and Emilie to download their contents."""
    
    # S3 setup
    s3_client = boto3.client('s3')
    bucket_name = 'typing-clients-uuid-system'
    
    # Folder information from metadata
    folders = [
        {
            'row_id': 476,
            'person_name': 'Patryk Makara',
            'folder_url': 'https://drive.google.com/drive/folders/1lk1xVjPKQvPsGMcBPUvA0KpvHhYJaI71',
            'folder_id': '1lk1xVjPKQvPsGMcBPUvA0KpvHhYJaI71'
        },
        {
            'row_id': 484,
            'person_name': 'Emilie',
            'folder_url': 'https://drive.google.com/drive/folders/1nrNku9G5dnWxGmfawSi6gLNb9Jaij_2r',
            'folder_id': '1nrNku9G5dnWxGmfawSi6gLNb9Jaij_2r'
        }
    ]
    
    # Create backup of CSV
    backup_path = f"outputs/output.csv.backup_folder_processing_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    os.system(f"cp outputs/output.csv {backup_path}")
    print(f"✅ Created CSV backup: {backup_path}")
    
    # Read CSV
    df = pd.read_csv('outputs/output.csv')
    
    all_uploaded_files = {}
    
    for folder_info in folders:
        row_id = folder_info['row_id']
        person_name = folder_info['person_name']
        folder_url = folder_info['folder_url']
        
        print(f"\\n{'='*60}")
        print(f"Processing {person_name} (Row {row_id})")
        print(f"Folder: {folder_url}")
        print(f"{'='*60}")
        
        # Create row context
        row_context = RowContext(
            row_id=str(row_id),
            row_index=row_id,
            type='drive_folder',
            name=person_name,
            email='unknown@example.com'
        )
        
        try:
            # Download folder contents
            print(f"📁 Downloading files from {person_name}'s Google Drive folder...")
            result = download_folder_files(folder_url, row_context)
            
            if not result.success:
                print(f"❌ Failed to download folder contents: {result.error_message}")
                continue
                
            if not result.files_downloaded:
                print(f"ℹ️  No files found in {person_name}'s folder")
                continue
            
            print(f"✅ Downloaded {len(result.files_downloaded)} files locally")
            
            # Upload each downloaded file to S3 with UUID
            uploaded_files = []
            
            for filename in result.files_downloaded:
                local_path = Path(f"downloads/{filename}")
                
                if not local_path.exists():
                    print(f"⚠️  Local file not found: {local_path}")
                    continue
                
                # Generate UUID for S3 file
                file_uuid = str(uuid.uuid4())
                file_ext = local_path.suffix
                s3_key = f"files/{file_uuid}{file_ext}"
                
                # Get file size
                file_size_mb = local_path.stat().st_size / (1024 * 1024)
                
                print(f"📤 Uploading: {filename} ({file_size_mb:.1f} MB)")
                print(f"   UUID: {file_uuid}")
                
                try:
                    # Upload to S3
                    s3_client.upload_file(str(local_path), bucket_name, s3_key)
                    
                    uploaded_files.append({
                        'uuid': file_uuid,
                        's3_key': s3_key,
                        'description': f"From folder: {filename} ({file_size_mb:.1f} MB)",
                        'original_filename': filename
                    })
                    
                    print(f"✅ Uploaded: s3://{bucket_name}/{s3_key}")
                    
                except Exception as e:
                    print(f"❌ Upload failed for {filename}: {e}")
                    continue
            
            if uploaded_files:
                all_uploaded_files[row_id] = uploaded_files
                
                # Update CSV with uploaded file UUIDs
                print(f"📝 Updating CSV for {person_name}...")
                
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
                    
                    if existing_uuids and existing_uuids != '[]':
                        try:
                            existing_uuids_dict = json.loads(existing_uuids)
                            file_uuids.update(existing_uuids_dict)
                        except:
                            pass
                    
                    if existing_paths and existing_paths != '[]':
                        try:
                            existing_paths_dict = json.loads(existing_paths)
                            s3_paths.update(existing_paths_dict)
                        except:
                            pass
                    
                    # Update DataFrame
                    df.loc[mask, 'file_uuids'] = json.dumps(file_uuids)
                    df.loc[mask, 's3_paths'] = json.dumps(s3_paths)
                    
                    print(f"✅ Updated CSV with {len(uploaded_files)} files for {person_name}")
                else:
                    print(f"❌ Row {row_id} not found in CSV")
            
        except Exception as e:
            print(f"❌ Error processing {person_name}'s folder: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Save updated CSV
    df.to_csv('outputs/output.csv', index=False)
    print(f"\\n✅ CSV updated with folder contents")
    
    # Summary
    print(f"\\n{'='*60}")
    print(f"FOLDER PROCESSING SUMMARY:")
    print(f"{'='*60}")
    
    total_files = 0
    for row_id, files in all_uploaded_files.items():
        folder_info = next(f for f in folders if f['row_id'] == row_id)
        print(f"{folder_info['person_name']} (Row {row_id}): {len(files)} files")
        for file_info in files:
            print(f"  ✅ {file_info['uuid']} - {file_info['description']}")
        total_files += len(files)
        print()
    
    print(f"Total files processed from folders: {total_files}")
    
    return all_uploaded_files

if __name__ == "__main__":
    process_drive_folders()