#!/usr/bin/env python3
"""Recover the 11 orphaned files that belong to Kaioxys DarkMacro (Row 472)"""

import boto3
import pandas as pd
import json
from orphaned_file_recovery_tracker import get_tracker

def recover_kaioxys_files():
    """Recover and map the 11 files belonging to Kaioxys DarkMacro"""
    tracker = get_tracker()
    
    # The 11 UUIDs that belong to Kaioxys DarkMacro (Row 472) from the log
    kaioxys_uuids = [
        "3ad7eaba-2c1b-41d6-a192-ac49889c9816",  # .mp3
        "b1881e9f-28e2-49dc-bdec-654951b2f290",  # .mp4
        "debecb6e-6044-4e1d-ab8b-d3f714553de1",  # .mp3
        "14095d79-6b05-471b-8eff-04cdb3ced948",  # .mp4
        "20af35fd-7324-4ea3-9014-c825fe183b10",  # .mp3
        "33bb8baa-8d51-4e5e-b9f4-b0442b8463c4",  # .mp4
        "9cdf7d10-6b66-46ad-ac47-a303c48cd418",  # .webm
        "a10ea218-3498-46b8-b2ed-71a56f77295b",  # .mp3 (might not exist in current orphaned files)
        "708cdc6b-f887-457e-8e60-935b328fa411",  # .mp4
        "175f229b-3fcd-4d91-bd42-caae0e9e2285",  # .part
        "27cc3d6a-6846-4efb-8d3e-567567f884b1"   # .part
    ]
    
    try:
        # Load current CSV
        df = pd.read_csv('outputs/output.csv')
        
        # Check if Row 472 exists in current CSV
        kaioxys_row = df[df['row_id'] == 472]
        
        if kaioxys_row.empty:
            print("❌ Row 472 (Kaioxys DarkMacro) not found in current CSV")
            print("   This might be an older client that was removed")
            return False
        
        kaioxys_info = kaioxys_row.iloc[0]
        print(f"✅ Found Kaioxys DarkMacro in CSV:")
        print(f"   Row ID: {kaioxys_info['row_id']}")
        print(f"   Name: {kaioxys_info['name']}")
        print(f"   Email: {kaioxys_info.get('email', 'N/A')}")
        print(f"   Type: {kaioxys_info.get('type', 'N/A')}")
        
        # Check which UUIDs actually exist in S3 orphaned files
        s3 = boto3.client('s3')
        bucket = 'typing-clients-uuid-system'
        
        # Get all orphaned files
        response = s3.list_objects_v2(Bucket=bucket, Prefix="files/")
        existing_orphaned = set()
        orphaned_files_info = {}
        
        if 'Contents' in response:
            for obj in response['Contents']:
                if not obj['Key'].endswith('/'):
                    uuid = obj['Key'].split('/')[-1].split('.')[0]
                    existing_orphaned.add(uuid)
                    orphaned_files_info[uuid] = {
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified']
                    }
        
        # Find which Kaioxys UUIDs exist in orphaned files
        found_kaioxys_files = []
        missing_kaioxys_files = []
        
        for uuid in kaioxys_uuids:
            if uuid in existing_orphaned:
                found_kaioxys_files.append({
                    'uuid': uuid,
                    'file_info': orphaned_files_info[uuid]
                })
            else:
                missing_kaioxys_files.append(uuid)
        
        print(f"\n📊 Kaioxys File Analysis:")
        print(f"   Expected files: {len(kaioxys_uuids)}")
        print(f"   Found in orphaned: {len(found_kaioxys_files)}")
        print(f"   Missing/Not orphaned: {len(missing_kaioxys_files)}")
        
        if found_kaioxys_files:
            print(f"\n✅ Found Kaioxys files in orphaned directory:")
            total_size = 0
            for file_info in found_kaioxys_files:
                size_mb = file_info['file_info']['size'] / (1024 * 1024)
                total_size += file_info['file_info']['size']
                print(f"   - {file_info['uuid']} ({size_mb:.1f} MB)")
            
            total_size_mb = total_size / (1024 * 1024)
            print(f"   Total size: {total_size_mb:.1f} MB")
            
            # Update CSV with UUID mappings for Kaioxys
            print(f"\n🔄 Updating CSV with Kaioxys UUID mappings...")
            
            # Get current UUID data for Kaioxys
            current_file_uuids = kaioxys_info.get('file_uuids', '{}')
            current_youtube_uuids = kaioxys_info.get('youtube_uuids', '[]')
            current_drive_uuids = kaioxys_info.get('drive_uuids', '[]')
            current_s3_paths = kaioxys_info.get('s3_paths', '{}')
            
            # Parse current data
            try:
                file_uuids = json.loads(str(current_file_uuids)) if current_file_uuids != '{}' else {}
                youtube_uuids = json.loads(str(current_youtube_uuids)) if current_youtube_uuids != '[]' else []
                drive_uuids = json.loads(str(current_drive_uuids)) if current_drive_uuids != '[]' else []
                s3_paths = json.loads(str(current_s3_paths)) if current_s3_paths != '{}' else {}
            except:
                file_uuids = {}
                youtube_uuids = []
                drive_uuids = []
                s3_paths = {}
            
            # Add found UUIDs
            for file_info in found_kaioxys_files:
                uuid = file_info['uuid']
                s3_key = file_info['file_info']['key']
                filename = s3_key.split('/')[-1]
                
                # Add to file_uuids mapping
                file_uuids[filename] = uuid
                
                # Add to s3_paths mapping
                s3_paths[uuid] = s3_key
                
                # Determine if it's YouTube or Drive based on file extension
                if any(s3_key.endswith(ext) for ext in ['.mp3', '.mp4', '.m4a', '.webm']):
                    if uuid not in youtube_uuids:
                        youtube_uuids.append(uuid)
                else:
                    if uuid not in drive_uuids:
                        drive_uuids.append(uuid)
            
            # Update the CSV row
            df.loc[df['row_id'] == 472, 'file_uuids'] = json.dumps(file_uuids)
            df.loc[df['row_id'] == 472, 'youtube_uuids'] = json.dumps(youtube_uuids)
            df.loc[df['row_id'] == 472, 'drive_uuids'] = json.dumps(drive_uuids)
            df.loc[df['row_id'] == 472, 's3_paths'] = json.dumps(s3_paths)
            
            # Save updated CSV
            df.to_csv('outputs/output.csv', index=False)
            
            print(f"✅ Updated Kaioxys DarkMacro (Row 472) with {len(found_kaioxys_files)} recovered UUIDs")
            print(f"   File UUIDs: {len(file_uuids)} total")
            print(f"   YouTube UUIDs: {len(youtube_uuids)}")
            print(f"   Drive UUIDs: {len(drive_uuids)}")
            print(f"   S3 Paths: {len(s3_paths)}")
            
            # Move files from orphaned directory to client directory
            print(f"\n📦 Moving files to client directory...")
            
            moved_files = []
            for file_info in found_kaioxys_files:
                old_key = file_info['file_info']['key']
                filename = old_key.split('/')[-1]
                new_key = f"clients/472/{filename}"
                
                try:
                    # Copy to new location
                    s3.copy_object(
                        CopySource={'Bucket': bucket, 'Key': old_key},
                        Bucket=bucket,
                        Key=new_key
                    )
                    
                    # Delete from old location
                    s3.delete_object(Bucket=bucket, Key=old_key)
                    
                    moved_files.append({
                        'old_key': old_key,
                        'new_key': new_key,
                        'uuid': file_info['uuid']
                    })
                    
                    print(f"   ✅ Moved {filename}")
                    
                except Exception as e:
                    print(f"   ❌ Failed to move {filename}: {str(e)}")
            
            print(f"\n🎉 RECOVERY COMPLETE!")
            print(f"   Files recovered: {len(moved_files)}")
            print(f"   Client: Kaioxys DarkMacro (Row 472)")
            print(f"   Total size: {total_size_mb:.1f} MB")
            
            # Update S3 paths in CSV to reflect new location
            updated_s3_paths = {}
            for move_info in moved_files:
                updated_s3_paths[move_info['uuid']] = move_info['new_key']
            
            # Merge with existing s3_paths
            s3_paths.update(updated_s3_paths)
            df.loc[df['row_id'] == 472, 's3_paths'] = json.dumps(s3_paths)
            df.to_csv('outputs/output.csv', index=False)
            
            tracker.log("SUCCESS", "Kaioxys files recovered successfully", {
                'client_row_id': 472,
                'client_name': 'Kaioxys DarkMacro',
                'files_recovered': len(moved_files),
                'total_size_mb': total_size_mb
            })
            
            return {
                'success': True,
                'files_recovered': len(moved_files),
                'client_info': {
                    'row_id': 472,
                    'name': 'Kaioxys DarkMacro'
                },
                'moved_files': moved_files
            }
        
        else:
            print(f"\n❌ No Kaioxys files found in orphaned directory")
            return {'success': False, 'reason': 'no_files_found'}
        
        if missing_kaioxys_files:
            print(f"\n⚠️  Missing Kaioxys files (not in orphaned directory):")
            for uuid in missing_kaioxys_files:
                print(f"   - {uuid}")
    
    except Exception as e:
        tracker.add_error('kaioxys_recovery', {
            'error_type': 'recovery_error',
            'error_message': str(e)
        })
        raise

if __name__ == "__main__":
    print("🔍 RECOVERING KAIOXYS DARKMACRO FILES")
    print("=" * 60)
    
    result = recover_kaioxys_files()
    
    if result.get('success'):
        print(f"\n✅ SUCCESS: Recovered {result['files_recovered']} files for {result['client_info']['name']}")
    else:
        print(f"\n❌ FAILED: {result.get('reason', 'Unknown error')}")