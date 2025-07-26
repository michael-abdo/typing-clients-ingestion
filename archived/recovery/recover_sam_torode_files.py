#!/usr/bin/env python3
"""Recover the 6 orphaned files that belong to Sam Torode (Row 502)"""

import boto3
import pandas as pd
import json
from orphaned_file_recovery_tracker import get_tracker

def recover_sam_torode_files():
    """Recover and map the 6 files belonging to Sam Torode"""
    tracker = get_tracker()
    
    # The 6 UUIDs that belong to Sam Torode (Row 502) from the log
    sam_uuids_mapping = {
        "b9fded0f-f2d3-467e-8be4-9867a61663d4": "youtube_M36f9CGC0QY.mp3",
        "6cee39a1-ab8c-4921-8bcb-fe47f4cce2ff": "youtube_q2QMw4nGV0A.mp3", 
        "4b2a3ab0-f60c-4985-a8cf-1ee97c23cae0": "youtube_jgmL98lDNDU.mp3",
        "518893f0-0956-4505-b4d2-a9d7d299125d": "youtube_sV5oH7itRyo.mp3",
        "cf45ce2c-c381-486d-b459-9bae320ca271": "youtube_7cufMri1c5o.mp3",
        "f099bd02-e19c-4904-a1c0-26332ea8033a": "youtube_cfZmeDJ7Rls.mp3"
    }
    
    sam_uuids = list(sam_uuids_mapping.keys())
    
    try:
        # Load current CSV
        df = pd.read_csv('outputs/output.csv')
        
        # Check if Row 502 exists in current CSV
        sam_row = df[df['row_id'] == 502]
        
        if sam_row.empty:
            print("❌ Row 502 (Sam Torode) not found in current CSV")
            return False
        
        sam_info = sam_row.iloc[0]
        print(f"✅ Found Sam Torode in CSV:")
        print(f"   Row ID: {sam_info['row_id']}")
        print(f"   Name: {sam_info['name']}")
        print(f"   Email: {sam_info.get('email', 'N/A')}")
        print(f"   Type: {sam_info.get('type', 'N/A')}")
        
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
        
        # Find which Sam UUIDs exist in orphaned files
        found_sam_files = []
        missing_sam_files = []
        
        for uuid in sam_uuids:
            if uuid in existing_orphaned:
                found_sam_files.append({
                    'uuid': uuid,
                    'original_filename': sam_uuids_mapping[uuid],
                    'file_info': orphaned_files_info[uuid]
                })
            else:
                missing_sam_files.append(uuid)
        
        print(f"\n📊 Sam Torode File Analysis:")
        print(f"   Expected files: {len(sam_uuids)}")
        print(f"   Found in orphaned: {len(found_sam_files)}")
        print(f"   Missing/Not orphaned: {len(missing_sam_files)}")
        
        if found_sam_files:
            print(f"\n✅ Found Sam Torode files in orphaned directory:")
            total_size = 0
            for file_info in found_sam_files:
                size_mb = file_info['file_info']['size'] / (1024 * 1024)
                total_size += file_info['file_info']['size']
                print(f"   - {file_info['uuid']} → {file_info['original_filename']} ({size_mb:.1f} MB)")
            
            total_size_mb = total_size / (1024 * 1024)
            print(f"   Total size: {total_size_mb:.1f} MB")
            
            # Update CSV with UUID mappings for Sam
            print(f"\n🔄 Updating CSV with Sam Torode UUID mappings...")
            
            # Get current UUID data for Sam
            current_file_uuids = sam_info.get('file_uuids', '{}')
            current_youtube_uuids = sam_info.get('youtube_uuids', '[]')
            current_drive_uuids = sam_info.get('drive_uuids', '[]')
            current_s3_paths = sam_info.get('s3_paths', '{}')
            
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
            for file_info in found_sam_files:
                uuid = file_info['uuid']
                s3_key = file_info['file_info']['key']
                original_filename = file_info['original_filename']
                
                # Add to file_uuids mapping using original filename
                file_uuids[original_filename] = uuid
                
                # Add to s3_paths mapping
                s3_paths[uuid] = s3_key
                
                # All Sam's files are YouTube videos
                if uuid not in youtube_uuids:
                    youtube_uuids.append(uuid)
            
            # Update the CSV row
            df.loc[df['row_id'] == 502, 'file_uuids'] = json.dumps(file_uuids)
            df.loc[df['row_id'] == 502, 'youtube_uuids'] = json.dumps(youtube_uuids)
            df.loc[df['row_id'] == 502, 'drive_uuids'] = json.dumps(drive_uuids)
            df.loc[df['row_id'] == 502, 's3_paths'] = json.dumps(s3_paths)
            
            # Save updated CSV
            df.to_csv('outputs/output.csv', index=False)
            
            print(f"✅ Updated Sam Torode (Row 502) with {len(found_sam_files)} recovered UUIDs")
            print(f"   File UUIDs: {len(file_uuids)} total")
            print(f"   YouTube UUIDs: {len(youtube_uuids)}")
            print(f"   Drive UUIDs: {len(drive_uuids)}")
            print(f"   S3 Paths: {len(s3_paths)}")
            
            # Move files from orphaned directory to client directory
            print(f"\n📦 Moving files to client directory...")
            
            moved_files = []
            for file_info in found_sam_files:
                old_key = file_info['file_info']['key']
                original_filename = file_info['original_filename']
                new_key = f"clients/502/{original_filename}"
                
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
                        'uuid': file_info['uuid'],
                        'original_filename': original_filename
                    })
                    
                    print(f"   ✅ Moved {original_filename}")
                    
                except Exception as e:
                    print(f"   ❌ Failed to move {original_filename}: {str(e)}")
            
            print(f"\n🎉 RECOVERY COMPLETE!")
            print(f"   Files recovered: {len(moved_files)}")
            print(f"   Client: Sam Torode (Row 502)")
            print(f"   Total size: {total_size_mb:.1f} MB")
            
            # Update S3 paths in CSV to reflect new location
            updated_s3_paths = {}
            for move_info in moved_files:
                updated_s3_paths[move_info['uuid']] = move_info['new_key']
            
            # Merge with existing s3_paths
            s3_paths.update(updated_s3_paths)
            df.loc[df['row_id'] == 502, 's3_paths'] = json.dumps(s3_paths)
            df.to_csv('outputs/output.csv', index=False)
            
            tracker.log("SUCCESS", "Sam Torode files recovered successfully", {
                'client_row_id': 502,
                'client_name': 'Sam Torode',
                'files_recovered': len(moved_files),
                'total_size_mb': total_size_mb
            })
            
            return {
                'success': True,
                'files_recovered': len(moved_files),
                'client_info': {
                    'row_id': 502,
                    'name': 'Sam Torode'
                },
                'moved_files': moved_files
            }
        
        else:
            print(f"\n❌ No Sam Torode files found in orphaned directory")
            return {'success': False, 'reason': 'no_files_found'}
        
        if missing_sam_files:
            print(f"\n⚠️  Missing Sam Torode files (not in orphaned directory):")
            for uuid in missing_sam_files:
                print(f"   - {uuid} → {sam_uuids_mapping[uuid]}")
    
    except Exception as e:
        tracker.add_error('sam_recovery', {
            'error_type': 'recovery_error',
            'error_message': str(e)
        })
        raise

if __name__ == "__main__":
    print("🔍 RECOVERING SAM TORODE FILES")
    print("=" * 60)
    
    result = recover_sam_torode_files()
    
    if result.get('success'):
        print(f"\n✅ SUCCESS: Recovered {result['files_recovered']} files for {result['client_info']['name']}")
    else:
        print(f"\n❌ FAILED: {result.get('reason', 'Unknown error')}")