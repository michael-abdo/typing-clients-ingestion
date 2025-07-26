#!/usr/bin/env python3
"""Execute recovery of the 9 high-confidence JSON mappings and find companion files"""

import boto3
import pandas as pd
import json
from orphaned_file_recovery_tracker import get_tracker

def find_companion_files_for_json(json_uuids):
    """Find companion media files that share UUIDs with JSON files"""
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    
    print("🔍 Looking for companion media files...")
    
    # Get all orphaned files
    response = s3.list_objects_v2(Bucket=bucket, Prefix="files/")
    all_files = {}
    
    if 'Contents' in response:
        for obj in response['Contents']:
            if not obj['Key'].endswith('/'):
                filename = obj['Key'].split('/')[-1]
                uuid = filename.split('.')[0]
                extension = filename.split('.')[-1] if '.' in filename else 'no_ext'
                
                all_files[uuid] = {
                    'filename': filename,
                    'extension': extension,
                    's3_key': obj['Key'],
                    'size': obj['Size'],
                    'size_mb': obj['Size'] / (1024 * 1024)
                }
    
    # Find companion files for each JSON UUID
    companion_groups = {}
    
    for json_uuid in json_uuids:
        companions = []
        
        # Check if there are media files with the same UUID
        if json_uuid in all_files:
            json_file = all_files[json_uuid]
            if json_file['extension'] == 'json':
                # Look for other extensions with same UUID
                for uuid, file_info in all_files.items():
                    if uuid == json_uuid and file_info['extension'] != 'json':
                        companions.append(file_info)
                
                if companions:
                    companion_groups[json_uuid] = {
                        'json_file': json_file,
                        'companions': companions,
                        'total_files': 1 + len(companions),
                        'total_size_mb': json_file['size_mb'] + sum(c['size_mb'] for c in companions)
                    }
                    
                    print(f"   ✅ {json_uuid}: JSON + {len(companions)} media files ({companion_groups[json_uuid]['total_size_mb']:.1f} MB)")
                    for companion in companions:
                        print(f"      - {companion['filename']} ({companion['size_mb']:.1f} MB)")
    
    print(f"\n📊 Companion File Results:")
    print(f"   JSON files with companions: {len(companion_groups)}")
    total_files = sum(group['total_files'] for group in companion_groups.values())
    total_size = sum(group['total_size_mb'] for group in companion_groups.values())
    print(f"   Total files to recover: {total_files}")
    print(f"   Total size: {total_size:.1f} MB")
    
    return companion_groups

def execute_recovery_with_companions():
    """Execute the recovery of JSON files and their companions"""
    tracker = get_tracker()
    
    print("🚀 EXECUTING JSON + COMPANION FILE RECOVERY")
    print("=" * 50)
    
    # High-confidence mappings from previous analysis
    mappings = [
        {'uuid': '114aa117-cc70-4aec-9d01-f298ea5bc65a', 'client_name': 'Patryk Makara', 'row_id': 476},
        {'uuid': '5cb10b5d-400a-44e1-a01d-57d8c0b3fd27', 'client_name': 'Brenden Ohlsson', 'row_id': 474},
        {'uuid': '96c39a7c-f94a-487e-a6ae-62e3af484dcd', 'client_name': 'Shelsea Evans', 'row_id': 486},
        {'uuid': 'd341a3f1-f67a-463c-9005-03dd737899f8', 'client_name': 'Patryk Makara', 'row_id': 476},
        {'uuid': 'e6d2f69e-ddce-4f42-90ce-ad2588ab42f7', 'client_name': 'Caroline Chiu', 'row_id': 497},
        {'uuid': '165dfc11-9ead-49e9-9728-011455f8cfb9', 'client_name': 'Shelly Chen', 'row_id': 477},
        {'uuid': '1cba1795-604c-443a-9e47-dcedc77cac23', 'client_name': 'James Kirton', 'row_id': 496},
        {'uuid': 'd99a7053-24f2-4ef1-8ff9-4c4e0605a96d', 'client_name': 'James Kirton', 'row_id': 496},
        {'uuid': '0b50a0e5-844e-4ffc-b69b-032fe4a97481', 'client_name': 'Caroline Chiu', 'row_id': 497}
    ]
    
    json_uuids = [m['uuid'] for m in mappings]
    
    # Step 1: Find companion files
    companion_groups = find_companion_files_for_json(json_uuids)
    
    # Step 2: Execute recoveries
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    df = pd.read_csv('outputs/output.csv')
    
    successful_recoveries = []
    
    print(f"\n🎯 Executing recoveries for {len(mappings)} mappings...")
    
    for mapping in mappings:
        uuid = mapping['uuid']
        client_name = mapping['client_name']
        row_id = mapping['row_id']
        
        try:
            print(f"\n📦 Processing {uuid} → {client_name} (Row {row_id})")
            
            # Get files to move
            files_to_move = []
            
            # Always include the JSON file
            json_key = f"files/{uuid}.json"
            try:
                s3.head_object(Bucket=bucket, Key=json_key)
                files_to_move.append({
                    'old_key': json_key,
                    'filename': f"{uuid}.json",
                    'extension': 'json'
                })
            except:
                print(f"   ⚠️  JSON file not found: {json_key}")
                continue
            
            # Add companion files if they exist
            if uuid in companion_groups:
                for companion in companion_groups[uuid]['companions']:
                    files_to_move.append({
                        'old_key': companion['s3_key'],
                        'filename': companion['filename'],
                        'extension': companion['extension']
                    })
            
            print(f"   Found {len(files_to_move)} files to move")
            
            # Update CSV with UUID mappings
            client_row = df[df['row_id'] == row_id]
            if client_row.empty:
                print(f"   ❌ Client row {row_id} not found in CSV")
                continue
            
            # Get current UUID data
            current_file_uuids = client_row.iloc[0].get('file_uuids', '{}')
            current_youtube_uuids = client_row.iloc[0].get('youtube_uuids', '[]')
            current_drive_uuids = client_row.iloc[0].get('drive_uuids', '[]')
            current_s3_paths = client_row.iloc[0].get('s3_paths', '{}')
            
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
            
            # Move each file
            moved_files = []
            for file_info in files_to_move:
                old_key = file_info['old_key']
                filename = file_info['filename']
                extension = file_info['extension']
                new_key = f"clients/{row_id}/{filename}"
                
                try:
                    # Copy to new location
                    s3.copy_object(
                        CopySource={'Bucket': bucket, 'Key': old_key},
                        Bucket=bucket,
                        Key=new_key
                    )
                    
                    # Delete from old location
                    s3.delete_object(Bucket=bucket, Key=old_key)
                    
                    # Update UUID mappings
                    file_uuids[filename] = uuid
                    s3_paths[uuid] = new_key
                    
                    # Categorize by file type
                    if extension in ['mp3', 'mp4', 'webm', 'm4a']:
                        if uuid not in youtube_uuids:
                            youtube_uuids.append(uuid)
                    else:
                        if uuid not in drive_uuids:
                            drive_uuids.append(uuid)
                    
                    moved_files.append({
                        'filename': filename,
                        'old_key': old_key,
                        'new_key': new_key,
                        'extension': extension
                    })
                    
                    print(f"   ✅ Moved {filename}")
                    
                except Exception as e:
                    print(f"   ❌ Failed to move {filename}: {str(e)}")
                    continue
            
            if moved_files:
                # Update CSV
                df.loc[df['row_id'] == row_id, 'file_uuids'] = json.dumps(file_uuids)
                df.loc[df['row_id'] == row_id, 'youtube_uuids'] = json.dumps(youtube_uuids)
                df.loc[df['row_id'] == row_id, 'drive_uuids'] = json.dumps(drive_uuids)
                df.loc[df['row_id'] == row_id, 's3_paths'] = json.dumps(s3_paths)
                
                successful_recoveries.append({
                    'uuid': uuid,
                    'client_name': client_name,
                    'client_row_id': row_id,
                    'files_moved': len(moved_files),
                    'moved_files': moved_files
                })
                
                print(f"   🎉 Successfully recovered {len(moved_files)} files for {client_name}")
        
        except Exception as e:
            print(f"   ❌ Error processing {uuid}: {str(e)}")
            continue
    
    # Save updated CSV
    if successful_recoveries:
        df.to_csv('outputs/output.csv', index=False)
    
    # Report results
    print(f"\n🎊 RECOVERY COMPLETE!")
    print(f"   Clients processed: {len(set(r['client_name'] for r in successful_recoveries))}")
    print(f"   Total files recovered: {sum(r['files_moved'] for r in successful_recoveries)}")
    
    # Group by client
    by_client = {}
    for recovery in successful_recoveries:
        client = recovery['client_name']
        if client not in by_client:
            by_client[client] = []
        by_client[client].append(recovery)
    
    print(f"\n📋 Recovery by Client:")
    for client, recoveries in by_client.items():
        total_files = sum(r['files_moved'] for r in recoveries)
        print(f"   {client}: {total_files} files from {len(recoveries)} UUIDs")
    
    tracker.session_data['json_recovery'] = {
        'successful_recoveries': successful_recoveries,
        'total_files_recovered': sum(r['files_moved'] for r in successful_recoveries),
        'clients_recovered': len(by_client)
    }
    
    return successful_recoveries

if __name__ == "__main__":
    recoveries = execute_recovery_with_companions()
    
    if recoveries:
        total_files = sum(r['files_moved'] for r in recoveries)
        print(f"\n✅ SUCCESS: Recovered {total_files} files for {len(set(r['client_name'] for r in recoveries))} clients!")
    else:
        print(f"\n❌ No files were successfully recovered")