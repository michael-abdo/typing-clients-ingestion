#!/usr/bin/env python3
"""Execute the 7 high-confidence structured row_id mappings"""

import boto3
import pandas as pd
import json
from orphaned_file_recovery_tracker import get_tracker

def execute_structured_row_id_mappings():
    """Execute the 7 structured mappings found through row_id analysis"""
    tracker = get_tracker()
    
    print("🚀 EXECUTING STRUCTURED ROW_ID MAPPINGS")
    print("=" * 50)
    
    # High-confidence mappings from investigation
    mappings = [
        {'uuid': '0d2b0e2b-2a12-4b91-ac7e-1f9a8e1d25eb', 'client_name': 'Taro', 'row_id': 483, 'confidence': 0.85},
        {'uuid': '18a6ee85-cda9-48c6-bf94-633df1ccda5d', 'client_name': 'Taro', 'row_id': 483, 'confidence': 0.85},
        {'uuid': '439e5ee1-6c94-4a3a-885d-ceec28c871ec', 'client_name': 'Emilie', 'row_id': 484, 'confidence': 0.85},
        {'uuid': '5eebcbf6-6654-4744-a075-b1d6d402801a', 'client_name': 'Kiko', 'row_id': 492, 'confidence': 0.85},
        {'uuid': '882d41b0-21d7-40ed-a8f1-10b3464fc977', 'client_name': 'Kiko', 'row_id': 492, 'confidence': 0.85},
        {'uuid': 'c92ee039-d7e2-4807-9e8f-65fa644bf12f', 'client_name': 'Kiko', 'row_id': 492, 'confidence': 0.85},
        {'uuid': 'df0da9ee-da7c-4e18-b871-c0d05fbec66d', 'client_name': 'Taro', 'row_id': 483, 'confidence': 0.85}
    ]
    
    print(f"📦 Executing {len(mappings)} structured mappings:")
    
    # Group by client for summary
    by_client = {}
    for mapping in mappings:
        client = mapping['client_name']
        if client not in by_client:
            by_client[client] = []
        by_client[client].append(mapping)
    
    for client, client_mappings in by_client.items():
        print(f"   {client} (Row {client_mappings[0]['row_id']}): {len(client_mappings)} files")
    
    # Execute recoveries
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    df = pd.read_csv('outputs/output.csv')
    
    successful_recoveries = []
    
    for mapping in mappings:
        uuid = mapping['uuid']
        client_name = mapping['client_name']
        row_id = mapping['row_id']
        
        try:
            print(f"\n📦 Processing {uuid} → {client_name} (Row {row_id})")
            
            # Find all files with this UUID (likely just JSON but check for others)
            response = s3.list_objects_v2(Bucket=bucket, Prefix=f"files/{uuid}")
            files_to_move = []
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    if not obj['Key'].endswith('/'):
                        filename = obj['Key'].split('/')[-1]
                        extension = filename.split('.')[-1] if '.' in filename else 'no_ext'
                        
                        files_to_move.append({
                            'old_key': obj['Key'],
                            'filename': filename,
                            'extension': extension,
                            'size_mb': obj['Size'] / (1024 * 1024)
                        })
            
            if not files_to_move:
                print(f"   ❌ No files found for UUID {uuid}")
                continue
            
            print(f"   Found {len(files_to_move)} files to move:")
            for file_info in files_to_move:
                print(f"     - {file_info['filename']} ({file_info['size_mb']:.3f} MB)")
            
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
            
            # Move files and update mappings
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
                    
                    # Categorize by file type (JSON files go to drive_uuids)
                    if extension in ['mp3', 'mp4', 'webm', 'm4a']:
                        if uuid not in youtube_uuids:
                            youtube_uuids.append(uuid)
                    else:
                        if uuid not in drive_uuids:
                            drive_uuids.append(uuid)
                    
                    moved_files.append(file_info)
                    print(f"     ✅ Moved {filename}")
                    
                except Exception as e:
                    print(f"     ❌ Failed to move {filename}: {str(e)}")
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
                    'total_size_mb': sum(f['size_mb'] for f in moved_files),
                    'method': 'structured_row_id',
                    'confidence': mapping['confidence']
                })
                
                print(f"   🎉 Successfully recovered {len(moved_files)} files!")
        
        except Exception as e:
            print(f"   ❌ Error executing mapping for {uuid}: {str(e)}")
            continue
    
    # Save updated CSV
    if successful_recoveries:
        df.to_csv('outputs/output.csv', index=False)
        
        print(f"\n🎊 STRUCTURED MAPPING EXECUTION COMPLETE!")
        print(f"   Total recoveries: {len(successful_recoveries)}")
        print(f"   Total files moved: {sum(r['files_moved'] for r in successful_recoveries)}")
        total_size = sum(r['total_size_mb'] for r in successful_recoveries)
        print(f"   Total size recovered: {total_size:.3f} MB")
        
        # Group by client for final summary
        final_by_client = {}
        for recovery in successful_recoveries:
            client = recovery['client_name']
            if client not in final_by_client:
                final_by_client[client] = []
            final_by_client[client].append(recovery)
        
        print(f"\n📋 Recovery Summary by Client:")
        for client, recoveries in final_by_client.items():
            total_files = sum(r['files_moved'] for r in recoveries)
            client_row_id = recoveries[0]['client_row_id']
            print(f"   {client} (Row {client_row_id}): {total_files} files from {len(recoveries)} UUIDs")
        
        # Save to tracker
        tracker.session_data['structured_row_id_execution'] = {
            'successful_recoveries': successful_recoveries,
            'total_files_recovered': sum(r['files_moved'] for r in successful_recoveries),
            'clients_recovered': len(final_by_client),
            'total_size_mb': total_size
        }
    
    return successful_recoveries

def main():
    """Main execution function"""
    print("🎯 STRUCTURED ROW_ID MAPPING EXECUTION")
    print("=" * 60)
    
    recoveries = execute_structured_row_id_mappings()
    
    if recoveries:
        total_files = sum(r['files_moved'] for r in recoveries)
        clients = len(set(r['client_name'] for r in recoveries))
        print(f"\n✅ SUCCESS: Recovered {total_files} files for {clients} more clients!")
        print(f"🏆 Method: Structured row_id analysis - 85% confidence mappings")
    else:
        print(f"\n❌ No files were successfully recovered")
    
    return recoveries

if __name__ == "__main__":
    recoveries = main()