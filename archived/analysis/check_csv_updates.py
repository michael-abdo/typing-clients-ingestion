#!/usr/bin/env python3
"""Check CSV for all recovered file mappings"""

import pandas as pd
import json

def check_csv_recovery_status():
    """Check the current state of recovered files in CSV"""
    df = pd.read_csv('outputs/output.csv')
    
    print("🔍 CHECKING CSV RECOVERY STATUS")
    print("=" * 50)
    
    # Count clients with recovered files
    clients_with_files = 0
    total_files_tracked = 0
    recovery_details = []
    
    for _, row in df.iterrows():
        file_uuids = row.get('file_uuids', '{}')
        youtube_uuids = row.get('youtube_uuids', '[]')
        drive_uuids = row.get('drive_uuids', '[]')
        s3_paths = row.get('s3_paths', '{}')
        
        try:
            file_uuids_dict = json.loads(str(file_uuids)) if file_uuids != '{}' else {}
            youtube_list = json.loads(str(youtube_uuids)) if youtube_uuids != '[]' else []
            drive_list = json.loads(str(drive_uuids)) if drive_uuids != '[]' else []
            s3_paths_dict = json.loads(str(s3_paths)) if s3_paths != '{}' else {}
            
            total_client_files = len(file_uuids_dict)
            total_client_uuids = len(youtube_list) + len(drive_list)
            total_s3_paths = len(s3_paths_dict)
            
            if total_client_files > 0 or total_client_uuids > 0:
                clients_with_files += 1
                total_files_tracked += total_client_files
                
                recovery_details.append({
                    'row_id': row['row_id'],
                    'name': row['name'],
                    'files': total_client_files,
                    'youtube_uuids': len(youtube_list),
                    'drive_uuids': len(drive_list),
                    's3_paths': total_s3_paths
                })
                
                print(f"Row {row['row_id']}: {row['name']} - {total_client_files} files, {len(youtube_list)} YouTube UUIDs, {len(drive_list)} Drive UUIDs")
        except Exception as e:
            pass
    
    print(f"\n📊 RECOVERY SUMMARY:")
    print(f"   Clients with recovered files: {clients_with_files}")
    print(f"   Total files tracked in CSV: {total_files_tracked}")
    
    # Group by recovery method based on what we know
    print(f"\n📋 RECOVERY BREAKDOWN:")
    
    # Kaioxys (Row 472) - should have 11 files
    kaioxys = next((d for d in recovery_details if d['row_id'] == 472), None)
    if kaioxys:
        print(f"   Kaioxys DarkMacro (Row 472): {kaioxys['files']} files - Historical log method")
    
    # JSON name matching clients (from previous session)
    json_clients = [476, 474, 486, 497, 477, 496]  # Patryk, Brenden, Shelsea, Caroline, Shelly, James
    json_recovered = [d for d in recovery_details if d['row_id'] in json_clients]
    if json_recovered:
        print(f"   JSON name matching: {len(json_recovered)} clients, {sum(d['files'] for d in json_recovered)} files")
        for client in json_recovered:
            print(f"     - {client['name']} (Row {client['row_id']}): {client['files']} files")
    
    # Structured row_id clients (from this session)
    structured_clients = [483, 484, 492]  # Taro, Emilie, Kiko
    structured_recovered = [d for d in recovery_details if d['row_id'] in structured_clients]
    if structured_recovered:
        print(f"   Structured row_id method: {len(structured_recovered)} clients, {sum(d['files'] for d in structured_recovered)} files")
        for client in structured_recovered:
            print(f"     - {client['name']} (Row {client['row_id']}): {client['files']} files")
    
    return {
        'total_clients': clients_with_files,
        'total_files': total_files_tracked,
        'details': recovery_details
    }

if __name__ == "__main__":
    results = check_csv_recovery_status()