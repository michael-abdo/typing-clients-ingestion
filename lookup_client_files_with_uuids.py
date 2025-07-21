#!/usr/bin/env python3
"""
Enhanced Client File Lookup - Shows files with their UUIDs
"""

import boto3
import pandas as pd
import json
import argparse
from pathlib import Path

def lookup_client_with_uuids(search_term):
    """Enhanced lookup that shows UUIDs"""
    print(f"🔍 Searching for client: {search_term}\n")
    
    # Load CSV
    csv_path = "outputs/output.csv"
    df = pd.read_csv(csv_path)
    
    # Search for client
    if search_term.isdigit():
        # Search by row_id
        client_data = df[df['row_id'] == int(search_term)]
    else:
        # Search by name (case insensitive)
        client_data = df[df['name'].str.contains(search_term, case=False, na=False)]
    
    if client_data.empty:
        print(f"❌ No client found matching: {search_term}")
        return
    
    # Process each matching client
    for _, client in client_data.iterrows():
        row_id = client['row_id']
        name = client['name']
        client_type = client['type']
        email = client['email']
        
        print(f"👤 Client Information:")
        print(f"   Row ID: {row_id}")
        print(f"   Name: {name}")
        print(f"   Type: {client_type}")
        print(f"   Email: {email}")
        
        # Get UUID mappings
        has_uuids = False
        if 'file_uuids' in df.columns and pd.notna(client.get('file_uuids')):
            try:
                file_uuids = json.loads(client['file_uuids'])
                s3_paths = json.loads(client.get('s3_paths', '{}'))
                youtube_uuids = json.loads(client.get('youtube_uuids', '[]'))
                drive_uuids = json.loads(client.get('drive_uuids', '[]'))
                
                if file_uuids:
                    has_uuids = True
                    print(f"\n📁 Files with UUIDs:")
                    
                    # Sort files by type
                    youtube_files = []
                    drive_files = []
                    other_files = []
                    
                    for filename, file_uuid in file_uuids.items():
                        file_info = {
                            'filename': filename,
                            'uuid': file_uuid,
                            's3_path': s3_paths.get(file_uuid, 'Unknown')
                        }
                        
                        if file_uuid in youtube_uuids:
                            youtube_files.append(file_info)
                        elif file_uuid in drive_uuids:
                            drive_files.append(file_info)
                        else:
                            other_files.append(file_info)
                    
                    # Display YouTube files
                    if youtube_files:
                        print("\n   🎥 YouTube Files:")
                        for file_info in youtube_files:
                            print(f"      {file_info['filename']}")
                            print(f"      UUID: {file_info['uuid']}")
                            print(f"      S3: {file_info['s3_path']}")
                            print()
                    
                    # Display Drive files
                    if drive_files:
                        print("   📁 Google Drive Files:")
                        for file_info in drive_files:
                            print(f"      {file_info['filename']}")
                            print(f"      UUID: {file_info['uuid']}")
                            print(f"      S3: {file_info['s3_path']}")
                            print()
                    
                    # Display other files
                    if other_files:
                        print("   📄 Other Files:")
                        for file_info in other_files:
                            print(f"      {file_info['filename']}")
                            print(f"      UUID: {file_info['uuid']}")
                            print(f"      S3: {file_info['s3_path']}")
                            print()
                    
                    print(f"📊 Summary: {len(file_uuids)} files with UUIDs")
                    print(f"   YouTube: {len(youtube_uuids)}")
                    print(f"   Drive: {len(drive_uuids)}")
            except Exception as e:
                print(f"⚠️  Error parsing UUID data: {str(e)}")
        
        if not has_uuids:
            print("\n⚠️  No UUID tracking for this client yet")
            print("   Run 'python3 add_uuid_columns.py --generate' to create UUIDs")
        
        print("\n" + "="*60 + "\n")

def show_uuid_statistics():
    """Show overall UUID tracking statistics"""
    print("📊 UUID Tracking Statistics\n")
    
    df = pd.read_csv("outputs/output.csv")
    
    total_clients = len(df)
    clients_with_uuids = 0
    total_files_with_uuids = 0
    
    if 'file_uuids' in df.columns:
        for _, row in df.iterrows():
            if pd.notna(row.get('file_uuids')):
                try:
                    file_uuids = json.loads(row['file_uuids'])
                    if file_uuids:
                        clients_with_uuids += 1
                        total_files_with_uuids += len(file_uuids)
                except:
                    pass
    
    print(f"Total clients: {total_clients}")
    print(f"Clients with UUID tracking: {clients_with_uuids}")
    print(f"Total files with UUIDs: {total_files_with_uuids}")
    
    if clients_with_uuids > 0:
        print(f"\nAverage files per client: {total_files_with_uuids / clients_with_uuids:.1f}")

def main():
    parser = argparse.ArgumentParser(description='Enhanced client file lookup with UUIDs')
    parser.add_argument('search', nargs='?', help='Client row_id or name to search for')
    parser.add_argument('--stats', action='store_true', help='Show UUID tracking statistics')
    
    args = parser.parse_args()
    
    if args.stats:
        show_uuid_statistics()
    elif args.search:
        lookup_client_with_uuids(args.search)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()