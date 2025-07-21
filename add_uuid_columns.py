#!/usr/bin/env python3
"""
Add UUID Columns to CSV - Implements UUID tracking for files
"""

import pandas as pd
import json
import uuid
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional

def add_uuid_columns_to_csv(csv_path: str = "outputs/output.csv"):
    """Add UUID tracking columns to the CSV"""
    print("=== Adding UUID Columns to CSV ===\n")
    
    # Load existing CSV
    df = pd.read_csv(csv_path)
    print(f"📊 Loaded {len(df)} rows from {csv_path}")
    
    # Define new columns if they don't exist
    new_columns = {
        'file_uuids': '{}',          # JSON object: {filename: uuid}
        'youtube_uuids': '[]',       # JSON array of YouTube UUIDs
        'drive_uuids': '[]',         # JSON array of Drive UUIDs  
        's3_paths': '{}'             # JSON object: {uuid: s3_path}
    }
    
    # Add columns if they don't exist
    added_columns = []
    for col, default_val in new_columns.items():
        if col not in df.columns:
            df[col] = default_val
            added_columns.append(col)
    
    if added_columns:
        print(f"✅ Added columns: {', '.join(added_columns)}")
        
        # Save updated CSV
        df.to_csv(csv_path, index=False)
        print(f"💾 Saved updated CSV with UUID columns")
    else:
        print("ℹ️  All UUID columns already exist")
    
    return df

def generate_uuids_for_existing_files(csv_path: str = "outputs/output.csv"):
    """Generate UUIDs for existing files in S3"""
    import boto3
    
    print("\n=== Generating UUIDs for Existing Files ===\n")
    
    # Load CSV with UUID columns
    df = add_uuid_columns_to_csv(csv_path)
    
    # Initialize S3 client
    s3 = boto3.client('s3')
    bucket_name = 'typing-clients-uuid-system'
    
    # Process each row
    updates_made = 0
    
    for idx, row in df.iterrows():
        row_id = row['row_id']
        
        # Skip if already has UUIDs
        file_uuids = json.loads(row.get('file_uuids', '{}'))
        if file_uuids:
            continue
        
        # List files for this client in S3
        prefix = f"{row_id}/"
        try:
            response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            
            if 'Contents' in response:
                youtube_uuids = []
                drive_uuids = []
                s3_paths = {}
                
                for obj in response['Contents']:
                    s3_key = obj['Key']
                    if s3_key == prefix:  # Skip directory
                        continue
                    
                    # Extract filename
                    filename = s3_key.split('/')[-1]
                    
                    # Generate UUID for this file
                    file_uuid = str(uuid.uuid4())
                    
                    # Store mappings
                    file_uuids[filename] = file_uuid
                    s3_paths[file_uuid] = s3_key
                    
                    # Categorize by type
                    if 'youtube' in filename.lower():
                        youtube_uuids.append(file_uuid)
                    elif 'drive' in filename.lower():
                        drive_uuids.append(file_uuid)
                
                # Update DataFrame
                df.at[idx, 'file_uuids'] = json.dumps(file_uuids)
                df.at[idx, 'youtube_uuids'] = json.dumps(youtube_uuids)
                df.at[idx, 'drive_uuids'] = json.dumps(drive_uuids)
                df.at[idx, 's3_paths'] = json.dumps(s3_paths)
                
                updates_made += 1
                print(f"✅ Generated UUIDs for {row['name']} (Row {row_id}): {len(file_uuids)} files")
        
        except Exception as e:
            print(f"⚠️  Error processing row {row_id}: {str(e)}")
    
    if updates_made > 0:
        # Save updated CSV
        df.to_csv(csv_path, index=False)
        print(f"\n💾 Updated {updates_made} rows with UUIDs")
        print(f"🎉 UUID tracking is now active!")
    else:
        print("\nℹ️  No updates needed - all files already have UUIDs")
    
    return df

def lookup_file_by_uuid(file_uuid: str, csv_path: str = "outputs/output.csv"):
    """Look up file information by UUID"""
    df = pd.read_csv(csv_path)
    
    for _, row in df.iterrows():
        # Check file_uuids
        file_uuids = json.loads(row.get('file_uuids', '{}'))
        for filename, uuid_val in file_uuids.items():
            if uuid_val == file_uuid:
                s3_paths = json.loads(row.get('s3_paths', '{}'))
                s3_path = s3_paths.get(file_uuid, '')
                
                return {
                    'found': True,
                    'row_id': row['row_id'],
                    'client_name': row['name'],
                    'client_type': row['type'],
                    'filename': filename,
                    's3_path': s3_path,
                    'uuid': file_uuid
                }
    
    return {'found': False}

def display_uuid_summary(csv_path: str = "outputs/output.csv"):
    """Display summary of UUID tracking"""
    print("\n=== UUID Tracking Summary ===\n")
    
    df = pd.read_csv(csv_path)
    
    # Count rows with UUIDs
    rows_with_uuids = 0
    total_file_uuids = 0
    total_youtube_uuids = 0
    total_drive_uuids = 0
    
    for _, row in df.iterrows():
        file_uuids = json.loads(row.get('file_uuids', '{}'))
        if file_uuids:
            rows_with_uuids += 1
            total_file_uuids += len(file_uuids)
            
            youtube_uuids = json.loads(row.get('youtube_uuids', '[]'))
            drive_uuids = json.loads(row.get('drive_uuids', '[]'))
            
            total_youtube_uuids += len(youtube_uuids)
            total_drive_uuids += len(drive_uuids)
    
    print(f"📊 Statistics:")
    print(f"  - Total rows: {len(df)}")
    print(f"  - Rows with UUIDs: {rows_with_uuids}")
    print(f"  - Total file UUIDs: {total_file_uuids}")
    print(f"  - YouTube UUIDs: {total_youtube_uuids}")
    print(f"  - Drive UUIDs: {total_drive_uuids}")
    
    # Show sample UUID mapping
    if rows_with_uuids > 0:
        print("\n📋 Sample UUID Mappings:")
        sample_shown = 0
        for _, row in df.iterrows():
            file_uuids = json.loads(row.get('file_uuids', '{}'))
            if file_uuids and sample_shown < 3:
                print(f"\n  Client: {row['name']} (Row {row['row_id']})")
                for filename, uuid_val in list(file_uuids.items())[:2]:
                    print(f"    {filename}")
                    print(f"    → UUID: {uuid_val}")
                sample_shown += 1

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Add UUID tracking to CSV')
    parser.add_argument('--generate', action='store_true', 
                       help='Generate UUIDs for existing files')
    parser.add_argument('--lookup', type=str,
                       help='Look up file by UUID')
    parser.add_argument('--summary', action='store_true',
                       help='Show UUID tracking summary')
    
    args = parser.parse_args()
    
    if args.lookup:
        result = lookup_file_by_uuid(args.lookup)
        if result['found']:
            print(f"✅ Found file:")
            print(f"  Client: {result['client_name']} (Row {result['row_id']})")
            print(f"  Type: {result['client_type']}")
            print(f"  Filename: {result['filename']}")
            print(f"  S3 Path: {result['s3_path']}")
        else:
            print(f"❌ UUID not found: {args.lookup}")
    elif args.summary:
        display_uuid_summary()
    elif args.generate:
        generate_uuids_for_existing_files()
        display_uuid_summary()
    else:
        # Just add columns
        add_uuid_columns_to_csv()
        print("\n💡 Run with --generate to create UUIDs for existing files")
        print("💡 Run with --summary to see UUID tracking statistics")