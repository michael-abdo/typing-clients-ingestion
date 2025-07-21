#!/usr/bin/env python3
"""
Lookup Client Files - Quick lookup tool to find files for a specific client
"""

import boto3
import pandas as pd
import argparse
from pathlib import Path

def lookup_client_files(search_term):
    """Look up files for a specific client by row_id or name"""
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
    
    # Initialize S3
    s3 = boto3.client('s3')
    bucket_name = 'typing-clients-uuid-system'
    
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
        print(f"\n📁 Files in S3:")
        
        # List files for this client
        prefix = f"{row_id}/"
        
        try:
            response = s3.list_objects_v2(
                Bucket=bucket_name,
                Prefix=prefix
            )
            
            if 'Contents' in response:
                total_size = 0
                file_count = 0
                
                for obj in response['Contents']:
                    key = obj['Key']
                    size = obj['Size']
                    last_modified = obj['LastModified']
                    
                    # Skip if it's just the directory
                    if key == prefix:
                        continue
                    
                    # Extract filename
                    filename = key.split('/')[-1]
                    size_mb = size / (1024 * 1024)
                    
                    print(f"   - {filename}")
                    print(f"     Size: {size_mb:.2f} MB")
                    print(f"     S3 Path: {key}")
                    print(f"     Modified: {last_modified}")
                    print(f"     URL: https://{bucket_name}.s3.amazonaws.com/{key}")
                    print()
                    
                    total_size += size
                    file_count += 1
                
                total_size_mb = total_size / (1024 * 1024)
                print(f"📊 Summary: {file_count} files, {total_size_mb:.2f} MB total")
            else:
                print("   ⚠️  No files found in S3 for this client")
        
        except Exception as e:
            print(f"   ❌ Error accessing S3: {str(e)}")
        
        print("\n" + "="*60 + "\n")

def list_clients_with_files():
    """List all clients that have files in S3"""
    print("📋 Listing all clients with files in S3...\n")
    
    # Load CSV
    csv_path = "outputs/output.csv"
    df = pd.read_csv(csv_path)
    
    # Create row_id to name mapping
    id_to_info = {
        row['row_id']: {
            'name': row['name'],
            'type': row['type']
        }
        for _, row in df.iterrows()
    }
    
    # Initialize S3
    s3 = boto3.client('s3')
    bucket_name = 'typing-clients-uuid-system'
    
    # Get all unique row_ids from S3
    clients_with_files = set()
    
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name)
    
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                
                # Skip csv-versions
                if key.startswith('csv-versions/'):
                    continue
                
                # Extract row_id
                parts = key.split('/')
                if len(parts) >= 2:
                    try:
                        row_id = int(parts[0])
                        clients_with_files.add(row_id)
                    except ValueError:
                        pass
    
    # Display sorted list
    print(f"Found {len(clients_with_files)} clients with files:\n")
    
    for row_id in sorted(clients_with_files):
        if row_id in id_to_info:
            info = id_to_info[row_id]
            print(f"  {row_id}: {info['name']} (Type: {info['type']})")
        else:
            print(f"  {row_id}: [Unknown client]")

def main():
    parser = argparse.ArgumentParser(description='Look up files for a specific client')
    parser.add_argument('search', nargs='?', help='Client row_id or name to search for')
    parser.add_argument('--list', action='store_true', help='List all clients with files')
    
    args = parser.parse_args()
    
    if args.list:
        list_clients_with_files()
    elif args.search:
        lookup_client_files(args.search)
    else:
        parser.print_help()

if __name__ == "__main__":
    main()