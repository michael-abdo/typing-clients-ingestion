#!/usr/bin/env python3
"""
Generate Client-File Mapping Report
Creates a comprehensive mapping between clients, their types, and all associated files in S3
"""

import boto3
import pandas as pd
import json
from datetime import datetime
from pathlib import Path
from collections import defaultdict

def generate_client_file_mapping():
    """Generate a comprehensive client-to-file mapping report"""
    print("=== Generating Client-File Mapping Report ===\n")
    
    # Load the CSV to get client information
    csv_path = "outputs/output.csv"
    df = pd.read_csv(csv_path)
    print(f"📊 Loaded {len(df)} clients from {csv_path}")
    
    # Initialize S3 client
    s3 = boto3.client('s3')
    bucket_name = 'typing-clients-uuid-system'
    
    # Create mapping structure
    client_mapping = {}
    
    # Process each client
    print(f"\n🔍 Scanning S3 bucket for client files...")
    
    # List all objects in the bucket
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket_name)
    
    file_count = 0
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                
                # Skip csv-versions folder
                if key.startswith('csv-versions/'):
                    continue
                
                # Extract row_id from the path
                parts = key.split('/')
                if len(parts) >= 2:
                    try:
                        row_id = int(parts[0])
                        
                        # Initialize client entry if not exists
                        if row_id not in client_mapping:
                            # Get client info from CSV
                            client_row = df[df['row_id'] == row_id]
                            if not client_row.empty:
                                client = client_row.iloc[0]
                                client_mapping[row_id] = {
                                    'row_id': row_id,
                                    'name': client['name'],
                                    'email': client['email'],
                                    'type': client['type'],
                                    'doc_link': client.get('link', ''),
                                    'files': []
                                }
                        
                        # Add file info
                        if row_id in client_mapping:
                            file_info = {
                                's3_key': key,
                                'filename': parts[-1] if len(parts) > 1 else key,
                                'size': obj['Size'],
                                'last_modified': obj['LastModified'].isoformat(),
                                'file_type': determine_file_type(parts[-1] if len(parts) > 1 else key)
                            }
                            client_mapping[row_id]['files'].append(file_info)
                            file_count += 1
                    except (ValueError, IndexError):
                        # Skip files that don't follow the expected pattern
                        pass
    
    print(f"✅ Found {file_count} files for {len(client_mapping)} clients")
    
    # Generate reports
    generate_summary_report(client_mapping)
    generate_detailed_json_report(client_mapping)
    generate_csv_mapping_report(client_mapping)
    
    return client_mapping

def determine_file_type(filename):
    """Determine the type of file based on filename"""
    filename_lower = filename.lower()
    
    if 'youtube' in filename_lower or filename_lower.endswith(('.mp4', '.webm', '.m4a')):
        return 'YouTube'
    elif 'drive' in filename_lower or filename_lower.endswith(('.mkv', '.mov', '.avi')):
        return 'Google Drive'
    elif filename_lower.endswith('.json'):
        return 'Metadata'
    else:
        return 'Other'

def generate_summary_report(client_mapping):
    """Generate a summary report"""
    print("\n📊 SUMMARY REPORT")
    print("=" * 60)
    
    # Count by personality type
    type_counts = defaultdict(lambda: {'clients': 0, 'files': 0, 'size': 0})
    
    for row_id, client in client_mapping.items():
        client_type = client['type']
        type_counts[client_type]['clients'] += 1
        type_counts[client_type]['files'] += len(client['files'])
        type_counts[client_type]['size'] += sum(f['size'] for f in client['files'])
    
    # Display by type
    print("\n📈 Files by Personality Type:")
    for ptype, counts in sorted(type_counts.items()):
        size_mb = counts['size'] / (1024 * 1024)
        print(f"  {ptype}:")
        print(f"    - Clients: {counts['clients']}")
        print(f"    - Files: {counts['files']}")
        print(f"    - Total Size: {size_mb:.1f} MB")
    
    # Top clients by file count
    print("\n👥 Top Clients by File Count:")
    sorted_clients = sorted(client_mapping.items(), 
                          key=lambda x: len(x[1]['files']), 
                          reverse=True)[:10]
    
    for row_id, client in sorted_clients:
        file_count = len(client['files'])
        if file_count > 0:
            print(f"  {client['name']} (Row {row_id}, Type: {client['type']}): {file_count} files")
    
    # File type distribution
    print("\n📁 File Type Distribution:")
    file_types = defaultdict(int)
    for client in client_mapping.values():
        for file in client['files']:
            file_types[file['file_type']] += 1
    
    for ftype, count in sorted(file_types.items(), key=lambda x: x[1], reverse=True):
        print(f"  {ftype}: {count} files")

def generate_detailed_json_report(client_mapping):
    """Generate detailed JSON report"""
    report_path = f"client_file_mapping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    
    # Convert to list format for better readability
    report_data = {
        'generated_at': datetime.now().isoformat(),
        'total_clients': len(client_mapping),
        'total_files': sum(len(c['files']) for c in client_mapping.values()),
        'clients': list(client_mapping.values())
    }
    
    with open(report_path, 'w') as f:
        json.dump(report_data, f, indent=2, default=str)
    
    print(f"\n💾 Detailed JSON report saved to: {report_path}")

def generate_csv_mapping_report(client_mapping):
    """Generate CSV mapping report for easy viewing"""
    report_path = f"client_file_mapping_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
    
    # Flatten the data for CSV
    rows = []
    for row_id, client in client_mapping.items():
        if client['files']:
            for file in client['files']:
                rows.append({
                    'row_id': client['row_id'],
                    'name': client['name'],
                    'type': client['type'],
                    'email': client['email'],
                    'filename': file['filename'],
                    's3_key': file['s3_key'],
                    'file_type': file['file_type'],
                    'size_mb': round(file['size'] / (1024 * 1024), 2),
                    'last_modified': file['last_modified']
                })
        else:
            # Include clients with no files
            rows.append({
                'row_id': client['row_id'],
                'name': client['name'],
                'type': client['type'],
                'email': client['email'],
                'filename': 'NO FILES',
                's3_key': '',
                'file_type': '',
                'size_mb': 0,
                'last_modified': ''
            })
    
    # Create DataFrame and save
    mapping_df = pd.DataFrame(rows)
    mapping_df.to_csv(report_path, index=False)
    
    print(f"📄 CSV mapping report saved to: {report_path}")
    
    # Show sample
    print("\n📋 Sample mappings:")
    print(mapping_df.head(10).to_string(index=False))

if __name__ == "__main__":
    generate_client_file_mapping()