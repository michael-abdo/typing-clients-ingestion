#!/usr/bin/env python3
"""
Check all S3 buckets for files from clients 502-506.
"""

import boto3
from collections import defaultdict
from datetime import datetime

def check_bucket(bucket_name, client_ids):
    """Check a single bucket for client files."""
    s3 = boto3.client('s3')
    found_files = defaultdict(list)
    
    try:
        for client_id in client_ids:
            paginator = s3.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(
                Bucket=bucket_name,
                Prefix=f"{client_id}_"
            )
            
            for page in page_iterator:
                if 'Contents' in page:
                    for obj in page['Contents']:
                        found_files[client_id].append({
                            'bucket': bucket_name,
                            'key': obj['Key'],
                            'size': obj['Size'],
                            'last_modified': obj['LastModified']
                        })
    except Exception as e:
        print(f"  Error accessing bucket {bucket_name}: {e}")
        return found_files
    
    return found_files

def format_size(size_bytes):
    """Format size in human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

def main():
    """Main function."""
    client_ids = ['502', '503', '504', '505', '506']
    buckets = [
        'typing-clients-uuid-system',
        'typing-clients-storage-2025',
        'xenodex-client-output-new',
        'xenodex'  # Just in case
    ]
    
    print("S3 Bucket Scanner for Clients 502-506")
    print("=" * 80)
    print(f"Checking buckets: {', '.join(buckets)}")
    print(f"Looking for clients: {', '.join(client_ids)}")
    print()
    
    all_files = defaultdict(list)
    
    for bucket in buckets:
        print(f"\nChecking bucket: {bucket}")
        print("-" * 40)
        
        found = check_bucket(bucket, client_ids)
        
        if any(found.values()):
            for client_id, files in found.items():
                if files:
                    print(f"\nClient {client_id}: {len(files)} files")
                    for f in files:
                        print(f"  • {f['key']} ({format_size(f['size'])})")
                        all_files[client_id].append(f)
        else:
            print("  No files found for any client")
    
    # Summary
    print("\n\nSUMMARY")
    print("=" * 80)
    
    for client_id in client_ids:
        files = all_files.get(client_id, [])
        if files:
            total_size = sum(f['size'] for f in files)
            print(f"\nClient {client_id}:")
            print(f"  Total files: {len(files)}")
            print(f"  Total size: {format_size(total_size)}")
            
            # Group by bucket
            by_bucket = defaultdict(int)
            for f in files:
                by_bucket[f['bucket']] += 1
            
            print("  Distribution:")
            for bucket, count in by_bucket.items():
                print(f"    - {bucket}: {count} files")
        else:
            print(f"\nClient {client_id}: NO FILES FOUND IN ANY BUCKET")
    
    # Save results
    import json
    results = {
        'timestamp': datetime.now().isoformat(),
        'buckets_checked': buckets,
        'clients_checked': client_ids,
        'files_found': dict(all_files)
    }
    
    with open('s3_bucket_scan_results.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n\nDetailed results saved to: s3_bucket_scan_results.json")

if __name__ == "__main__":
    main()