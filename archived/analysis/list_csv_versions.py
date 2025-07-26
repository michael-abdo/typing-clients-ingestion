#!/usr/bin/env python3
"""
List CSV Versions - View all CSV versions stored in S3
"""

import argparse
from datetime import datetime
from utils.csv_s3_versioning import get_csv_versioning, CSVS3Versioning
from utils.config import get_config

def format_size(size_bytes):
    """Format bytes to human readable format"""
    for unit in ['B', 'KB', 'MB', 'GB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} TB"

def list_csv_versions(prefix=None, limit=50):
    """List all CSV versions in S3"""
    print("=== CSV Versions in S3 ===\n")
    
    config = get_config()
    bucket = config.get('downloads.s3.default_bucket', 'typing-clients-uuid-system')
    
    versioning = get_csv_versioning()
    print(f"Bucket: {bucket}")
    print(f"Folder: csv-versions/\n")
    
    # Get versions
    versions = versioning.list_csv_versions(prefix_filter=prefix, limit=limit)
    
    if not versions:
        print("No CSV versions found.")
        return
    
    # Group by base filename
    grouped = {}
    for version in versions:
        # Extract base filename (before timestamp)
        filename = version['filename']
        # Try to extract base name before timestamp pattern
        import re
        match = re.match(r'(.+?)_\d{4}-\d{2}-\d{2}_\d{6}(\..+)?$', filename)
        if match:
            base_name = match.group(1) + (match.group(2) or '')
        else:
            base_name = filename
        
        if base_name not in grouped:
            grouped[base_name] = []
        grouped[base_name].append(version)
    
    # Display grouped versions
    for base_name, file_versions in grouped.items():
        print(f"📄 {base_name}")
        print(f"   Versions: {len(file_versions)}")
        
        # Show latest 3 versions
        for i, version in enumerate(file_versions[:3]):
            timestamp = version['last_modified']
            size = format_size(version['size'])
            print(f"   - {version['filename']}")
            print(f"     Modified: {timestamp} | Size: {size}")
        
        if len(file_versions) > 3:
            print(f"   ... and {len(file_versions) - 3} more versions")
        print()
    
    # Summary statistics
    total_versions = len(versions)
    total_size = sum(v['size'] for v in versions)
    
    print("\n📊 Summary")
    print(f"Total versions: {total_versions}")
    print(f"Total storage: {format_size(total_size)}")
    print(f"Unique files: {len(grouped)}")
    
    # Show latest overall
    if versions:
        latest = versions[0]  # Already sorted by date
        print(f"\n🕐 Most recent update:")
        print(f"   {latest['filename']}")
        print(f"   {latest['last_modified']}")

def download_csv_version(s3_key, output_path=None):
    """Download a specific CSV version from S3"""
    import boto3
    from pathlib import Path
    
    config = get_config()
    bucket = config.get('downloads.s3.default_bucket', 'typing-clients-uuid-system')
    
    if not output_path:
        # Use filename from S3 key
        output_path = Path(s3_key).name
    
    print(f"Downloading {s3_key} to {output_path}...")
    
    s3_client = boto3.client('s3')
    try:
        s3_client.download_file(bucket, s3_key, output_path)
        print(f"✅ Downloaded successfully to: {output_path}")
    except Exception as e:
        print(f"❌ Download failed: {e}")

def main():
    parser = argparse.ArgumentParser(description='List and manage CSV versions in S3')
    parser.add_argument('--prefix', help='Filter by filename prefix (e.g., "output")')
    parser.add_argument('--limit', type=int, default=50, help='Maximum versions to list (default: 50)')
    parser.add_argument('--download', help='Download a specific version by S3 key')
    parser.add_argument('--output', help='Output filename for download')
    
    args = parser.parse_args()
    
    if args.download:
        download_csv_version(args.download, args.output)
    else:
        list_csv_versions(prefix=args.prefix, limit=args.limit)

if __name__ == "__main__":
    main()