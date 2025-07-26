#!/usr/bin/env python3
"""
Upload Real CSV - Upload the actual output.csv to S3 with versioning
"""

from pathlib import Path
from utils.csv_s3_versioning import get_csv_versioning, CSVS3Versioning
from utils.config import get_config

def upload_real_csv():
    """Upload the real output.csv file to S3 with versioning"""
    print("=== Uploading Real output.csv to S3 ===\n")
    
    # Get the real CSV path from config
    config = get_config()
    csv_path = config.get('paths.output_csv', 'outputs/output.csv')
    
    # Check if file exists
    csv_file = Path(csv_path)
    if not csv_file.exists():
        print(f"❌ Error: CSV file not found at {csv_path}")
        return
    
    # Get file info
    file_size = csv_file.stat().st_size
    print(f"📄 File: {csv_path}")
    print(f"📏 Size: {file_size:,} bytes ({file_size/1024:.1f} KB)")
    
    # Count rows
    with open(csv_file, 'r') as f:
        row_count = sum(1 for line in f)
    print(f"📊 Rows: {row_count:,} (including header)")
    
    # Get CSV versioning instance
    versioning = get_csv_versioning()
    
    # Upload with metadata
    print(f"\n🚀 Uploading to S3...")
    metadata = {
        'source': 'manual_upload',
        'row_count': str(row_count),
        'description': 'Real output.csv from typing-clients-ingestion pipeline'
    }
    
    result = versioning.upload_csv_version(str(csv_path), metadata)
    
    if result['success']:
        print(f"\n✅ Upload successful!")
        print(f"📁 S3 Key: {result['s3_key']}")
        print(f"📎 Versioned Name: {result['versioned_name']}")
        print(f"🔗 S3 URL: {result['s3_url']}")
        print(f"⏱️ Upload Time: {result['timestamp']}")
        
        # List recent versions
        print(f"\n📋 Recent versions of output.csv:")
        versions = versioning.list_csv_versions(prefix_filter="output_", limit=5)
        for i, version in enumerate(versions[:5]):
            print(f"  {i+1}. {version['filename']} - {version['size']:,} bytes - {version['last_modified']}")
    else:
        print(f"\n❌ Upload failed: {result['error']}")

if __name__ == "__main__":
    upload_real_csv()