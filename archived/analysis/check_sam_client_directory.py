#!/usr/bin/env python3
"""Check Sam Torode's client directory for existing files"""

import boto3

def check_sam_directory():
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    
    print("🔍 Checking Sam Torode's client directory...")
    
    # Check clients/502/ directory
    try:
        response = s3.list_objects_v2(Bucket=bucket, Prefix="clients/502/")
        
        if 'Contents' in response:
            print(f"✅ Found {len(response['Contents'])} files in clients/502/:")
            for obj in response['Contents']:
                size_mb = obj['Size'] / (1024 * 1024)
                print(f"   - {obj['Key']} ({size_mb:.1f} MB)")
        else:
            print("📂 clients/502/ directory is empty")
            
    except Exception as e:
        print(f"❌ Error checking clients/502/: {str(e)}")
    
    # Also check if the UUIDs exist anywhere else in S3
    sam_uuids = [
        "b9fded0f-f2d3-467e-8be4-9867a61663d4",
        "6cee39a1-ab8c-4921-8bcb-fe47f4cce2ff", 
        "4b2a3ab0-f60c-4985-a8cf-1ee97c23cae0",
        "518893f0-0956-4505-b4d2-a9d7d299125d",
        "cf45ce2c-c381-486d-b459-9bae320ca271",
        "f099bd02-e19c-4904-a1c0-26332ea8033a"
    ]
    
    print(f"\n🔍 Searching for Sam's UUIDs anywhere in S3...")
    
    # Get all objects in bucket
    paginator = s3.get_paginator('list_objects_v2')
    pages = paginator.paginate(Bucket=bucket)
    
    found_uuids = {}
    
    for page in pages:
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                # Extract UUID from key
                if '/' in key:
                    filename = key.split('/')[-1]
                    uuid = filename.split('.')[0]
                    
                    if uuid in sam_uuids:
                        found_uuids[uuid] = {
                            'key': key,
                            'size': obj['Size'],
                            'last_modified': obj['LastModified']
                        }
    
    if found_uuids:
        print(f"✅ Found {len(found_uuids)} of Sam's UUIDs in S3:")
        for uuid, info in found_uuids.items():
            size_mb = info['size'] / (1024 * 1024)
            print(f"   - {uuid} → {info['key']} ({size_mb:.1f} MB)")
    else:
        print("❌ None of Sam's UUIDs found anywhere in S3")
        print("   This suggests the files were never successfully uploaded")

if __name__ == "__main__":
    check_sam_directory()