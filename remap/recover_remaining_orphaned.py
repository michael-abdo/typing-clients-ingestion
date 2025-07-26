#!/usr/bin/env python3
"""
Recover remaining orphaned files in small batches
"""

import boto3
import json
from datetime import datetime

def recover_remaining_files():
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    
    # Get remaining orphaned files
    orphaned_files = []
    paginator = s3.get_paginator('list_objects_v2')
    
    for page in paginator.paginate(Bucket=bucket, Prefix='files/'):
        for obj in page.get('Contents', []):
            orphaned_files.append(obj['Key'])
    
    print(f"Found {len(orphaned_files)} remaining orphaned files")
    
    recovered = 0
    failed = 0
    
    for i, key in enumerate(orphaned_files):
        try:
            # Get metadata
            response = s3.head_object(Bucket=bucket, Key=key)
            metadata = response.get('Metadata', {})
            
            if metadata and 'person_id' in metadata:
                person_id = metadata.get('person_id')
                person_name = metadata.get('person_name', '').replace(' ', '_')
                original_key = metadata.get('original_key', '')
                
                if original_key:
                    original_filename = original_key.split('/')[-1]
                else:
                    original_filename = key.split('/')[-1]
                
                new_key = f"{person_id}/{person_name}/{original_filename}"
                
                # Copy and delete
                copy_source = {'Bucket': bucket, 'Key': key}
                s3.copy_object(
                    CopySource=copy_source,
                    Bucket=bucket,
                    Key=new_key,
                    Metadata=metadata,
                    MetadataDirective='REPLACE'
                )
                s3.delete_object(Bucket=bucket, Key=key)
                
                recovered += 1
                print(f"[{i+1}/{len(orphaned_files)}] Recovered: {key} -> {new_key}")
            else:
                failed += 1
                print(f"[{i+1}/{len(orphaned_files)}] No metadata for: {key}")
                
        except Exception as e:
            failed += 1
            print(f"[{i+1}/{len(orphaned_files)}] Error processing {key}: {e}")
    
    print(f"\nRecovery complete:")
    print(f"- Recovered: {recovered}")
    print(f"- Failed: {failed}")
    print(f"- Total processed: {len(orphaned_files)}")
    
    # Save summary
    summary = {
        'timestamp': datetime.now().isoformat(),
        'total_processed': len(orphaned_files),
        'recovered': recovered,
        'failed': failed
    }
    
    with open(f'final_recovery_summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json', 'w') as f:
        json.dump(summary, f, indent=2)

if __name__ == "__main__":
    recover_remaining_files()