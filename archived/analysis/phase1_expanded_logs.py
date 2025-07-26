#!/usr/bin/env python3
"""Phase 1: Search expanded logs for additional client-file mappings"""

import glob
import re
import boto3
import pandas as pd
from orphaned_file_recovery_tracker import get_tracker

def search_all_logs_for_mappings():
    """Search all available logs for client-file mappings"""
    tracker = get_tracker()
    
    print("🔍 PHASE 1: EXPANDED LOG SEARCH")
    print("=" * 40)
    
    # Search patterns for different log types
    log_files = []
    
    # Collect all potential log files
    patterns = [
        "logs/runs/*/main.log",
        "logs/runs/*/errors.log", 
        "logs/runs/*/youtube.log",
        "logs/runs/*/drive.log",
        "*.log",
        "**/*.txt"
    ]
    
    for pattern in patterns:
        log_files.extend(glob.glob(pattern, recursive=True))
    
    # Remove duplicates and our recovery logs
    log_files = list(set([f for f in log_files if 'recovery_session' not in f]))
    
    print(f"   Searching {len(log_files)} log files...")
    
    client_mappings = {}
    
    for log_file in log_files:
        try:
            with open(log_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                
                # Pattern 1: "Missing in S3: files/UUID" with client context
                missing_pattern = r'📤.*?for\s+(.+?)\s+\(Row\s+(\d+)\).*?Missing in S3: files/([a-f0-9-]{36})'
                matches = re.findall(missing_pattern, content, re.MULTILINE | re.DOTALL)
                
                for client_name, row_id, uuid in matches:
                    client_mappings[uuid] = {
                        'client_name': client_name.strip(),
                        'row_id': int(row_id),
                        'source_file': log_file,
                        'pattern': 'missing_in_s3',
                        'confidence': 0.95
                    }
                    print(f"   ✅ Found mapping: {uuid} → {client_name} (Row {row_id})")
                
                # Pattern 2: "Uploading X as files/UUID"
                upload_pattern = r'Uploading\s+(.+?)\s+as\s+files/([a-f0-9-]{36})'
                matches = re.findall(upload_pattern, content)
                
                for filename, uuid in matches:
                    if uuid not in client_mappings:  # Don't overwrite higher confidence mappings
                        client_mappings[uuid] = {
                            'original_filename': filename.strip(),
                            'source_file': log_file,
                            'pattern': 'upload_log',
                            'confidence': 0.7
                        }
                        print(f"   📤 Found upload: {uuid} ← {filename}")
                
                # Pattern 3: Client processing logs
                processing_pattern = r'Processing.*?(\w+\s+\w+).*?Row\s+(\d+).*?([a-f0-9-]{36})'
                matches = re.findall(processing_pattern, content, re.MULTILINE)
                
                for client_name, row_id, uuid in matches:
                    if uuid not in client_mappings:
                        client_mappings[uuid] = {
                            'client_name': client_name.strip(),
                            'row_id': int(row_id),
                            'source_file': log_file,
                            'pattern': 'processing_log',
                            'confidence': 0.8
                        }
                        print(f"   🔄 Found processing: {uuid} → {client_name} (Row {row_id})")
                
        except Exception as e:
            continue
    
    print(f"\n📊 Log Search Results:")
    print(f"   Files searched: {len(log_files)}")
    print(f"   UUID mappings found: {len(client_mappings)}")
    
    # Verify which UUIDs still exist in orphaned files
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    
    response = s3.list_objects_v2(Bucket=bucket, Prefix="files/")
    current_orphaned_uuids = set()
    
    if 'Contents' in response:
        for obj in response['Contents']:
            if not obj['Key'].endswith('/'):
                uuid = obj['Key'].split('/')[-1].split('.')[0]
                current_orphaned_uuids.add(uuid)
    
    # Filter to only mappings for files that still exist
    valid_mappings = {uuid: data for uuid, data in client_mappings.items() 
                     if uuid in current_orphaned_uuids}
    
    print(f"   Valid mappings (files still orphaned): {len(valid_mappings)}")
    
    if valid_mappings:
        print(f"\n🎯 Ready to Execute:")
        for uuid, data in valid_mappings.items():
            if 'row_id' in data:
                print(f"   {uuid} → {data.get('client_name', 'Unknown')} (Row {data['row_id']}) - {data['confidence']:.0%} confidence")
            else:
                print(f"   {uuid} ← {data.get('original_filename', 'Unknown file')} - {data['confidence']:.0%} confidence")
    
    tracker.session_data['log_mappings'] = valid_mappings
    
    return valid_mappings

if __name__ == "__main__":
    mappings = search_all_logs_for_mappings()
    
    if mappings:
        print(f"\n✅ Found {len(mappings)} valid client-file mappings in logs!")
    else:
        print(f"\n❌ No additional mappings found in expanded log search")