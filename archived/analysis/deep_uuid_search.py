#!/usr/bin/env python3
"""Deep search for UUID references across all files"""

import boto3
import json
import os
import glob
import re
from orphaned_file_recovery_tracker import get_tracker

def get_current_orphaned_uuids():
    """Get current list of orphaned UUIDs from S3"""
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    
    response = s3.list_objects_v2(Bucket=bucket, Prefix="files/")
    orphaned_uuids = []
    
    if 'Contents' in response:
        for obj in response['Contents']:
            if not obj['Key'].endswith('/'):
                uuid = obj['Key'].split('/')[-1].split('.')[0]
                orphaned_uuids.append(uuid)
    
    return orphaned_uuids

def search_for_uuid_references():
    """Search for UUID references in all local files"""
    tracker = get_tracker()
    
    print("🔍 Getting current orphaned UUIDs...")
    orphaned_uuids = get_current_orphaned_uuids()
    print(f"   Found {len(orphaned_uuids)} orphaned UUIDs to search for")
    
    # Sample a few UUIDs to search for
    sample_uuids = orphaned_uuids[:10]  # Search for first 10 to start
    
    print(f"\n🔍 Searching for references to sample UUIDs...")
    
    # Search patterns
    search_locations = [
        "*.json",
        "*.csv", 
        "*.txt",
        "*.log",
        "*.py",
        "logs/**/*.log",
        "outputs/**/*",
        "**/*.json"
    ]
    
    uuid_references = {}
    
    for pattern in search_locations:
        try:
            files = glob.glob(pattern, recursive=True)
            
            for file_path in files:
                try:
                    # Skip large files and binary files
                    if os.path.getsize(file_path) > 50 * 1024 * 1024:  # Skip files > 50MB
                        continue
                    
                    # Skip binary files
                    if file_path.endswith(('.gz', '.zip', '.tar', '.bin')):
                        continue
                        
                    with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                        content = f.read()
                        
                        found_uuids = []
                        for uuid in sample_uuids:
                            if uuid in content:
                                found_uuids.append(uuid)
                        
                        if found_uuids:
                            uuid_references[file_path] = {
                                'found_uuids': found_uuids,
                                'file_size': os.path.getsize(file_path),
                                'file_type': os.path.splitext(file_path)[1]
                            }
                            print(f"✅ Found {len(found_uuids)} UUIDs in {file_path}")
                
                except Exception as e:
                    continue
                    
        except Exception as e:
            continue
    
    # Now search for any UUID pattern and cross-reference
    print(f"\n🔍 Searching for any UUID patterns...")
    
    uuid_pattern = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
    pattern_matches = {}
    
    for pattern in ["*.json", "*.log", "*.txt"]:
        files = glob.glob(pattern, recursive=True)
        
        for file_path in files:
            try:
                if os.path.getsize(file_path) > 10 * 1024 * 1024:  # Skip files > 10MB
                    continue
                    
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                    
                    # Find all UUID patterns
                    found_uuid_patterns = re.findall(uuid_pattern, content, re.IGNORECASE)
                    
                    if found_uuid_patterns:
                        # Check if any match our orphaned UUIDs
                        matching_orphaned = []
                        for found_uuid in found_uuid_patterns:
                            if found_uuid.lower() in [uuid.lower() for uuid in orphaned_uuids]:
                                matching_orphaned.append(found_uuid)
                        
                        if matching_orphaned:
                            pattern_matches[file_path] = {
                                'all_uuids_found': found_uuid_patterns,
                                'orphaned_matches': matching_orphaned,
                                'file_size': os.path.getsize(file_path)
                            }
                            print(f"🎯 Found {len(matching_orphaned)} orphaned UUIDs in {file_path}")
                            for uuid in matching_orphaned:
                                print(f"     - {uuid}")
            
            except Exception as e:
                continue
    
    print(f"\n📊 UUID Search Results:")
    print(f"   Direct UUID references: {len(uuid_references)}")
    print(f"   Pattern matches with orphaned UUIDs: {len(pattern_matches)}")
    
    tracker.session_data['uuid_search_results'] = {
        'direct_references': uuid_references,
        'pattern_matches': pattern_matches,
        'searched_uuids': sample_uuids,
        'total_orphaned': len(orphaned_uuids)
    }
    
    return {
        'direct_references': uuid_references,
        'pattern_matches': pattern_matches
    }

if __name__ == "__main__":
    print("🔍 DEEP UUID SEARCH")
    print("=" * 60)
    
    results = search_for_uuid_references()
    
    if results['direct_references'] or results['pattern_matches']:
        print(f"\n✅ Found UUID references!")
    else:
        print(f"\n❌ No UUID references found in local files")
        print(f"   This suggests the orphaned files may not have local mapping records")