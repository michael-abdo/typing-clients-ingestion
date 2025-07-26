#!/usr/bin/env python3
"""
Check if people with metadata in clients/ directory have actual media files in S3
"""

import boto3
import json
import csv
from collections import defaultdict

def check_metadata_vs_media():
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    
    print('=== CHECKING METADATA vs ACTUAL MEDIA FILES ===')
    print()
    
    # Step 1: Get all people with metadata
    metadata_people = set()
    metadata_by_person = defaultdict(list)
    
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix='clients/'):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.json'):
                response = s3.get_object(Bucket=bucket, Key=obj['Key'])
                metadata = json.loads(response['Body'].read())
                
                row_id = metadata.get('row_id')
                person_name = metadata.get('person', 'Unknown')
                
                if row_id:
                    metadata_people.add(row_id)
                    metadata_by_person[row_id].append({
                        'name': person_name,
                        'type': metadata.get('type'),
                        'url': metadata.get('url'),
                        'id': metadata.get('id') or metadata.get('playlist_id')
                    })
    
    # Step 2: Get all people with actual media files from CSV
    people_with_media = {}
    
    with open('outputs/output.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_id = row.get('row_id', '').strip()
            person_name = row.get('name', '').strip()
            
            if row_id and row_id.isdigit():
                row_id = int(row_id)
                s3_paths_str = row.get('s3_paths', '{}')
                
                if s3_paths_str and s3_paths_str != '{}':
                    try:
                        s3_paths = json.loads(s3_paths_str)
                        if s3_paths:
                            people_with_media[row_id] = {
                                'name': person_name,
                                'file_count': len(s3_paths),
                                'files': list(s3_paths.values())
                            }
                    except:
                        pass
    
    # Step 3: Compare and report
    print('📊 ANALYSIS RESULTS:')
    print()
    
    # Check each person with metadata
    for row_id in sorted(metadata_people):
        person_metadata = metadata_by_person[row_id]
        person_name = person_metadata[0]['name']
        
        print(f'{row_id} - {person_name}:')
        print(f'  📋 Metadata: {len(person_metadata)} items')
        for meta in person_metadata:
            print(f'     - {meta["type"]}: {meta["id"]}')
        
        if row_id in people_with_media:
            media_info = people_with_media[row_id]
            print(f'  ✅ MEDIA FILES: {media_info["file_count"]} files in S3')
            for i, file_path in enumerate(media_info['files'][:3]):
                print(f'     - {file_path}')
            if media_info['file_count'] > 3:
                print(f'     ... and {media_info["file_count"] - 3} more files')
        else:
            print(f'  ❌ NO MEDIA FILES in S3')
        print()
    
    # Summary
    has_both = sum(1 for rid in metadata_people if rid in people_with_media)
    metadata_only = sum(1 for rid in metadata_people if rid not in people_with_media)
    
    print('=' * 60)
    print('SUMMARY:')
    print(f'✅ {has_both} people have BOTH metadata AND media files')
    print(f'❌ {metadata_only} people have metadata but NO media files')
    print()
    
    if metadata_only > 0:
        print('⚠️  RECOMMENDATION: Do NOT delete metadata for people without media files!')
        print('   These metadata files may need to be processed to download the media.')
    else:
        print('🎯 SAFE TO DELETE: All people with metadata already have media files.')
    
    # List people missing media
    if metadata_only > 0:
        print('\n❌ People with metadata but NO media files:')
        for row_id in sorted(metadata_people):
            if row_id not in people_with_media:
                person_name = metadata_by_person[row_id][0]['name']
                print(f'   - {row_id} - {person_name}')

if __name__ == "__main__":
    check_metadata_vs_media()