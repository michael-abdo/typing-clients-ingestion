#!/usr/bin/env python3
"""
Check if people with metadata but no media files have the same URLs in their CSV entries
"""

import boto3
import json
import csv

def check_csv_urls_vs_metadata():
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    
    print('=== CHECKING CSV URLs vs METADATA URLs ===')
    print()
    
    # People with metadata but no media files
    people_to_check = [476, 477, 483, 484, 496]
    
    # Step 1: Get metadata URLs for these people
    metadata_urls = {}
    
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix='clients/'):
        for obj in page.get('Contents', []):
            if obj['Key'].endswith('.json'):
                response = s3.get_object(Bucket=bucket, Key=obj['Key'])
                metadata = json.loads(response['Body'].read())
                
                row_id = metadata.get('row_id')
                if row_id in people_to_check:
                    if row_id not in metadata_urls:
                        metadata_urls[row_id] = {
                            'name': metadata.get('person'),
                            'urls': []
                        }
                    metadata_urls[row_id]['urls'].append(metadata.get('url'))
    
    # Step 2: Get CSV entries for these people
    csv_entries = {}
    
    with open('outputs/output.csv', 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            row_id = row.get('row_id', '').strip()
            
            if row_id and row_id.isdigit():
                row_id = int(row_id)
                if row_id in people_to_check:
                    csv_entries[row_id] = {
                        'name': row.get('name', ''),
                        'typing_link': row.get('typing_link', ''),
                        'text_content': row.get('text_content', ''),
                        's3_paths': row.get('s3_paths', '{}'),
                        'file_uuids': row.get('file_uuids', '{}')
                    }
    
    # Step 3: Compare and report
    print('📊 COMPARISON RESULTS:')
    print()
    
    for row_id in sorted(people_to_check):
        person_name = metadata_urls.get(row_id, {}).get('name', 'Unknown')
        print(f'{row_id} - {person_name}:')
        
        # Show metadata URLs
        if row_id in metadata_urls:
            print(f'  📋 Metadata URLs:')
            for url in metadata_urls[row_id]['urls']:
                print(f'     - {url}')
        
        # Show CSV info
        if row_id in csv_entries:
            csv_data = csv_entries[row_id]
            print(f'  📄 CSV Entry:')
            print(f'     - typing_link: {csv_data["typing_link"]}')
            print(f'     - text_content: {csv_data["text_content"][:100]}...' if csv_data["text_content"] else '     - text_content: (empty)')
            print(f'     - s3_paths: {csv_data["s3_paths"]}')
            print(f'     - file_uuids: {csv_data["file_uuids"]}')
            
            # Check if typing_link matches metadata URLs
            typing_link = csv_data['typing_link']
            metadata_url_list = metadata_urls.get(row_id, {}).get('urls', [])
            
            if typing_link in metadata_url_list:
                print(f'  ✅ CSV typing_link MATCHES metadata URL')
            else:
                print(f'  ❌ CSV typing_link DOES NOT match metadata URLs')
                
                # Check if they're related (e.g., same document/folder ID)
                for meta_url in metadata_url_list:
                    if any(part in typing_link for part in meta_url.split('/') if len(part) > 10):
                        print(f'  🔗 But they appear to be related URLs')
                        break
        else:
            print(f'  ❌ NO CSV ENTRY FOUND')
        
        print()
    
    print('=' * 60)
    print('SUMMARY:')
    print('The metadata URLs are extraction results from the typing_link processing.')
    print('The CSV typing_link is the original document, while metadata shows')
    print('the specific files/folders found within those documents.')

if __name__ == "__main__":
    check_csv_urls_vs_metadata()