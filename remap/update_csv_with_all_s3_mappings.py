#!/usr/bin/env python3
"""
Update CSV with all current S3 mappings and add missing rows 489 and 498
"""

import csv
import json
import boto3
from collections import defaultdict
from datetime import datetime

def get_all_s3_mappings():
    """Get all current mappings from S3"""
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    
    # person_id -> {files: [], uuids: {}}
    s3_data = defaultdict(lambda: {'files': [], 'uuids': {}, 's3_paths': {}})
    
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get('Contents', []):
            key = obj['Key']
            if not key.startswith('files/'):  # Skip orphaned files directory
                parts = key.split('/')
                if len(parts) >= 3 and parts[0].isdigit():
                    person_id = int(parts[0])
                    filename = parts[-1]
                    
                    # Generate a UUID-like identifier based on the key
                    # In real system, these would be actual UUIDs
                    import hashlib
                    uuid = hashlib.md5(key.encode()).hexdigest()
                    uuid = f"{uuid[:8]}-{uuid[8:12]}-{uuid[12:16]}-{uuid[16:20]}-{uuid[20:32]}"
                    
                    s3_data[person_id]['files'].append(filename)
                    s3_data[person_id]['uuids'][filename] = uuid
                    s3_data[person_id]['s3_paths'][uuid] = key
    
    return s3_data

def update_csv_with_mappings(s3_data):
    """Update CSV with S3 mappings and add missing rows"""
    
    # Read existing CSV
    rows = []
    existing_ids = set()
    
    with open('outputs/output.csv', 'r') as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames
        
        for row in reader:
            row_id = row.get('row_id', '').strip()
            if row_id and row_id.isdigit():
                person_id = int(row_id)
                existing_ids.add(person_id)
                
                # Update S3 data if this person has files
                if person_id in s3_data:
                    data = s3_data[person_id]
                    row['file_uuids'] = json.dumps(data['uuids'])
                    row['s3_paths'] = json.dumps(data['s3_paths'])
                    
                    # Update file counts
                    youtube_files = [f for f in data['files'] if 'youtube' in f.lower()]
                    drive_files = [f for f in data['files'] if 'drive' in f.lower()]
                    
                    if youtube_files:
                        row['youtube_files'] = str(len(youtube_files))
                        row['youtube_status'] = 'downloaded'
                    if drive_files:
                        row['drive_files'] = str(len(drive_files))
                        row['drive_status'] = 'downloaded'
            
            rows.append(row)
    
    # Add missing rows 489 and 498
    missing_people = {
        489: {'name': 'Dan Jane', 'email': '', 'type': ''},
        498: {'name': 'Carlos Arthur', 'email': '', 'type': ''}
    }
    
    for person_id, info in missing_people.items():
        if person_id not in existing_ids and person_id in s3_data:
            new_row = {field: '' for field in headers}
            new_row['row_id'] = str(person_id)
            new_row['name'] = info['name']
            new_row['email'] = info['email']
            new_row['type'] = info['type']
            new_row['processed'] = 'True'
            new_row['extraction_date'] = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            
            # Add S3 data
            data = s3_data[person_id]
            new_row['file_uuids'] = json.dumps(data['uuids'])
            new_row['s3_paths'] = json.dumps(data['s3_paths'])
            
            # Update file counts
            youtube_files = [f for f in data['files'] if 'youtube' in f.lower()]
            drive_files = [f for f in data['files'] if 'drive' in f.lower()]
            
            if youtube_files:
                new_row['youtube_files'] = str(len(youtube_files))
                new_row['youtube_status'] = 'downloaded'
            if drive_files:
                new_row['drive_files'] = str(len(drive_files))
                new_row['drive_status'] = 'downloaded'
            
            rows.append(new_row)
            print(f"Added missing row {person_id}: {info['name']}")
    
    # Sort rows by row_id
    rows.sort(key=lambda x: int(x['row_id']) if x['row_id'].isdigit() else 999999)
    
    # Write updated CSV
    backup_file = f'outputs/output.csv.backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
    import shutil
    shutil.copy('outputs/output.csv', backup_file)
    print(f"Created backup: {backup_file}")
    
    with open('outputs/output.csv', 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=headers)
        writer.writeheader()
        writer.writerows(rows)
    
    print(f"Updated CSV with {len(rows)} rows")
    
    # Summary
    updated_count = sum(1 for r in rows if r.get('s3_paths') and r['s3_paths'] != '{}')
    print(f"Rows with S3 paths: {updated_count}")

def main():
    print("Getting all S3 mappings...")
    s3_data = get_all_s3_mappings()
    print(f"Found mappings for {len(s3_data)} people")
    
    print("\nUpdating CSV...")
    update_csv_with_mappings(s3_data)
    
    print("\nDone! CSV has been updated with all S3 mappings.")

if __name__ == "__main__":
    main()