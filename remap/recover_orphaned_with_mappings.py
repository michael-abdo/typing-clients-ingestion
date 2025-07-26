#!/usr/bin/env python3
"""
Recover orphaned files using discovered client mappings from JSON files.
Based on findings from:
- sam_torode_s3_upload_502.json (UUIDs for Sam Torode)
- client_file_mapping_*.json (client-file relationships)
- s3_upload_specific_rows_report.json (rows 503-506)
"""

import json
import boto3
from pathlib import Path
from datetime import datetime
import os
from typing import Dict, List, Tuple

def load_sam_torode_mappings() -> Dict[str, Dict]:
    """Load UUID mappings from sam_torode_s3_upload_502.json"""
    mappings = {}
    
    try:
        with open('sam_torode_s3_upload_502.json', 'r') as f:
            data = json.load(f)
            
        person_id = data.get('person_id', 502)
        person_name = data.get('person_name', 'Sam Torode')
        
        for file_info in data.get('files', []):
            uuid = file_info.get('file_uuid')
            if uuid:
                mappings[uuid] = {
                    'person_id': person_id,
                    'person_name': person_name,
                    'original_filename': file_info.get('original_filename'),
                    'file_type': file_info.get('file_type'),
                    's3_key': file_info.get('s3_key')
                }
                
    except Exception as e:
        print(f"Error loading Sam Torode mappings: {e}")
        
    return mappings

def load_client_file_mappings() -> Dict[int, Dict]:
    """Load client-file mappings from client_file_mapping JSON files"""
    client_mappings = {}
    
    mapping_files = [
        'client_file_mapping_20250721_142812.json',
        'client_file_mapping_20250721_233159.json'
    ]
    
    for mapping_file in mapping_files:
        if not Path(mapping_file).exists():
            continue
            
        try:
            with open(mapping_file, 'r') as f:
                data = json.load(f)
                
            for client in data.get('clients', []):
                row_id = client.get('row_id')
                if row_id:
                    client_mappings[row_id] = {
                        'name': client.get('name'),
                        'email': client.get('email'),
                        'type': client.get('type'),
                        'files': client.get('files', [])
                    }
                    
        except Exception as e:
            print(f"Error loading {mapping_file}: {e}")
            
    return client_mappings

def get_orphaned_files_in_s3() -> List[Tuple[str, str]]:
    """Get list of orphaned UUID files from S3"""
    s3 = boto3.client('s3')
    bucket_name = 'typing-clients-uuid-system'
    orphaned_files = []
    
    try:
        paginator = s3.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket_name, Prefix='files/'):
            for obj in page.get('Contents', []):
                key = obj['Key']
                # UUID files are in files/ directory with UUID pattern
                if key.startswith('files/') and '-' in key:
                    filename = key.split('/')[-1]
                    # Extract UUID from filename
                    uuid_part = filename.split('.')[0]
                    if len(uuid_part) == 36 and uuid_part.count('-') == 4:
                        orphaned_files.append((uuid_part, key))
                        
    except Exception as e:
        print(f"Error listing S3 files: {e}")
        
    return orphaned_files

def recover_mapped_files(uuid_mappings: Dict[str, Dict], client_mappings: Dict[int, Dict]) -> Dict:
    """Recover orphaned files using discovered mappings"""
    s3 = boto3.client('s3')
    bucket_name = 'typing-clients-uuid-system'
    recovery_stats = {
        'total_orphaned': 0,
        'recovered': 0,
        'failed': 0,
        'recoveries': []
    }
    
    # Get orphaned files from S3
    orphaned_files = get_orphaned_files_in_s3()
    recovery_stats['total_orphaned'] = len(orphaned_files)
    
    print(f"Found {len(orphaned_files)} orphaned UUID files in S3")
    
    # Process each orphaned file
    for file_uuid, s3_key in orphaned_files:
        recovered = False
        
        # Check if we have a direct UUID mapping (Sam Torode)
        if file_uuid in uuid_mappings:
            mapping = uuid_mappings[file_uuid]
            person_id = mapping['person_id']
            person_name = mapping['person_name']
            original_filename = mapping['original_filename']
            
            # Create new S3 key with client structure
            new_key = f"{person_id}/{person_name.replace(' ', '_')}/{original_filename}"
            
            try:
                # Copy to new location
                copy_source = {'Bucket': bucket_name, 'Key': s3_key}
                s3.copy_object(
                    CopySource=copy_source,
                    Bucket=bucket_name,
                    Key=new_key
                )
                
                recovery_stats['recovered'] += 1
                recovery_stats['recoveries'].append({
                    'uuid': file_uuid,
                    'original_key': s3_key,
                    'new_key': new_key,
                    'person_id': person_id,
                    'person_name': person_name,
                    'method': 'uuid_mapping'
                })
                
                print(f"Recovered {file_uuid} -> {new_key}")
                recovered = True
                
            except Exception as e:
                print(f"Failed to recover {file_uuid}: {e}")
                recovery_stats['failed'] += 1
        
        # If not recovered, try pattern matching with client files
        if not recovered:
            # Extract possible original filename patterns
            file_extension = s3_key.split('.')[-1] if '.' in s3_key else ''
            
            # Check each client's files for potential matches
            for row_id, client_info in client_mappings.items():
                for file_info in client_info.get('files', []):
                    # Check if file extension matches
                    if file_info['filename'].endswith(f'.{file_extension}'):
                        # Additional heuristics could be added here
                        # For now, we'll log potential matches
                        print(f"Potential match: {file_uuid} might be {file_info['filename']} for client {row_id}")
    
    return recovery_stats

def create_recovery_report(recovery_stats: Dict):
    """Create a detailed recovery report"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = f'orphaned_file_recovery_report_{timestamp}.json'
    
    report = {
        'timestamp': datetime.now().isoformat(),
        'summary': {
            'total_orphaned_files': recovery_stats['total_orphaned'],
            'files_recovered': recovery_stats['recovered'],
            'files_failed': recovery_stats['failed'],
            'recovery_rate': f"{(recovery_stats['recovered'] / recovery_stats['total_orphaned'] * 100):.2f}%" if recovery_stats['total_orphaned'] > 0 else "0%"
        },
        'recoveries': recovery_stats['recoveries']
    }
    
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2)
        
    print(f"\nRecovery report saved to: {report_file}")
    print(f"Summary:")
    print(f"- Total orphaned files: {recovery_stats['total_orphaned']}")
    print(f"- Files recovered: {recovery_stats['recovered']}")
    print(f"- Files failed: {recovery_stats['failed']}")
    print(f"- Recovery rate: {report['summary']['recovery_rate']}")

def main():
    """Main recovery process"""
    print("Starting orphaned file recovery using discovered mappings...")
    
    # Load mappings
    print("\n1. Loading UUID mappings from sam_torode_s3_upload_502.json...")
    uuid_mappings = load_sam_torode_mappings()
    print(f"   Found {len(uuid_mappings)} UUID mappings")
    
    print("\n2. Loading client-file mappings...")
    client_mappings = load_client_file_mappings()
    print(f"   Found mappings for {len(client_mappings)} clients")
    
    # Show what we have
    if uuid_mappings:
        print("\n   UUID mappings available for:")
        for uuid, info in list(uuid_mappings.items())[:3]:
            print(f"   - {uuid}: {info['original_filename']} ({info['person_name']})")
        if len(uuid_mappings) > 3:
            print(f"   ... and {len(uuid_mappings) - 3} more")
    
    if client_mappings:
        print("\n   Client mappings available for:")
        for row_id, info in list(client_mappings.items())[:5]:
            print(f"   - Row {row_id}: {info['name']} ({len(info['files'])} files)")
    
    # Perform recovery
    print("\n3. Starting file recovery...")
    recovery_stats = recover_mapped_files(uuid_mappings, client_mappings)
    
    # Create report
    print("\n4. Creating recovery report...")
    create_recovery_report(recovery_stats)
    
    print("\nRecovery process completed!")

if __name__ == "__main__":
    main()