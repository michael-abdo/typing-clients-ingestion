#!/usr/bin/env python3
"""
Smart recovery of orphaned files using file size matching and discovered mappings.
Uses file sizes from S3 to correlate orphaned UUID files with client files.
"""

import json
import boto3
from pathlib import Path
from datetime import datetime
import os
from typing import Dict, List, Tuple, Optional
from collections import defaultdict

def get_orphaned_files_with_sizes() -> Dict[str, Dict]:
    """Get orphaned UUID files from S3 with their sizes"""
    s3 = boto3.client('s3')
    bucket_name = 'typing-clients-uuid-system'
    orphaned_files = {}
    
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
                        orphaned_files[uuid_part] = {
                            's3_key': key,
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'],
                            'extension': filename.split('.')[-1] if '.' in filename else ''
                        }
                        
    except Exception as e:
        print(f"Error listing S3 files: {e}")
        
    return orphaned_files

def get_client_files_with_sizes() -> Dict[int, List[Dict]]:
    """Get all client files from S3 with their sizes"""
    s3 = boto3.client('s3')
    bucket_name = 'typing-clients-uuid-system'
    client_files = defaultdict(list)
    
    try:
        paginator = s3.get_paginator('list_objects_v2')
        
        # List all files in client directories
        for page in paginator.paginate(Bucket=bucket_name):
            for obj in page.get('Contents', []):
                key = obj['Key']
                # Skip orphaned UUID files
                if key.startswith('files/'):
                    continue
                    
                # Parse client directory structure: row_id/client_name/filename
                parts = key.split('/')
                if len(parts) >= 3 and parts[0].isdigit():
                    row_id = int(parts[0])
                    filename = parts[-1]
                    
                    client_files[row_id].append({
                        's3_key': key,
                        'filename': filename,
                        'size': obj['Size'],
                        'last_modified': obj['LastModified']
                    })
                    
    except Exception as e:
        print(f"Error listing client files: {e}")
        
    return dict(client_files)

def load_known_mappings() -> Dict[str, Dict]:
    """Load known UUID mappings from sam_torode_s3_upload_502.json"""
    mappings = {}
    
    try:
        with open('sam_torode_s3_upload_502.json', 'r') as f:
            data = json.load(f)
            
        person_id = data.get('person_id', 502)
        person_name = data.get('person_name', 'Sam Torode')
        
        for file_info in data.get('files', []):
            uuid = file_info.get('file_uuid')
            if uuid:
                original_filename = file_info.get('original_filename', '')
                # Handle extension mismatch - orphaned files might have different extension
                mappings[uuid] = {
                    'person_id': person_id,
                    'person_name': person_name,
                    'original_filename': original_filename,
                    'file_type': file_info.get('file_type'),
                    'base_filename': original_filename.rsplit('.', 1)[0] if '.' in original_filename else original_filename
                }
                
    except Exception as e:
        print(f"Error loading known mappings: {e}")
        
    return mappings

def find_size_matches(orphaned_files: Dict[str, Dict], client_files: Dict[int, List[Dict]]) -> Dict[str, List[Dict]]:
    """Find potential matches based on file size"""
    size_matches = defaultdict(list)
    
    # Create size index for orphaned files
    size_to_uuid = defaultdict(list)
    for uuid, info in orphaned_files.items():
        size_to_uuid[info['size']].append(uuid)
    
    # Find matches
    for row_id, files in client_files.items():
        for file_info in files:
            size = file_info['size']
            if size in size_to_uuid:
                # Found files with matching size
                for uuid in size_to_uuid[size]:
                    # Check extension match
                    orphan_ext = orphaned_files[uuid]['extension']
                    client_ext = file_info['filename'].split('.')[-1] if '.' in file_info['filename'] else ''
                    
                    if orphan_ext == client_ext:
                        size_matches[uuid].append({
                            'row_id': row_id,
                            'filename': file_info['filename'],
                            's3_key': file_info['s3_key'],
                            'confidence': 'high' if len(size_to_uuid[size]) == 1 else 'medium'
                        })
    
    return dict(size_matches)

def execute_recovery(orphaned_files: Dict[str, Dict], size_matches: Dict[str, List[Dict]], 
                    known_mappings: Dict[str, Dict], dry_run: bool = True) -> Dict:
    """Execute the recovery based on matches"""
    s3 = boto3.client('s3')
    bucket_name = 'typing-clients-uuid-system'
    
    recovery_stats = {
        'total_orphaned': len(orphaned_files),
        'known_mappings': 0,
        'size_matches': 0,
        'recovered': 0,
        'failed': 0,
        'actions': []
    }
    
    # Process known mappings first (highest confidence)
    for uuid, mapping in known_mappings.items():
        if uuid in orphaned_files:
            person_id = mapping['person_id']
            person_name = mapping['person_name'].replace(' ', '_')
            
            # Use actual extension from orphaned file
            orphan_ext = orphaned_files[uuid]['extension']
            base_filename = mapping['base_filename']
            actual_filename = f"{base_filename}.{orphan_ext}" if orphan_ext else mapping['original_filename']
            
            old_key = orphaned_files[uuid]['s3_key']
            new_key = f"{person_id}/{person_name}/{actual_filename}"
            
            action = {
                'uuid': uuid,
                'old_key': old_key,
                'new_key': new_key,
                'method': 'known_mapping',
                'confidence': 'certain',
                'original_mapping': mapping['original_filename'],
                'actual_filename': actual_filename
            }
            
            if not dry_run:
                try:
                    copy_source = {'Bucket': bucket_name, 'Key': old_key}
                    s3.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=new_key)
                    s3.delete_object(Bucket=bucket_name, Key=old_key)
                    action['status'] = 'success'
                    recovery_stats['recovered'] += 1
                except Exception as e:
                    action['status'] = 'failed'
                    action['error'] = str(e)
                    recovery_stats['failed'] += 1
            else:
                action['status'] = 'dry_run'
                
            recovery_stats['actions'].append(action)
            recovery_stats['known_mappings'] += 1
    
    # Process size matches (medium confidence)
    for uuid, matches in size_matches.items():
        if uuid in known_mappings:
            continue  # Already processed
            
        if len(matches) == 1:  # Unique size match
            match = matches[0]
            old_key = orphaned_files[uuid]['s3_key']
            
            # Extract client name from existing S3 key
            client_parts = match['s3_key'].split('/')
            if len(client_parts) >= 2:
                new_key = f"{client_parts[0]}/{client_parts[1]}/{match['filename']}"
            else:
                new_key = match['s3_key']
            
            action = {
                'uuid': uuid,
                'old_key': old_key,
                'new_key': new_key,
                'method': 'size_match',
                'confidence': match['confidence'],
                'file_size': orphaned_files[uuid]['size']
            }
            
            if not dry_run and match['confidence'] == 'high':
                try:
                    copy_source = {'Bucket': bucket_name, 'Key': old_key}
                    s3.copy_object(CopySource=copy_source, Bucket=bucket_name, Key=new_key)
                    s3.delete_object(Bucket=bucket_name, Key=old_key)
                    action['status'] = 'success'
                    recovery_stats['recovered'] += 1
                except Exception as e:
                    action['status'] = 'failed'
                    action['error'] = str(e)
                    recovery_stats['failed'] += 1
            else:
                action['status'] = 'dry_run' if dry_run else 'skipped_low_confidence'
                
            recovery_stats['actions'].append(action)
            recovery_stats['size_matches'] += 1
    
    return recovery_stats

def create_detailed_report(recovery_stats: Dict, orphaned_files: Dict[str, Dict], 
                          size_matches: Dict[str, List[Dict]]):
    """Create a detailed recovery report"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = f'smart_recovery_report_{timestamp}.json'
    
    # Calculate statistics
    total_orphaned = recovery_stats['total_orphaned']
    matched = recovery_stats['known_mappings'] + recovery_stats['size_matches']
    unmatched = total_orphaned - matched
    
    report = {
        'timestamp': datetime.now().isoformat(),
        'summary': {
            'total_orphaned_files': total_orphaned,
            'files_with_known_mappings': recovery_stats['known_mappings'],
            'files_with_size_matches': recovery_stats['size_matches'],
            'files_recovered': recovery_stats['recovered'],
            'files_failed': recovery_stats['failed'],
            'files_unmatched': unmatched,
            'match_rate': f"{(matched / total_orphaned * 100):.2f}%" if total_orphaned > 0 else "0%"
        },
        'actions': recovery_stats['actions'],
        'unmatched_files': []
    }
    
    # Add unmatched files
    for uuid, info in orphaned_files.items():
        if uuid not in [a['uuid'] for a in recovery_stats['actions']]:
            report['unmatched_files'].append({
                'uuid': uuid,
                's3_key': info['s3_key'],
                'size': info['size'],
                'extension': info['extension']
            })
    
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2, default=str)
        
    print(f"\nRecovery report saved to: {report_file}")
    print(f"\nSummary:")
    print(f"- Total orphaned files: {total_orphaned}")
    print(f"- Files with known mappings: {recovery_stats['known_mappings']}")
    print(f"- Files with size matches: {recovery_stats['size_matches']}")
    print(f"- Files unmatched: {unmatched}")
    print(f"- Match rate: {report['summary']['match_rate']}")

def main():
    """Main recovery process"""
    print("Starting smart recovery of orphaned files...")
    
    # Get orphaned files
    print("\n1. Getting orphaned files from S3...")
    orphaned_files = get_orphaned_files_with_sizes()
    print(f"   Found {len(orphaned_files)} orphaned UUID files")
    
    # Get client files
    print("\n2. Getting client files from S3...")
    client_files = get_client_files_with_sizes()
    print(f"   Found files for {len(client_files)} clients")
    
    # Load known mappings
    print("\n3. Loading known UUID mappings...")
    known_mappings = load_known_mappings()
    print(f"   Found {len(known_mappings)} known mappings")
    
    # Find size matches
    print("\n4. Finding matches based on file size...")
    size_matches = find_size_matches(orphaned_files, client_files)
    print(f"   Found size matches for {len(size_matches)} files")
    
    # Show match preview
    print("\n5. Match preview:")
    known_in_orphaned = sum(1 for uuid in known_mappings if uuid in orphaned_files)
    print(f"   - Known mappings that can be recovered: {known_in_orphaned}")
    print(f"   - Size-based matches found: {len(size_matches)}")
    
    # Execute recovery (dry run first)
    print("\n6. Executing recovery (DRY RUN)...")
    recovery_stats = execute_recovery(orphaned_files, size_matches, known_mappings, dry_run=True)
    
    # Create report
    print("\n7. Creating detailed report...")
    create_detailed_report(recovery_stats, orphaned_files, size_matches)
    
    # Ask for confirmation
    print("\n" + "="*60)
    print("DRY RUN COMPLETE - Review the report before proceeding")
    print("To execute actual recovery, run with --execute flag")
    print("="*60)

if __name__ == "__main__":
    import sys
    if '--execute' in sys.argv:
        print("\nEXECUTING ACTUAL RECOVERY...")
        # Re-run with dry_run=False
        orphaned_files = get_orphaned_files_with_sizes()
        client_files = get_client_files_with_sizes()
        known_mappings = load_known_mappings()
        size_matches = find_size_matches(orphaned_files, client_files)
        recovery_stats = execute_recovery(orphaned_files, size_matches, known_mappings, dry_run=False)
        create_detailed_report(recovery_stats, orphaned_files, size_matches)
    else:
        main()