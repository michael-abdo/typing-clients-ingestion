#!/usr/bin/env python3
"""
Recover ALL orphaned files using S3 object metadata.
The S3 objects contain complete metadata including:
- person_id
- person_name
- original_key
- migration_timestamp
"""

import json
import boto3
from datetime import datetime
from typing import Dict, List, Tuple

def get_all_orphaned_files_with_metadata() -> List[Dict]:
    """Get all orphaned files and their metadata from S3"""
    s3 = boto3.client('s3')
    bucket_name = 'typing-clients-uuid-system'
    files_with_metadata = []
    files_without_metadata = []
    
    print("Retrieving metadata for all orphaned files...")
    
    try:
        paginator = s3.get_paginator('list_objects_v2')
        
        for page in paginator.paginate(Bucket=bucket_name, Prefix='files/'):
            for obj in page.get('Contents', []):
                key = obj['Key']
                
                # Skip if not a UUID file
                if not key.startswith('files/') or '-' not in key:
                    continue
                
                filename = key.split('/')[-1]
                uuid_part = filename.split('.')[0]
                
                # Check if it's a valid UUID
                if len(uuid_part) != 36 or uuid_part.count('-') != 4:
                    continue
                
                # Get object metadata
                try:
                    response = s3.head_object(Bucket=bucket_name, Key=key)
                    metadata = response.get('Metadata', {})
                    
                    if metadata and 'person_id' in metadata:
                        files_with_metadata.append({
                            'uuid': uuid_part,
                            'current_key': key,
                            'size': obj['Size'],
                            'last_modified': obj['LastModified'],
                            'metadata': metadata
                        })
                    else:
                        files_without_metadata.append({
                            'uuid': uuid_part,
                            'current_key': key,
                            'size': obj['Size'],
                            'last_modified': obj['LastModified']
                        })
                        
                except Exception as e:
                    print(f"Error getting metadata for {key}: {e}")
                    continue
                    
    except Exception as e:
        print(f"Error listing S3 files: {e}")
        
    print(f"Found {len(files_with_metadata)} files WITH metadata")
    print(f"Found {len(files_without_metadata)} files WITHOUT metadata")
    
    return files_with_metadata, files_without_metadata

def recover_files_with_metadata(files_with_metadata: List[Dict], dry_run: bool = True) -> Dict:
    """Recover files using their S3 metadata"""
    s3 = boto3.client('s3')
    bucket_name = 'typing-clients-uuid-system'
    
    recovery_stats = {
        'total_files': len(files_with_metadata),
        'recovered': 0,
        'failed': 0,
        'actions': []
    }
    
    print(f"\nProcessing {len(files_with_metadata)} files for recovery...")
    
    for file_info in files_with_metadata:
        metadata = file_info['metadata']
        person_id = metadata.get('person_id')
        person_name = metadata.get('person_name', '').replace(' ', '_')
        original_key = metadata.get('original_key', '')
        
        # Extract original filename from original_key or use current filename
        if original_key:
            original_filename = original_key.split('/')[-1]
        else:
            # Use current filename
            original_filename = file_info['current_key'].split('/')[-1].replace(file_info['uuid'], f"file_{person_id}")
        
        # Create new key
        new_key = f"{person_id}/{person_name}/{original_filename}"
        
        action = {
            'uuid': file_info['uuid'],
            'current_key': file_info['current_key'],
            'new_key': new_key,
            'person_id': person_id,
            'person_name': person_name,
            'original_key': original_key,
            'migration_timestamp': metadata.get('migration_timestamp', 'unknown')
        }
        
        if not dry_run:
            try:
                # Copy to new location
                copy_source = {'Bucket': bucket_name, 'Key': file_info['current_key']}
                s3.copy_object(
                    CopySource=copy_source,
                    Bucket=bucket_name,
                    Key=new_key,
                    Metadata=metadata,
                    MetadataDirective='REPLACE'
                )
                
                # Delete old file
                s3.delete_object(Bucket=bucket_name, Key=file_info['current_key'])
                
                action['status'] = 'success'
                recovery_stats['recovered'] += 1
                
            except Exception as e:
                action['status'] = 'failed'
                action['error'] = str(e)
                recovery_stats['failed'] += 1
        else:
            action['status'] = 'dry_run'
            
        recovery_stats['actions'].append(action)
        
        # Progress indicator
        if len(recovery_stats['actions']) % 10 == 0:
            print(f"   Processed {len(recovery_stats['actions'])} files...")
    
    return recovery_stats

def create_recovery_report(recovery_stats: Dict, files_without_metadata: List[Dict]):
    """Create a comprehensive recovery report"""
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = f'metadata_recovery_report_{timestamp}.json'
    
    # Group recoveries by person
    person_summary = {}
    for action in recovery_stats['actions']:
        person_id = action.get('person_id', 'unknown')
        person_name = action.get('person_name', 'unknown')
        key = f"{person_id}_{person_name}"
        
        if key not in person_summary:
            person_summary[key] = {
                'person_id': person_id,
                'person_name': person_name,
                'file_count': 0,
                'files': []
            }
        
        person_summary[key]['file_count'] += 1
        person_summary[key]['files'].append(action['original_key'] or action['new_key'])
    
    report = {
        'timestamp': datetime.now().isoformat(),
        'summary': {
            'total_orphaned_files': recovery_stats['total_files'] + len(files_without_metadata),
            'files_with_metadata': recovery_stats['total_files'],
            'files_without_metadata': len(files_without_metadata),
            'files_recovered': recovery_stats['recovered'],
            'files_failed': recovery_stats['failed'],
            'recovery_rate': f"{(recovery_stats['recovered'] / recovery_stats['total_files'] * 100):.2f}%" if recovery_stats['total_files'] > 0 else "0%"
        },
        'person_summary': list(person_summary.values()),
        'recovery_actions': recovery_stats['actions'][:50],  # First 50 for review
        'files_without_metadata': [
            {
                'uuid': f['uuid'],
                'key': f['current_key'],
                'size': f['size']
            } for f in files_without_metadata[:20]  # First 20
        ]
    }
    
    with open(report_file, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    print(f"\nRecovery report saved to: {report_file}")
    
    # Print summary
    print("\n" + "="*60)
    print("RECOVERY SUMMARY")
    print("="*60)
    print(f"Total orphaned files found: {report['summary']['total_orphaned_files']}")
    print(f"Files with metadata (recoverable): {report['summary']['files_with_metadata']}")
    print(f"Files without metadata: {report['summary']['files_without_metadata']}")
    print(f"Files recovered: {report['summary']['files_recovered']}")
    print(f"Recovery rate: {report['summary']['recovery_rate']}")
    
    print(f"\nFiles recovered by person:")
    for person in sorted(person_summary.values(), key=lambda x: x['file_count'], reverse=True)[:10]:
        print(f"  - {person['person_name']} (ID: {person['person_id']}): {person['file_count']} files")
    
    return report

def main():
    """Main recovery process"""
    print("Starting S3 metadata-based recovery...")
    print("="*60)
    
    # Get all orphaned files with metadata
    files_with_metadata, files_without_metadata = get_all_orphaned_files_with_metadata()
    
    if not files_with_metadata:
        print("\n❌ No files with metadata found. Cannot proceed with recovery.")
        return
    
    # First do a dry run
    print("\n" + "="*60)
    print("DRY RUN - Simulating recovery...")
    print("="*60)
    
    recovery_stats = recover_files_with_metadata(files_with_metadata, dry_run=True)
    report = create_recovery_report(recovery_stats, files_without_metadata)
    
    # Ask for confirmation
    print("\n" + "="*60)
    print("DRY RUN COMPLETE")
    print(f"Ready to recover {len(files_with_metadata)} files")
    print("To execute actual recovery, run with --execute flag")
    print("="*60)

if __name__ == "__main__":
    import sys
    if '--execute' in sys.argv:
        print("\n" + "="*60)
        print("EXECUTING ACTUAL RECOVERY...")
        print("="*60)
        
        files_with_metadata, files_without_metadata = get_all_orphaned_files_with_metadata()
        recovery_stats = recover_files_with_metadata(files_with_metadata, dry_run=False)
        report = create_recovery_report(recovery_stats, files_without_metadata)
        
        print("\n✅ RECOVERY COMPLETE!")
    else:
        main()