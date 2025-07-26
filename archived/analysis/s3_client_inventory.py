#!/usr/bin/env python3
"""
Create comprehensive S3 inventory for clients 502-506 and compare with local files.
"""

import boto3
import json
import os
from datetime import datetime
from pathlib import Path
import hashlib
from collections import defaultdict

def get_s3_client():
    """Initialize S3 client with credentials from environment."""
    return boto3.client(
        's3',
        aws_access_key_id=os.environ.get('AWS_ACCESS_KEY_ID'),
        aws_secret_access_key=os.environ.get('AWS_SECRET_ACCESS_KEY'),
        region_name=os.environ.get('AWS_DEFAULT_REGION', 'us-east-1')
    )

def list_s3_files_for_clients(bucket_name, client_ids):
    """List all S3 files for specified client IDs."""
    s3 = get_s3_client()
    s3_inventory = defaultdict(list)
    
    for client_id in client_ids:
        print(f"\nChecking S3 for client {client_id}...")
        
        # List objects with prefix
        paginator = s3.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(
            Bucket=bucket_name,
            Prefix=f"{client_id}_"
        )
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    file_info = {
                        'key': obj['Key'],
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'].isoformat(),
                        'etag': obj['ETag'].strip('"')
                    }
                    s3_inventory[client_id].append(file_info)
                    print(f"  Found: {obj['Key']} ({obj['Size']} bytes)")
    
    return s3_inventory

def get_local_files(download_dir, client_ids):
    """Get all local files for specified client IDs."""
    local_inventory = defaultdict(list)
    
    for client_id in client_ids:
        client_dirs = list(Path(download_dir).glob(f"{client_id}_*"))
        
        for client_dir in client_dirs:
            if client_dir.is_dir():
                print(f"\nScanning local directory: {client_dir}")
                
                for file_path in client_dir.rglob("*"):
                    if file_path.is_file():
                        # Skip .part files and info.json files for comparison
                        if file_path.suffix == '.part':
                            continue
                            
                        rel_path = file_path.relative_to(download_dir)
                        file_info = {
                            'path': str(rel_path),
                            'full_path': str(file_path),
                            'size': file_path.stat().st_size,
                            'name': file_path.name,
                            'is_metadata': file_path.name.endswith('_info.json') or file_path.name.endswith('_metadata.json')
                        }
                        local_inventory[client_id].append(file_info)
                        print(f"  Found: {rel_path} ({file_info['size']} bytes)")
    
    return local_inventory

def compare_inventories(s3_inventory, local_inventory, client_ids):
    """Compare S3 and local inventories."""
    comparison_results = {}
    
    for client_id in client_ids:
        print(f"\n\nAnalyzing client {client_id}...")
        
        s3_files = s3_inventory.get(client_id, [])
        local_files = local_inventory.get(client_id, [])
        
        # Create lookup dictionaries
        s3_by_name = {}
        for s3_file in s3_files:
            # Extract filename from S3 key
            filename = os.path.basename(s3_file['key'])
            s3_by_name[filename] = s3_file
        
        local_by_name = {}
        for local_file in local_files:
            if not local_file['is_metadata']:  # Skip metadata files for main comparison
                local_by_name[local_file['name']] = local_file
        
        # Find matches and mismatches
        matched_files = []
        size_mismatches = []
        missing_in_s3 = []
        missing_locally = []
        
        # Check local files against S3
        for filename, local_file in local_by_name.items():
            if filename in s3_by_name:
                s3_file = s3_by_name[filename]
                if local_file['size'] == s3_file['size']:
                    matched_files.append({
                        'filename': filename,
                        'size': local_file['size'],
                        'local_path': local_file['path'],
                        's3_key': s3_file['key']
                    })
                else:
                    size_mismatches.append({
                        'filename': filename,
                        'local_size': local_file['size'],
                        's3_size': s3_file['size'],
                        'local_path': local_file['path'],
                        's3_key': s3_file['key']
                    })
            else:
                missing_in_s3.append(local_file)
        
        # Check S3 files not in local
        for filename, s3_file in s3_by_name.items():
            if filename not in local_by_name:
                missing_locally.append(s3_file)
        
        comparison_results[client_id] = {
            'matched_files': matched_files,
            'size_mismatches': size_mismatches,
            'missing_in_s3': missing_in_s3,
            'missing_locally': missing_locally,
            'total_local_files': len(local_files),
            'total_s3_files': len(s3_files),
            'matched_count': len(matched_files),
            'mismatch_count': len(size_mismatches),
            'missing_in_s3_count': len(missing_in_s3),
            'missing_locally_count': len(missing_locally)
        }
        
        # Print summary
        print(f"  Total local files: {len(local_files)}")
        print(f"  Total S3 files: {len(s3_files)}")
        print(f"  Matched files: {len(matched_files)}")
        print(f"  Size mismatches: {len(size_mismatches)}")
        print(f"  Missing in S3: {len(missing_in_s3)}")
        print(f"  Missing locally: {len(missing_locally)}")
    
    return comparison_results

def generate_report(s3_inventory, local_inventory, comparison_results, client_ids):
    """Generate comprehensive verification report."""
    report = {
        'timestamp': datetime.now().isoformat(),
        'buckets_checked': ['typing-clients-uuid-system', 'typing-clients-storage-2025', 'xenodex-client-output-new'],
        'clients_checked': client_ids,
        'summary': {},
        's3_inventory': s3_inventory,
        'local_inventory': local_inventory,
        'comparison_results': comparison_results,
        'verification_status': {}
    }
    
    # Calculate overall summary
    total_matched = 0
    total_mismatches = 0
    total_missing_s3 = 0
    total_missing_local = 0
    all_verified = True
    
    for client_id in client_ids:
        result = comparison_results.get(client_id, {})
        total_matched += result.get('matched_count', 0)
        total_mismatches += result.get('mismatch_count', 0)
        total_missing_s3 += result.get('missing_in_s3_count', 0)
        total_missing_local += result.get('missing_locally_count', 0)
        
        # Determine verification status
        client_verified = (
            result.get('mismatch_count', 0) == 0 and
            result.get('missing_in_s3_count', 0) == 0
        )
        report['verification_status'][client_id] = {
            'verified': client_verified,
            'issues': []
        }
        
        if result.get('mismatch_count', 0) > 0:
            report['verification_status'][client_id]['issues'].append(
                f"{result['mismatch_count']} files have size mismatches"
            )
        if result.get('missing_in_s3_count', 0) > 0:
            report['verification_status'][client_id]['issues'].append(
                f"{result['missing_in_s3_count']} files missing in S3"
            )
        
        if not client_verified:
            all_verified = False
    
    report['summary'] = {
        'total_files_matched': total_matched,
        'total_size_mismatches': total_mismatches,
        'total_missing_in_s3': total_missing_s3,
        'total_missing_locally': total_missing_local,
        'all_clients_verified': all_verified,
        'safe_to_delete_local': all_verified and total_missing_s3 == 0
    }
    
    return report

def main():
    """Main execution function."""
    # Check multiple possible buckets
    bucket_names = [
        'typing-clients-uuid-system',
        'typing-clients-storage-2025',
        'xenodex-client-output-new'
    ]
    client_ids = ['502', '503', '504', '505', '506']
    download_dir = '/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/downloads'
    
    print("S3 Client Inventory and Verification Tool")
    print("=" * 50)
    
    # Get S3 inventory from all buckets
    print("\nStep 1: Getting S3 inventory from all buckets...")
    combined_s3_inventory = defaultdict(list)
    
    for bucket_name in bucket_names:
        print(f"\nChecking bucket: {bucket_name}")
        try:
            bucket_inventory = list_s3_files_for_clients(bucket_name, client_ids)
            # Merge results
            for client_id, files in bucket_inventory.items():
                for file_info in files:
                    file_info['bucket'] = bucket_name  # Add bucket info
                    combined_s3_inventory[client_id].append(file_info)
        except Exception as e:
            print(f"  Error accessing bucket {bucket_name}: {e}")
    
    s3_inventory = combined_s3_inventory
    
    # Get local inventory
    print("\nStep 2: Getting local file inventory...")
    local_inventory = get_local_files(download_dir, client_ids)
    
    # Compare inventories
    print("\nStep 3: Comparing S3 and local inventories...")
    comparison_results = compare_inventories(s3_inventory, local_inventory, client_ids)
    
    # Generate report
    print("\nStep 4: Generating verification report...")
    report = generate_report(s3_inventory, local_inventory, comparison_results, client_ids)
    
    # Save report
    report_path = f"s3_verification_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2, default=str)
    
    print(f"\n\nReport saved to: {report_path}")
    
    # Print summary
    print("\n" + "=" * 50)
    print("VERIFICATION SUMMARY")
    print("=" * 50)
    print(f"Total files matched: {report['summary']['total_files_matched']}")
    print(f"Total size mismatches: {report['summary']['total_size_mismatches']}")
    print(f"Total missing in S3: {report['summary']['total_missing_in_s3']}")
    print(f"Total missing locally: {report['summary']['total_missing_locally']}")
    print(f"\nAll clients verified: {report['summary']['all_clients_verified']}")
    print(f"Safe to delete local files: {report['summary']['safe_to_delete_local']}")
    
    if not report['summary']['safe_to_delete_local']:
        print("\n⚠️  WARNING: Not all files are verified in S3. DO NOT delete local files!")
        print("\nIssues found:")
        for client_id, status in report['verification_status'].items():
            if not status['verified']:
                print(f"\nClient {client_id}:")
                for issue in status['issues']:
                    print(f"  - {issue}")
    else:
        print("\n✅ All files verified in S3. Safe to proceed with local file removal.")
    
    return report

if __name__ == "__main__":
    main()