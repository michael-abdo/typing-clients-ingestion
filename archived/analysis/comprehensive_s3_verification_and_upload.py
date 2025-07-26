#!/usr/bin/env python3
"""
Comprehensive S3 verification and upload tool for clients 502-506.
This script will:
1. Verify current S3 status
2. Upload missing files
3. Re-verify after upload
4. Generate final verification report
"""

import boto3
import json
import os
import sys
import hashlib
from pathlib import Path
from datetime import datetime
from collections import defaultdict
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

class S3VerificationAndUpload:
    def __init__(self, bucket_name='typing-clients-storage-2025'):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3')
        self.upload_results = []
        
    def verify_s3_inventory(self, client_ids):
        """Get current S3 inventory for specified clients."""
        inventory = defaultdict(list)
        
        for client_id in client_ids:
            try:
                paginator = self.s3_client.get_paginator('list_objects_v2')
                page_iterator = paginator.paginate(
                    Bucket=self.bucket_name,
                    Prefix=f"{client_id}_"
                )
                
                for page in page_iterator:
                    if 'Contents' in page:
                        for obj in page['Contents']:
                            inventory[client_id].append({
                                'key': obj['Key'],
                                'size': obj['Size'],
                                'etag': obj['ETag'].strip('"'),
                                'last_modified': obj['LastModified'].isoformat()
                            })
            except Exception as e:
                print(f"Error listing objects for client {client_id}: {e}")
        
        return inventory
    
    def get_local_inventory(self, download_dir, client_ids):
        """Get local file inventory."""
        inventory = defaultdict(list)
        
        for client_id in client_ids:
            client_dirs = list(Path(download_dir).glob(f"{client_id}_*"))
            
            for client_dir in client_dirs:
                if client_dir.is_dir():
                    for file_path in client_dir.rglob("*"):
                        if file_path.is_file() and not file_path.suffix == '.part':
                            inventory[client_id].append({
                                'path': str(file_path.relative_to(download_dir)),
                                'full_path': str(file_path),
                                'size': file_path.stat().st_size,
                                'name': file_path.name
                            })
        
        return inventory
    
    def calculate_file_hash(self, file_path, chunk_size=8192):
        """Calculate MD5 hash of a file."""
        md5 = hashlib.md5()
        with open(file_path, 'rb') as f:
            while chunk := f.read(chunk_size):
                md5.update(chunk)
        return md5.hexdigest()
    
    def upload_file(self, local_path, s3_key):
        """Upload a single file to S3 with verification."""
        start_time = time.time()
        result = {
            'local_path': local_path,
            's3_key': s3_key,
            'status': 'pending',
            'size': Path(local_path).stat().st_size,
            'error': None,
            'duration': 0
        }
        
        try:
            print(f"\nUploading: {os.path.basename(local_path)}")
            print(f"  Size: {self.format_size(result['size'])}")
            print(f"  To: s3://{self.bucket_name}/{s3_key}")
            
            # Calculate local file hash
            local_hash = self.calculate_file_hash(local_path)
            
            # Upload file
            with open(local_path, 'rb') as f:
                self.s3_client.upload_fileobj(
                    f,
                    self.bucket_name,
                    s3_key,
                    Callback=lambda bytes_transferred: 
                        print(f"  Progress: {self.format_size(bytes_transferred)}/{self.format_size(result['size'])}", end='\r')
                )
            
            # Verify upload
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            s3_etag = response['ETag'].strip('"')
            
            # For single-part uploads, ETag is the MD5 hash
            if '-' not in s3_etag and s3_etag == local_hash:
                result['status'] = 'verified'
            else:
                # For multi-part uploads, just check size
                if response['ContentLength'] == result['size']:
                    result['status'] = 'completed'
                else:
                    result['status'] = 'size_mismatch'
                    result['error'] = f"Size mismatch: local={result['size']}, s3={response['ContentLength']}"
            
            result['duration'] = time.time() - start_time
            print(f"\n  ✅ Upload {result['status']} in {result['duration']:.2f}s")
            
        except Exception as e:
            result['status'] = 'failed'
            result['error'] = str(e)
            result['duration'] = time.time() - start_time
            print(f"\n  ❌ Upload failed: {e}")
        
        return result
    
    def upload_missing_files(self, missing_files):
        """Upload all missing files to S3."""
        total_files = sum(len(files) for files in missing_files.values())
        total_size = sum(f['size'] for files in missing_files.values() for f in files)
        
        print(f"\nStarting upload of {total_files} files ({self.format_size(total_size)})")
        print("=" * 80)
        
        upload_count = 0
        
        for client_id, files in missing_files.items():
            if not files:
                continue
                
            print(f"\n\nClient {client_id}: {len(files)} files")
            print("-" * 40)
            
            for file_info in files:
                upload_count += 1
                print(f"\n[{upload_count}/{total_files}] ", end='')
                
                result = self.upload_file(
                    file_info['full_path'],
                    file_info['path']  # Use relative path as S3 key
                )
                self.upload_results.append(result)
        
        return self.upload_results
    
    def format_size(self, size_bytes):
        """Format size in human-readable format."""
        for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
            if size_bytes < 1024.0:
                return f"{size_bytes:.2f} {unit}"
            size_bytes /= 1024.0
        return f"{size_bytes:.2f} PB"
    
    def generate_final_report(self, initial_inventory, final_inventory, upload_results):
        """Generate comprehensive final verification report."""
        report = {
            'timestamp': datetime.now().isoformat(),
            'bucket': self.bucket_name,
            'initial_s3_files': sum(len(files) for files in initial_inventory.values()),
            'final_s3_files': sum(len(files) for files in final_inventory.values()),
            'upload_summary': {
                'total_attempted': len(upload_results),
                'verified': sum(1 for r in upload_results if r['status'] == 'verified'),
                'completed': sum(1 for r in upload_results if r['status'] == 'completed'),
                'failed': sum(1 for r in upload_results if r['status'] == 'failed'),
                'size_mismatches': sum(1 for r in upload_results if r['status'] == 'size_mismatch')
            },
            'upload_details': upload_results,
            'verification_status': {}
        }
        
        # Check each client
        all_verified = True
        for client_id in ['502', '503', '504', '505', '506']:
            client_uploads = [r for r in upload_results if r['s3_key'].startswith(f"{client_id}_")]
            failed_uploads = [r for r in client_uploads if r['status'] == 'failed']
            
            client_status = {
                'total_files': len(client_uploads),
                'successful': len([r for r in client_uploads if r['status'] in ['verified', 'completed']]),
                'failed': len(failed_uploads),
                'verified': len(failed_uploads) == 0
            }
            
            if failed_uploads:
                client_status['failed_files'] = [r['local_path'] for r in failed_uploads]
                all_verified = False
            
            report['verification_status'][client_id] = client_status
        
        report['all_clients_verified'] = all_verified
        report['safe_to_delete'] = all_verified
        
        return report

def main():
    """Main execution function."""
    client_ids = ['502', '503', '504', '505', '506']
    download_dir = '/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/downloads'
    
    verifier = S3VerificationAndUpload()
    
    print("S3 Verification and Upload Tool")
    print("=" * 80)
    
    # Step 1: Get initial S3 inventory
    print("\nStep 1: Getting initial S3 inventory...")
    initial_s3_inventory = verifier.verify_s3_inventory(client_ids)
    
    # Step 2: Get local inventory
    print("\nStep 2: Getting local inventory...")
    local_inventory = verifier.get_local_inventory(download_dir, client_ids)
    
    # Step 3: Find missing files
    print("\nStep 3: Identifying missing files...")
    missing_files = defaultdict(list)
    
    for client_id in client_ids:
        local_files = local_inventory.get(client_id, [])
        s3_files = initial_s3_inventory.get(client_id, [])
        
        # Create S3 lookup by filename
        s3_by_name = {os.path.basename(f['key']): f for f in s3_files}
        
        for local_file in local_files:
            # Skip metadata files
            if local_file['name'].endswith('_info.json') or local_file['name'].endswith('_metadata.json'):
                continue
                
            if local_file['name'] not in s3_by_name:
                missing_files[client_id].append(local_file)
    
    total_missing = sum(len(files) for files in missing_files.values())
    
    if total_missing == 0:
        print("\n✅ All files are already in S3! No uploads needed.")
        return
    
    print(f"\nFound {total_missing} files missing from S3")
    
    # Step 4: Upload missing files
    response = input("\nProceed with upload? (yes/no): ")
    if response.lower() != 'yes':
        print("Upload cancelled.")
        return
    
    print("\nStep 4: Uploading missing files...")
    upload_results = verifier.upload_missing_files(missing_files)
    
    # Step 5: Get final S3 inventory
    print("\n\nStep 5: Verifying final S3 inventory...")
    final_s3_inventory = verifier.verify_s3_inventory(client_ids)
    
    # Step 6: Generate final report
    print("\nStep 6: Generating final verification report...")
    final_report = verifier.generate_final_report(
        initial_s3_inventory,
        final_s3_inventory,
        upload_results
    )
    
    # Save report
    report_path = f"final_verification_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(report_path, 'w') as f:
        json.dump(final_report, f, indent=2, default=str)
    
    print(f"\nFinal report saved to: {report_path}")
    
    # Print summary
    print("\n" + "=" * 80)
    print("FINAL VERIFICATION SUMMARY")
    print("=" * 80)
    print(f"Total files uploaded: {final_report['upload_summary']['total_attempted']}")
    print(f"  - Verified: {final_report['upload_summary']['verified']}")
    print(f"  - Completed: {final_report['upload_summary']['completed']}")
    print(f"  - Failed: {final_report['upload_summary']['failed']}")
    print(f"  - Size mismatches: {final_report['upload_summary']['size_mismatches']}")
    
    if final_report['safe_to_delete']:
        print("\n✅ ALL FILES VERIFIED IN S3")
        print("It is now safe to delete local files.")
        
        # Generate deletion script
        deletion_script = """#!/bin/bash
# Auto-generated deletion script for verified S3 uploads
# Review carefully before running!

echo "This will delete local files for clients 502-506 that have been verified in S3."
echo "Are you absolutely sure? (type 'DELETE' to confirm)"
read confirmation

if [ "$confirmation" != "DELETE" ]; then
    echo "Deletion cancelled."
    exit 1
fi

# Delete client directories
"""
        for client_id in client_ids:
            if client_id in local_inventory and local_inventory[client_id]:
                deletion_script += f'rm -rf "{download_dir}/{client_id}_"*\n'
        
        deletion_script += '\necho "Deletion complete."'
        
        with open('delete_verified_files.sh', 'w') as f:
            f.write(deletion_script)
        os.chmod('delete_verified_files.sh', 0o755)
        
        print("\nDeletion script generated: delete_verified_files.sh")
        print("Review the script carefully before running!")
    else:
        print("\n⚠️  WARNING: Not all files verified!")
        print("DO NOT delete local files.")
        
        # Show failed uploads
        for client_id, status in final_report['verification_status'].items():
            if status['failed'] > 0:
                print(f"\nClient {client_id} has {status['failed']} failed uploads:")
                for file_path in status.get('failed_files', []):
                    print(f"  - {file_path}")

if __name__ == "__main__":
    main()