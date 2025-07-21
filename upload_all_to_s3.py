#!/usr/bin/env python3
"""
Upload all local files to S3 for clients 502-506
"""
import boto3
import os
from pathlib import Path
import json
from datetime import datetime
import hashlib

def calculate_md5(file_path, chunk_size=8192):
    """Calculate MD5 hash of a file"""
    md5 = hashlib.md5()
    with open(file_path, 'rb') as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            md5.update(chunk)
    return md5.hexdigest()

def main():
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    downloads_dir = Path('downloads')
    
    upload_results = []
    total_size = 0
    total_files = 0
    
    # Target clients
    target_clients = [502, 503, 504, 505, 506]
    
    print("🚀 Starting S3 Upload for clients 502-506")
    print(f"🪣 Target bucket: {bucket}")
    print("=" * 70)
    
    for client_id in target_clients:
        # Find client directories
        client_dirs = list(downloads_dir.glob(f"{client_id}_*"))
        
        for client_dir in client_dirs:
            if not client_dir.is_dir():
                continue
                
            print(f"\n📁 Processing {client_dir.name}")
            
            # Upload all files (excluding JSON metadata)
            for file_path in client_dir.iterdir():
                if file_path.is_file() and not file_path.suffix == '.json':
                    # Create S3 key
                    s3_key = f"{client_id}/{client_dir.name.split('_', 1)[1]}/{file_path.name}"
                    
                    try:
                        # Check if already exists in S3
                        try:
                            s3_head = s3.head_object(Bucket=bucket, Key=s3_key)
                            s3_size = s3_head['ContentLength']
                            local_size = file_path.stat().st_size
                            
                            if s3_size == local_size:
                                print(f"  ✓ Already in S3: {file_path.name}")
                                upload_results.append({
                                    'file': str(file_path),
                                    's3_key': s3_key,
                                    'status': 'already_exists',
                                    'size': local_size
                                })
                                continue
                        except:
                            pass  # File doesn't exist in S3
                        
                        # Upload file
                        print(f"  ⬆️  Uploading {file_path.name}...")
                        
                        file_size = file_path.stat().st_size
                        
                        # Use multipart upload for large files
                        if file_size > 100 * 1024 * 1024:  # 100MB
                            s3.upload_file(
                                str(file_path),
                                bucket,
                                s3_key,
                                Config=boto3.s3.transfer.TransferConfig(
                                    multipart_threshold=1024 * 25,
                                    max_concurrency=10,
                                    multipart_chunksize=1024 * 25,
                                    use_threads=True
                                )
                            )
                        else:
                            s3.upload_file(str(file_path), bucket, s3_key)
                        
                        # Verify upload
                        s3_head = s3.head_object(Bucket=bucket, Key=s3_key)
                        if s3_head['ContentLength'] == file_size:
                            print(f"  ✅ Success: {file_path.name} ({file_size / (1024**2):.1f} MB)")
                            upload_results.append({
                                'file': str(file_path),
                                's3_key': s3_key,
                                'status': 'uploaded',
                                'size': file_size
                            })
                            total_size += file_size
                            total_files += 1
                        else:
                            print(f"  ❌ Size mismatch: {file_path.name}")
                            upload_results.append({
                                'file': str(file_path),
                                's3_key': s3_key,
                                'status': 'size_mismatch',
                                'local_size': file_size,
                                's3_size': s3_head['ContentLength']
                            })
                            
                    except Exception as e:
                        print(f"  ❌ Failed: {file_path.name} - {str(e)}")
                        upload_results.append({
                            'file': str(file_path),
                            's3_key': s3_key,
                            'status': 'failed',
                            'error': str(e)
                        })
    
    # Save upload report
    report = {
        'timestamp': datetime.now().isoformat(),
        'bucket': bucket,
        'total_files_uploaded': total_files,
        'total_size_bytes': total_size,
        'total_size_gb': total_size / (1024**3),
        'results': upload_results
    }
    
    with open('s3_upload_verification_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    # Print summary
    print("\n" + "=" * 70)
    print("📊 UPLOAD SUMMARY")
    print("=" * 70)
    print(f"✅ Files uploaded: {total_files}")
    print(f"💾 Total size: {total_size / (1024**3):.2f} GB")
    print(f"📄 Report saved: s3_upload_verification_report.json")
    
    # Check if all uploads successful
    failed = [r for r in upload_results if r['status'] == 'failed']
    if failed:
        print(f"\n⚠️  WARNING: {len(failed)} uploads failed!")
        print("DO NOT DELETE LOCAL FILES!")
    else:
        successful = [r for r in upload_results if r['status'] in ['uploaded', 'already_exists']]
        print(f"\n✅ All {len(successful)} files are in S3")
        print("It is now safe to delete local files if desired")

if __name__ == "__main__":
    main()