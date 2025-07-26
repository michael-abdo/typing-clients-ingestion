#!/usr/bin/env python3
"""
Safely remove local files that have been verified in S3
"""
import os
import json
import shutil
from pathlib import Path

def main():
    # Read the verification report
    report_file = 's3_upload_verification_report.json'
    
    if not os.path.exists(report_file):
        print("❌ Error: s3_upload_verification_report.json not found")
        print("Please run upload_all_to_s3.py first")
        return 1
    
    with open(report_file, 'r') as f:
        report = json.load(f)
    
    # Check if all uploads were successful
    successful = [r for r in report['results'] if r['status'] in ['uploaded', 'already_exists']]
    failed = [r for r in report['results'] if r['status'] == 'failed']
    
    if failed:
        print(f"❌ Cannot delete files - {len(failed)} uploads failed!")
        for f in failed:
            print(f"  - {f['file']}: {f.get('error', 'Unknown error')}")
        return 1
    
    print("✅ All files verified in S3")
    print(f"Total files: {len(successful)}")
    print(f"Total size: {report['total_size_gb']:.2f} GB")
    print("\nFiles to delete:")
    
    # List files to be deleted
    for result in successful:
        file_path = Path(result['file'])
        if file_path.exists():
            print(f"  - {file_path}")
    
    # Confirm deletion
    response = input("\nAre you sure you want to delete these local files? (yes/no): ")
    if response.lower() != 'yes':
        print("Deletion cancelled")
        return 0
    
    # Delete files
    deleted_count = 0
    deleted_size = 0
    
    for result in successful:
        file_path = Path(result['file'])
        if file_path.exists():
            try:
                file_size = file_path.stat().st_size
                file_path.unlink()
                deleted_count += 1
                deleted_size += file_size
                print(f"  ✅ Deleted: {file_path.name}")
            except Exception as e:
                print(f"  ❌ Error deleting {file_path}: {e}")
    
    # Clean up empty directories
    downloads_dir = Path('downloads')
    if downloads_dir.exists():
        for client_dir in downloads_dir.iterdir():
            if client_dir.is_dir():
                # Check if directory only contains JSON files
                non_json_files = [f for f in client_dir.iterdir() if f.is_file() and f.suffix != '.json']
                if not non_json_files:
                    # Remove JSON files
                    for json_file in client_dir.glob('*.json'):
                        json_file.unlink()
                    # Remove empty directory
                    client_dir.rmdir()
                    print(f"  🗑️  Removed empty directory: {client_dir.name}")
    
    print(f"\n✅ Deletion complete!")
    print(f"   Files deleted: {deleted_count}")
    print(f"   Space freed: {deleted_size / (1024**3):.2f} GB")
    print(f"\n📊 All content remains safely stored in S3")

if __name__ == "__main__":
    main()