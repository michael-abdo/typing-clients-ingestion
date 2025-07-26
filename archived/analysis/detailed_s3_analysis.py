#!/usr/bin/env python3
"""
Detailed S3 analysis and upload preparation for clients 502-506.
"""

import json
import os
from pathlib import Path
from datetime import datetime
from collections import defaultdict

def analyze_verification_report(report_path):
    """Analyze the verification report and create upload plan."""
    with open(report_path, 'r') as f:
        report = json.load(f)
    
    upload_plan = defaultdict(list)
    total_size_to_upload = 0
    total_files_to_upload = 0
    
    print("DETAILED S3 UPLOAD ANALYSIS")
    print("=" * 80)
    print(f"Report generated: {report['timestamp']}")
    print(f"Buckets checked: {', '.join(report['buckets_checked'])}")
    print()
    
    # Analyze each client
    for client_id in ['502', '503', '504', '505', '506']:
        print(f"\nCLIENT {client_id}:")
        print("-" * 40)
        
        comparison = report['comparison_results'].get(client_id, {})
        missing_in_s3 = comparison.get('missing_in_s3', [])
        
        if not missing_in_s3:
            if client_id == '505':
                print("  No local files found (client may not exist)")
            else:
                print("  ✅ All files already in S3")
            continue
        
        print(f"  Files missing in S3: {len(missing_in_s3)}")
        
        # Group by file type
        by_type = defaultdict(list)
        for file_info in missing_in_s3:
            ext = Path(file_info['name']).suffix.lower()
            if 'youtube' in file_info['name']:
                file_type = f"YouTube{ext}"
            elif 'drive' in file_info['name']:
                file_type = f"GoogleDrive{ext}"
            else:
                file_type = f"Other{ext}"
            
            by_type[file_type].append(file_info)
            upload_plan[client_id].append({
                'local_path': file_info['full_path'],
                'relative_path': file_info['path'],
                'size': file_info['size'],
                'file_type': file_type,
                'suggested_s3_key': file_info['path']  # Maintain same structure
            })
            total_size_to_upload += file_info['size']
            total_files_to_upload += 1
        
        # Show breakdown by type
        print("\n  Breakdown by type:")
        for file_type, files in sorted(by_type.items()):
            total_size = sum(f['size'] for f in files)
            print(f"    - {file_type}: {len(files)} files ({format_size(total_size)})")
            for f in files[:3]:  # Show first 3 files
                print(f"      • {f['name']} ({format_size(f['size'])})")
            if len(files) > 3:
                print(f"      ... and {len(files) - 3} more")
    
    # Show S3 files that exist
    print("\n\nFILES ALREADY IN S3:")
    print("-" * 40)
    s3_count = 0
    for client_id, files in report['s3_inventory'].items():
        if files:
            print(f"\nClient {client_id}:")
            for f in files:
                print(f"  • {f['key']} ({format_size(f['size'])}) in {f['bucket']}")
                s3_count += 1
    
    if s3_count == 0:
        print("  ⚠️  NO FILES FOUND IN S3 FOR ANY CLIENT!")
    
    # Summary
    print("\n\nUPLOAD SUMMARY:")
    print("=" * 80)
    print(f"Total files to upload: {total_files_to_upload}")
    print(f"Total size to upload: {format_size(total_size_to_upload)}")
    print(f"Files already in S3: {s3_count}")
    
    # Save upload plan
    upload_plan_path = f"upload_plan_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(upload_plan_path, 'w') as f:
        json.dump({
            'timestamp': datetime.now().isoformat(),
            'total_files': total_files_to_upload,
            'total_size': total_size_to_upload,
            'upload_plan': dict(upload_plan)
        }, f, indent=2)
    
    print(f"\nUpload plan saved to: {upload_plan_path}")
    
    return upload_plan

def format_size(size_bytes):
    """Format size in human-readable format."""
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size_bytes < 1024.0:
            return f"{size_bytes:.2f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.2f} PB"

def generate_upload_script(upload_plan):
    """Generate a script to upload missing files."""
    script_content = '''#!/usr/bin/env python3
"""
Upload missing files to S3 for clients 502-506.
Auto-generated script - review before running.
"""

import boto3
import os
import sys
from pathlib import Path
from datetime import datetime

def upload_file_to_s3(local_path, s3_key, bucket_name='typing-clients-storage-2025'):
    """Upload a single file to S3."""
    s3_client = boto3.client('s3')
    
    try:
        print(f"Uploading {local_path} to s3://{bucket_name}/{s3_key}")
        
        # Get file size for progress tracking
        file_size = Path(local_path).stat().st_size
        
        # Upload with progress callback
        with open(local_path, 'rb') as f:
            s3_client.upload_fileobj(
                f, 
                bucket_name, 
                s3_key,
                Callback=lambda bytes_transferred: print(f"  Progress: {bytes_transferred}/{file_size} bytes", end='\\r')
            )
        
        print(f"  ✅ Upload complete: {s3_key}")
        return True
    except Exception as e:
        print(f"  ❌ Error uploading {local_path}: {e}")
        return False

def main():
    """Main upload function."""
    # Upload plan
    uploads = '''
    
    # Add upload plan here
    for client_id, files in upload_plan.items():
        script_content += f"\n    # Client {client_id}\n"
        for file_info in files:
            script_content += f"    ('{file_info['local_path']}', '{file_info['suggested_s3_key']}'),\n"
    
    script_content += '''    ]
    
    print(f"Starting upload of {len(uploads)} files...")
    success_count = 0
    
    for local_path, s3_key in uploads:
        if upload_file_to_s3(local_path, s3_key):
            success_count += 1
    
    print(f"\\n\\nUpload complete: {success_count}/{len(uploads)} files uploaded successfully")

if __name__ == "__main__":
    main()
'''
    
    script_path = "upload_missing_files.py"
    with open(script_path, 'w') as f:
        f.write(script_content)
    
    os.chmod(script_path, 0o755)
    print(f"\nUpload script generated: {script_path}")
    print("Review the script before running!")

def main():
    """Main execution."""
    # Find the latest verification report
    import glob
    reports = sorted(glob.glob("s3_verification_report_*.json"))
    if not reports:
        print("No verification report found. Run s3_client_inventory.py first.")
        return
    
    latest_report = reports[-1]
    print(f"Using report: {latest_report}\n")
    
    upload_plan = analyze_verification_report(latest_report)
    
    if any(upload_plan.values()):
        generate_upload_script(upload_plan)
        print("\n⚠️  IMPORTANT: Do NOT delete any local files until all uploads are verified!")
    else:
        print("\n✅ No files need to be uploaded.")

if __name__ == "__main__":
    main()