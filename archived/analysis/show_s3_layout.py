#!/usr/bin/env python3
"""
Show the current S3 bucket structure layout
"""

import boto3
from collections import defaultdict

def show_s3_layout():
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    
    print('=' * 80)
    print(f'S3 BUCKET LAYOUT: {bucket}')
    print('=' * 80)
    
    # Organize files by directory structure
    directories = defaultdict(list)
    total_size = 0
    total_files = 0
    
    paginator = s3.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket):
        for obj in page.get('Contents', []):
            key = obj['Key']
            size = obj['Size']
            
            # Determine directory structure
            if '/' in key:
                directory = '/'.join(key.split('/')[:-1]) + '/'
                filename = key.split('/')[-1]
            else:
                directory = '(root)/'
                filename = key
            
            directories[directory].append({
                'name': filename,
                'size': size,
                'full_path': key
            })
            
            total_size += size
            total_files += 1
    
    # Sort directories
    sorted_dirs = sorted(directories.keys())
    
    for directory in sorted_dirs:
        files = directories[directory]
        dir_size = sum(f['size'] for f in files)
        
        print(f"\n📁 {directory} ({len(files)} files, {dir_size:,} bytes = {dir_size/1024/1024/1024:.2f} GB)")
        
        # Sort files by name for consistent display
        sorted_files = sorted(files, key=lambda x: x['name'])
        
        # Show first 10 files, then summary if more
        for i, file_info in enumerate(sorted_files):
            if i < 10:
                size_mb = file_info['size'] / 1024 / 1024
                if size_mb > 1000:
                    size_str = f"{size_mb/1024:.2f} GB"
                elif size_mb > 1:
                    size_str = f"{size_mb:.1f} MB"
                else:
                    size_str = f"{file_info['size']:,} bytes"
                    
                print(f"   📄 {file_info['name']} ({size_str})")
            elif i == 10:
                remaining = len(sorted_files) - 10
                remaining_size = sum(f['size'] for f in sorted_files[10:])
                print(f"   ... and {remaining} more files ({remaining_size:,} bytes)")
                break
    
    print(f"\n" + "=" * 80)
    print(f"SUMMARY:")
    print(f"📊 Total directories: {len(directories)}")
    print(f"📄 Total files: {total_files}")
    print(f"💾 Total size: {total_size:,} bytes = {total_size/1024/1024/1024:.2f} GB")
    
    # Show file type breakdown for files/ directory
    if 'files/' in directories:
        files_dir = directories['files/']
        extensions = defaultdict(int)
        ext_sizes = defaultdict(int)
        
        for file_info in files_dir:
            name = file_info['name']
            size = file_info['size']
            
            if '.' in name:
                ext = name.split('.')[-1].lower()
            else:
                ext = '(no extension)'
            
            extensions[ext] += 1
            ext_sizes[ext] += size
        
        print(f"\n📁 files/ directory breakdown:")
        for ext in sorted(extensions.keys()):
            count = extensions[ext]
            size = ext_sizes[ext]
            print(f"   .{ext}: {count} files ({size:,} bytes = {size/1024/1024:.1f} MB)")
    
    print("=" * 80)

if __name__ == "__main__":
    show_s3_layout()