#!/usr/bin/env python3
"""
Upload all downloaded files to S3 and update CSV with S3 URLs.
"""

import os
import boto3
import pandas as pd
from pathlib import Path
import json
from datetime import datetime
import mimetypes

class S3Uploader:
    def __init__(self, bucket_name='typing-clients-storage-2025'):
        self.bucket_name = bucket_name
        # Use AWS credentials from environment
        self.s3_client = boto3.client('s3', region_name='us-east-1')
        self.downloads_dir = Path('downloads')
        self.csv_file = 'outputs/output.csv'
        self.upload_report = {
            "started_at": datetime.now().isoformat(),
            "bucket": bucket_name,
            "uploads": [],
            "errors": []
        }
        
    def get_content_type(self, file_path):
        """Get MIME type for file."""
        content_type, _ = mimetypes.guess_type(file_path)
        if content_type:
            return content_type
        
        # Default types for our files
        if file_path.endswith('.mp3'):
            return 'audio/mpeg'
        elif file_path.endswith('.mp4'):
            return 'video/mp4'
        elif file_path.endswith('.webm'):
            return 'video/webm'
        elif file_path.endswith('.json'):
            return 'application/json'
        else:
            return 'application/octet-stream'
    
    def upload_file(self, local_path, s3_key):
        """Upload a single file to S3."""
        try:
            content_type = self.get_content_type(local_path)
            
            # Upload with metadata
            self.s3_client.upload_file(
                local_path,
                self.bucket_name,
                s3_key,
                ExtraArgs={
                    'ContentType': content_type,
                    'Metadata': {
                        'uploaded_at': datetime.now().isoformat(),
                        'source': 'typing-clients-ingestion'
                    }
                }
            )
            
            # Generate public URL
            s3_url = f"https://{self.bucket_name}.s3.amazonaws.com/{s3_key}"
            return True, s3_url
            
        except Exception as e:
            return False, str(e)
    
    def process_all_downloads(self):
        """Upload all files and track S3 URLs by person."""
        person_s3_data = {}  # row_id -> {youtube_urls: [], drive_urls: []}
        
        # Process each person's directory
        for person_dir in self.downloads_dir.iterdir():
            if not person_dir.is_dir():
                continue
                
            # Extract row_id and name
            dir_parts = person_dir.name.split('_', 1)
            if len(dir_parts) != 2:
                continue
                
            row_id = int(dir_parts[0])
            person_name = dir_parts[1]
            
            print(f"\n📤 Uploading files for {person_name} (Row {row_id})")
            
            person_s3_data[row_id] = {
                'youtube_urls': [],
                'drive_urls': [],
                'all_files': []
            }
            
            # Upload each file in person's directory
            for file_path in person_dir.iterdir():
                if file_path.is_file():
                    # Create S3 key with structure: row_id/person_name/filename
                    s3_key = f"{row_id}/{person_name}/{file_path.name}"
                    
                    print(f"  📎 Uploading {file_path.name}...")
                    success, result = self.upload_file(str(file_path), s3_key)
                    
                    if success:
                        print(f"     ✅ Uploaded to S3")
                        
                        # Track URL by type
                        if 'youtube' in file_path.name:
                            person_s3_data[row_id]['youtube_urls'].append(result)
                        elif 'drive' in file_path.name:
                            person_s3_data[row_id]['drive_urls'].append(result)
                        
                        person_s3_data[row_id]['all_files'].append(result)
                        
                        self.upload_report['uploads'].append({
                            'row_id': row_id,
                            'person': person_name,
                            'file': file_path.name,
                            's3_key': s3_key,
                            's3_url': result,
                            'size': file_path.stat().st_size
                        })
                    else:
                        print(f"     ❌ Failed: {result}")
                        self.upload_report['errors'].append({
                            'row_id': row_id,
                            'person': person_name,
                            'file': file_path.name,
                            'error': result
                        })
        
        return person_s3_data
    
    def update_csv_with_urls(self, person_s3_data):
        """Update CSV with S3 URLs."""
        print("\n📊 Updating CSV with S3 URLs...")
        
        # Read existing CSV
        df = pd.read_csv(self.csv_file)
        
        # Add new columns if they don't exist
        if 's3_youtube_urls' not in df.columns:
            df['s3_youtube_urls'] = ''
        if 's3_drive_urls' not in df.columns:
            df['s3_drive_urls'] = ''
        if 's3_all_files' not in df.columns:
            df['s3_all_files'] = ''
        
        # Update each row with S3 URLs
        for row_id, urls_data in person_s3_data.items():
            mask = df['row_id'] == row_id
            if mask.any():
                # Join URLs with pipe delimiter
                df.loc[mask, 's3_youtube_urls'] = '|'.join(urls_data['youtube_urls'])
                df.loc[mask, 's3_drive_urls'] = '|'.join(urls_data['drive_urls'])
                df.loc[mask, 's3_all_files'] = '|'.join(urls_data['all_files'])
        
        # Save updated CSV
        df.to_csv(self.csv_file, index=False)
        print("✅ CSV updated with S3 URLs")
        
        return df
    
    def save_report(self):
        """Save upload report."""
        self.upload_report['completed_at'] = datetime.now().isoformat()
        
        report_file = 's3_upload_report.json'
        with open(report_file, 'w') as f:
            json.dump(self.upload_report, f, indent=2)
        
        print(f"\n📄 Upload report saved to {report_file}")
        
    def run(self):
        """Main upload process."""
        print("🚀 Starting S3 Upload Process")
        print(f"🪣 Bucket: {self.bucket_name}")
        print("=" * 70)
        
        # Upload all files
        person_s3_data = self.process_all_downloads()
        
        # Update CSV
        self.update_csv_with_urls(person_s3_data)
        
        # Save report
        self.save_report()
        
        # Print summary
        print("\n" + "=" * 70)
        print("📊 UPLOAD SUMMARY")
        print(f"✅ Files uploaded: {len(self.upload_report['uploads'])}")
        print(f"❌ Errors: {len(self.upload_report['errors'])}")
        print(f"👥 People processed: {len(person_s3_data)}")
        
        # Calculate total size
        total_size = sum(u['size'] for u in self.upload_report['uploads'])
        print(f"💾 Total size uploaded: {total_size / (1024**3):.2f} GB")


if __name__ == "__main__":
    uploader = S3Uploader()
    uploader.run()