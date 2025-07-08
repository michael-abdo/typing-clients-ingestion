#!/usr/bin/env python3
"""
Direct-to-S3 download using streaming (no local storage).
Following CLAUDE.md: smallest possible execution with existing code.
"""

import os
import subprocess
import requests
import boto3
import pandas as pd
from pathlib import Path
from io import BytesIO
import tempfile

class DirectS3Downloader:
    def __init__(self, bucket_name='typing-clients-storage-2025'):
        self.bucket = bucket_name
        self.s3 = boto3.client('s3', region_name='us-east-1')
        self.csv_file = 'outputs/output.csv'
    
    def stream_youtube_to_s3(self, url, s3_key, person_name):
        """Stream YouTube directly to S3 using named pipe."""
        pipe_path = f"/tmp/youtube_{person_name}_{os.getpid()}"
        
        try:
            # Create named pipe
            if os.path.exists(pipe_path):
                os.remove(pipe_path)
            os.mkfifo(pipe_path)
            
            print(f"  📥 Streaming YouTube to S3: {s3_key}")
            
            # Start yt-dlp
            cmd = ["yt-dlp", "-f", "bestaudio", "-o", pipe_path, url]
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            # Upload from pipe to S3
            with open(pipe_path, 'rb') as pipe_file:
                self.s3.upload_fileobj(
                    pipe_file,
                    self.bucket,
                    s3_key,
                    ExtraArgs={'ContentType': 'video/webm'}
                )
            
            process.wait()
            
            if process.returncode == 0:
                print(f"     ✅ Streamed to S3")
                return True, f"https://{self.bucket}.s3.amazonaws.com/{s3_key}"
            else:
                error = process.stderr.read().decode()
                print(f"     ❌ yt-dlp failed: {error[:100]}")
                return False, error
                
        except Exception as e:
            print(f"     ❌ Stream failed: {str(e)}")
            return False, str(e)
        finally:
            if os.path.exists(pipe_path):
                os.remove(pipe_path)
    
    def stream_drive_to_s3(self, drive_id, s3_key):
        """Stream Drive file directly to S3."""
        download_url = f"https://drive.google.com/uc?id={drive_id}&export=download"
        
        try:
            print(f"  📁 Streaming Drive to S3: {s3_key}")
            
            response = requests.get(download_url, stream=True)
            response.raise_for_status()
            
            # For large files, use multipart upload
            file_obj = BytesIO()
            
            for chunk in response.iter_content(chunk_size=1024*1024):  # 1MB chunks
                if chunk:
                    file_obj.write(chunk)
            
            file_obj.seek(0)
            
            self.s3.upload_fileobj(
                file_obj,
                self.bucket,
                s3_key,
                ExtraArgs={'ContentType': 'application/octet-stream'}
            )
            
            print(f"     ✅ Streamed to S3")
            return True, f"https://{self.bucket}.s3.amazonaws.com/{s3_key}"
            
        except Exception as e:
            print(f"     ❌ Stream failed: {str(e)}")
            return False, str(e)
    
    def process_one_person(self, row):
        """Process downloads for one person directly to S3."""
        row_id = row['row_id']
        person_name = row['name'].replace(' ', '_')
        
        print(f"\n📤 Direct streaming for {row['name']} (Row {row_id})")
        
        s3_urls = []
        
        # Process YouTube
        youtube_links = str(row.get('youtube_playlist', '')).split('|') if pd.notna(row.get('youtube_playlist')) else []
        youtube_links = [l.strip() for l in youtube_links if l and l != 'nan' and l.strip()]
        
        for i, url in enumerate(youtube_links):
            s3_key = f"{row_id}/{person_name}/youtube_direct_{i}.webm"
            success, result = self.stream_youtube_to_s3(url, s3_key, person_name)
            if success:
                s3_urls.append(result)
        
        # Process Drive
        drive_links = str(row.get('google_drive', '')).split('|') if pd.notna(row.get('google_drive')) else []
        drive_links = [l.strip() for l in drive_links if l and l != 'nan' and l.strip()]
        
        for i, url in enumerate(drive_links):
            # Extract Drive ID from URL
            if 'drive.google.com' in url:
                if '/d/' in url:
                    drive_id = url.split('/d/')[1].split('/')[0]
                elif 'id=' in url:
                    drive_id = url.split('id=')[1].split('&')[0]
                else:
                    continue
                
                s3_key = f"{row_id}/{person_name}/drive_direct_{i}"
                success, result = self.stream_drive_to_s3(drive_id, s3_key)
                if success:
                    s3_urls.append(result)
        
        return s3_urls

def main():
    """Test direct streaming with 1 person."""
    downloader = DirectS3Downloader()
    
    # Read CSV and find person with smallest files for testing
    df = pd.read_csv(downloader.csv_file)
    
    # Find Austyn Brown (has 1 small YouTube video)
    test_person = df[df['name'] == 'Austyn Brown'].iloc[0]
    
    print("🚀 Testing Direct-to-S3 Streaming")
    print("=" * 50)
    
    s3_urls = downloader.process_one_person(test_person)
    
    print(f"\n✅ Completed! Generated {len(s3_urls)} S3 URLs")
    for url in s3_urls:
        print(f"  {url}")

if __name__ == "__main__":
    main()