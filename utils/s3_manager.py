#!/usr/bin/env python3
"""
Unified S3 Manager (DRY Consolidation)

Consolidates S3 upload functionality from:
- upload_to_s3.py
- upload_direct_to_s3.py

Provides both local-then-upload and direct-streaming capabilities.
"""

import os
import subprocess
import requests
import boto3
import pandas as pd
from pathlib import Path
from io import BytesIO
import tempfile
import json
import re
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Union
from dataclasses import dataclass
from enum import Enum
import mimetypes

# Import existing utilities
try:
    from .config import get_config
    from .logging_config import get_logger
    from .sanitization import sanitize_error_message
except ImportError:
    # Fallback for direct execution
    from config import get_config
    from logging_config import get_logger
    from sanitization import sanitize_error_message


class UploadMode(Enum):
    """S3 upload mode options"""
    LOCAL_THEN_UPLOAD = "local_then_upload"
    DIRECT_STREAMING = "direct_streaming"
    HYBRID = "hybrid"


@dataclass
class S3Config:
    """Configuration for S3 operations"""
    bucket_name: str = 'typing-clients-storage-2025'
    region: str = 'us-east-1'
    upload_mode: UploadMode = UploadMode.LOCAL_THEN_UPLOAD
    organize_by_person: bool = True
    add_metadata: bool = True
    update_csv: bool = True
    csv_file: str = 'outputs/output.csv'
    downloads_dir: str = 'downloads'
    create_public_urls: bool = True


@dataclass
class UploadResult:
    """Result of an S3 upload operation"""
    success: bool
    s3_key: str
    s3_url: Optional[str] = None
    error: Optional[str] = None
    file_size: Optional[int] = None
    upload_time: Optional[float] = None


class UnifiedS3Manager:
    """
    Unified S3 manager that handles both local-then-upload and direct streaming
    """
    
    def __init__(self, config: Optional[S3Config] = None):
        self.config = config or S3Config()
        self.logger = get_logger(__name__)
        
        # Initialize S3 client
        self.s3_client = boto3.client('s3', region_name=self.config.region)
        
        # Initialize paths
        self.downloads_dir = Path(self.config.downloads_dir)
        self.csv_file = self.config.csv_file
        
        # Upload tracking
        self.upload_report = {
            "started_at": datetime.now().isoformat(),
            "bucket": self.config.bucket_name,
            "mode": self.config.upload_mode.value,
            "uploads": [],
            "errors": []
        }
    
    def get_content_type(self, file_path: str) -> str:
        """Get MIME type for file"""
        content_type, _ = mimetypes.guess_type(file_path)
        if content_type:
            return content_type
        
        # Default types for common files
        file_ext = Path(file_path).suffix.lower()
        ext_mapping = {
            '.mp3': 'audio/mpeg',
            '.mp4': 'video/mp4',
            '.webm': 'video/webm',
            '.m4a': 'audio/mp4',
            '.json': 'application/json',
            '.txt': 'text/plain',
            '.csv': 'text/csv',
            '.pdf': 'application/pdf',
            '.docx': 'application/vnd.openxmlformats-officedocument.wordprocessingml.document',
            '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        }
        
        return ext_mapping.get(file_ext, 'application/octet-stream')
    
    def generate_s3_key(self, row_id: int, person_name: str, filename: str) -> str:
        """Generate S3 key for organized storage"""
        if self.config.organize_by_person:
            # Clean person name for S3 key
            clean_name = re.sub(r'[^\w\s-]', '', person_name)
            clean_name = re.sub(r'[-\s]+', '_', clean_name)
            return f"{row_id}/{clean_name}/{filename}"
        else:
            return f"{row_id}/{filename}"
    
    def upload_file_to_s3(self, local_path: Union[str, Path], s3_key: str, 
                         content_type: Optional[str] = None) -> UploadResult:
        """Upload a local file to S3"""
        local_path = Path(local_path)
        
        if not local_path.exists():
            return UploadResult(success=False, s3_key=s3_key, error="Local file not found")
        
        if not content_type:
            content_type = self.get_content_type(str(local_path))
        
        try:
            start_time = datetime.now()
            
            # Upload with metadata
            extra_args = {'ContentType': content_type}
            if self.config.add_metadata:
                extra_args['Metadata'] = {
                    'uploaded_at': start_time.isoformat(),
                    'source': 'typing-clients-ingestion',
                    'original_filename': local_path.name
                }
            
            self.s3_client.upload_file(
                str(local_path),
                self.config.bucket_name,
                s3_key,
                ExtraArgs=extra_args
            )
            
            upload_time = (datetime.now() - start_time).total_seconds()
            file_size = local_path.stat().st_size
            
            # Generate public URL if requested
            s3_url = None
            if self.config.create_public_urls:
                s3_url = f"https://{self.config.bucket_name}.s3.amazonaws.com/{s3_key}"
            
            return UploadResult(
                success=True,
                s3_key=s3_key,
                s3_url=s3_url,
                file_size=file_size,
                upload_time=upload_time
            )
            
        except Exception as e:
            return UploadResult(
                success=False,
                s3_key=s3_key,
                error=sanitize_error_message(str(e))
            )
    
    def stream_youtube_to_s3(self, url: str, s3_key: str, person_name: str) -> UploadResult:
        """Stream YouTube directly to S3 using named pipe"""
        pipe_path = f"/tmp/youtube_{person_name}_{os.getpid()}"
        
        try:
            # Create named pipe
            if os.path.exists(pipe_path):
                os.remove(pipe_path)
            os.mkfifo(pipe_path)
            
            self.logger.info(f"  📥 Streaming YouTube to S3: {s3_key}")
            
            # Start yt-dlp process
            cmd = ["yt-dlp", "-f", "bestaudio", "-o", pipe_path, url]
            process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            
            # Upload from pipe to S3
            start_time = datetime.now()
            
            with open(pipe_path, 'rb') as pipe_file:
                extra_args = {'ContentType': 'video/webm'}
                if self.config.add_metadata:
                    extra_args['Metadata'] = {
                        'uploaded_at': start_time.isoformat(),
                        'source': 'typing-clients-ingestion-youtube-stream',
                        'original_url': url
                    }
                
                self.s3_client.upload_fileobj(
                    pipe_file,
                    self.config.bucket_name,
                    s3_key,
                    ExtraArgs=extra_args
                )
            
            process.wait()
            upload_time = (datetime.now() - start_time).total_seconds()
            
            if process.returncode == 0:
                s3_url = f"https://{self.config.bucket_name}.s3.amazonaws.com/{s3_key}"
                return UploadResult(
                    success=True,
                    s3_key=s3_key,
                    s3_url=s3_url,
                    upload_time=upload_time
                )
            else:
                error = process.stderr.read().decode()
                return UploadResult(
                    success=False,
                    s3_key=s3_key,
                    error=f"yt-dlp failed: {error[:100]}"
                )
                
        except Exception as e:
            return UploadResult(
                success=False,
                s3_key=s3_key,
                error=sanitize_error_message(str(e))
            )
        finally:
            if os.path.exists(pipe_path):
                os.remove(pipe_path)
    
    def stream_drive_to_s3(self, drive_id: str, s3_key: str) -> UploadResult:
        """Stream Drive file directly to S3"""
        download_url = f"https://drive.google.com/uc?id={drive_id}&export=download"
        
        try:
            self.logger.info(f"  📁 Streaming Drive to S3: {s3_key}")
            
            start_time = datetime.now()
            response = requests.get(download_url, stream=True)
            response.raise_for_status()
            
            # Use BytesIO for streaming
            file_obj = BytesIO()
            file_size = 0
            
            for chunk in response.iter_content(chunk_size=1024*1024):  # 1MB chunks
                if chunk:
                    file_obj.write(chunk)
                    file_size += len(chunk)
            
            file_obj.seek(0)
            
            extra_args = {'ContentType': 'application/octet-stream'}
            if self.config.add_metadata:
                extra_args['Metadata'] = {
                    'uploaded_at': start_time.isoformat(),
                    'source': 'typing-clients-ingestion-drive-stream',
                    'drive_id': drive_id
                }
            
            self.s3_client.upload_fileobj(
                file_obj,
                self.config.bucket_name,
                s3_key,
                ExtraArgs=extra_args
            )
            
            upload_time = (datetime.now() - start_time).total_seconds()
            s3_url = f"https://{self.config.bucket_name}.s3.amazonaws.com/{s3_key}"
            
            return UploadResult(
                success=True,
                s3_key=s3_key,
                s3_url=s3_url,
                file_size=file_size,
                upload_time=upload_time
            )
            
        except Exception as e:
            return UploadResult(
                success=False,
                s3_key=s3_key,
                error=sanitize_error_message(str(e))
            )
    
    def upload_local_downloads(self) -> Dict[int, Dict]:
        """Upload all local downloads to S3"""
        self.logger.info("🚀 Starting Local Downloads Upload")
        
        person_s3_data = {}
        
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
            
            self.logger.info(f"\n📤 Uploading files for {person_name} (Row {row_id})")
            
            person_s3_data[row_id] = {
                'youtube_urls': [],
                'drive_urls': [],
                'all_files': []
            }
            
            # Upload each file
            for file_path in person_dir.iterdir():
                if file_path.is_file():
                    s3_key = self.generate_s3_key(row_id, person_name, file_path.name)
                    
                    self.logger.info(f"  📎 Uploading {file_path.name}...")
                    result = self.upload_file_to_s3(file_path, s3_key)
                    
                    if result.success:
                        self.logger.info(f"     ✅ Uploaded to S3")
                        
                        # Categorize by type
                        if 'youtube' in file_path.name:
                            person_s3_data[row_id]['youtube_urls'].append(result.s3_url)
                        elif 'drive' in file_path.name:
                            person_s3_data[row_id]['drive_urls'].append(result.s3_url)
                        
                        person_s3_data[row_id]['all_files'].append(result.s3_url)
                        
                        # Track in report
                        self.upload_report['uploads'].append({
                            'row_id': row_id,
                            'person': person_name,
                            'file': file_path.name,
                            's3_key': s3_key,
                            's3_url': result.s3_url,
                            'size': result.file_size,
                            'upload_time': result.upload_time
                        })
                    else:
                        self.logger.info(f"     ❌ Failed: {result.error}")
                        self.upload_report['errors'].append({
                            'row_id': row_id,
                            'person': person_name,
                            'file': file_path.name,
                            'error': result.error
                        })
        
        return person_s3_data
    
    def process_direct_streaming(self, csv_file: Optional[str] = None) -> Dict[int, List[str]]:
        """Process direct streaming uploads for all people"""
        if not csv_file:
            csv_file = self.config.csv_file
        
        self.logger.info("🚀 Starting Direct Streaming Upload")
        
        df = pd.read_csv(csv_file)
        person_s3_data = {}
        
        for _, row in df.iterrows():
            row_id = row['row_id']
            person_name = row['name'].replace(' ', '_')
            
            self.logger.info(f"\n📤 Direct streaming for {row['name']} (Row {row_id})")
            
            s3_urls = []
            
            # Process YouTube links
            youtube_links = self._extract_links(row, 'youtube_playlist')
            for i, url in enumerate(youtube_links):
                s3_key = self.generate_s3_key(row_id, person_name, f"youtube_direct_{i}.webm")
                result = self.stream_youtube_to_s3(url, s3_key, person_name)
                
                if result.success:
                    s3_urls.append(result.s3_url)
                    self.upload_report['uploads'].append({
                        'row_id': row_id,
                        'person': person_name,
                        'type': 'youtube_stream',
                        's3_key': s3_key,
                        's3_url': result.s3_url,
                        'upload_time': result.upload_time
                    })
                else:
                    self.upload_report['errors'].append({
                        'row_id': row_id,
                        'person': person_name,
                        'type': 'youtube_stream',
                        'error': result.error
                    })
            
            # Process Drive links
            drive_links = self._extract_links(row, 'google_drive')
            for i, url in enumerate(drive_links):
                # Extract Drive ID
                drive_id = self._extract_drive_id(url)
                if drive_id:
                    s3_key = self.generate_s3_key(row_id, person_name, f"drive_direct_{i}")
                    result = self.stream_drive_to_s3(drive_id, s3_key)
                    
                    if result.success:
                        s3_urls.append(result.s3_url)
                        self.upload_report['uploads'].append({
                            'row_id': row_id,
                            'person': person_name,
                            'type': 'drive_stream',
                            's3_key': s3_key,
                            's3_url': result.s3_url,
                            'file_size': result.file_size,
                            'upload_time': result.upload_time
                        })
                    else:
                        self.upload_report['errors'].append({
                            'row_id': row_id,
                            'person': person_name,
                            'type': 'drive_stream',
                            'error': result.error
                        })
            
            person_s3_data[row_id] = s3_urls
        
        return person_s3_data
    
    def _extract_links(self, row: pd.Series, column: str) -> List[str]:
        """Extract links from CSV row"""
        links = str(row.get(column, '')).split('|') if pd.notna(row.get(column)) else []
        return [l.strip() for l in links if l and l != 'nan' and l.strip()]
    
    def _extract_drive_id(self, url: str) -> Optional[str]:
        """Extract Drive ID from URL"""
        patterns = [
            r'/d/([a-zA-Z0-9_-]+)',
            r'id=([a-zA-Z0-9_-]+)'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        return None
    
    def update_csv_with_s3_urls(self, person_s3_data: Dict, csv_file: Optional[str] = None):
        """Update CSV with S3 URLs"""
        if not csv_file:
            csv_file = self.config.csv_file
        
        self.logger.info("\n📊 Updating CSV with S3 URLs...")
        
        # Read existing CSV
        df = pd.read_csv(csv_file)
        
        # Add new columns if they don't exist
        for col in ['s3_youtube_urls', 's3_drive_urls', 's3_all_files']:
            if col not in df.columns:
                df[col] = ''
        
        # Update each row with S3 URLs
        for row_id, urls_data in person_s3_data.items():
            mask = df['row_id'] == row_id
            if mask.any():
                if isinstance(urls_data, dict):
                    # Local upload format
                    df.loc[mask, 's3_youtube_urls'] = '|'.join(urls_data.get('youtube_urls', []))
                    df.loc[mask, 's3_drive_urls'] = '|'.join(urls_data.get('drive_urls', []))
                    df.loc[mask, 's3_all_files'] = '|'.join(urls_data.get('all_files', []))
                else:
                    # Direct streaming format (list of URLs)
                    df.loc[mask, 's3_all_files'] = '|'.join(urls_data)
        
        # Save updated CSV
        df.to_csv(csv_file, index=False)
        self.logger.info("✅ CSV updated with S3 URLs")
        
        return df
    
    def save_report(self, report_file: Optional[str] = None):
        """Save upload report"""
        if not report_file:
            report_file = f's3_upload_report_{self.config.upload_mode.value}.json'
        
        self.upload_report['completed_at'] = datetime.now().isoformat()
        
        with open(report_file, 'w') as f:
            json.dump(self.upload_report, f, indent=2)
        
        self.logger.info(f"\n📄 Upload report saved to {report_file}")
    
    def run_upload_process(self, mode: Optional[UploadMode] = None) -> Dict:
        """Run the complete upload process"""
        if mode:
            self.config.upload_mode = mode
        
        self.logger.info(f"🚀 Starting S3 Upload Process")
        self.logger.info(f"🪣 Bucket: {self.config.bucket_name}")
        self.logger.info(f"🔄 Mode: {self.config.upload_mode.value}")
        self.logger.info("=" * 70)
        
        # Execute based on mode
        if self.config.upload_mode == UploadMode.LOCAL_THEN_UPLOAD:
            person_s3_data = self.upload_local_downloads()
        elif self.config.upload_mode == UploadMode.DIRECT_STREAMING:
            person_s3_data = self.process_direct_streaming()
        else:
            raise ValueError(f"Unsupported upload mode: {self.config.upload_mode}")
        
        # Update CSV if requested
        if self.config.update_csv:
            self.update_csv_with_s3_urls(person_s3_data)
        
        # Save report
        self.save_report()
        
        # Print summary
        self._print_summary()
        
        return person_s3_data
    
    def _print_summary(self):
        """Print upload summary"""
        self.logger.info("\n" + "=" * 70)
        self.logger.info("📊 UPLOAD SUMMARY")
        self.logger.info("=" * 70)
        self.logger.info(f"✅ Files uploaded: {len(self.upload_report['uploads'])}")
        self.logger.info(f"❌ Errors: {len(self.upload_report['errors'])}")
        
        # Calculate total size
        total_size = sum(
            u.get('size', 0) for u in self.upload_report['uploads'] 
            if u.get('size')
        )
        if total_size > 0:
            self.logger.info(f"💾 Total size uploaded: {total_size / (1024**3):.2f} GB")
        
        # Calculate average upload time
        upload_times = [
            u.get('upload_time', 0) for u in self.upload_report['uploads'] 
            if u.get('upload_time')
        ]
        if upload_times:
            avg_time = sum(upload_times) / len(upload_times)
            self.logger.info(f"⏱️ Average upload time: {avg_time:.2f} seconds")


def create_local_uploader(bucket_name: str = 'typing-clients-storage-2025') -> UnifiedS3Manager:
    """Create S3 manager for local-then-upload mode"""
    config = S3Config(
        bucket_name=bucket_name,
        upload_mode=UploadMode.LOCAL_THEN_UPLOAD,
        organize_by_person=True,
        update_csv=True
    )
    return UnifiedS3Manager(config)


def create_streaming_uploader(bucket_name: str = 'typing-clients-storage-2025') -> UnifiedS3Manager:
    """Create S3 manager for direct streaming mode"""
    config = S3Config(
        bucket_name=bucket_name,
        upload_mode=UploadMode.DIRECT_STREAMING,
        organize_by_person=True,
        update_csv=True
    )
    return UnifiedS3Manager(config)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Unified S3 Upload Manager')
    parser.add_argument('--bucket', default='typing-clients-storage-2025', help='S3 bucket name')
    parser.add_argument('--mode', choices=['local', 'streaming'], default='local', help='Upload mode')
    parser.add_argument('--csv', default='outputs/output.csv', help='CSV file to update')
    parser.add_argument('--downloads-dir', default='downloads', help='Downloads directory')
    parser.add_argument('--no-csv-update', action='store_true', help='Skip CSV update')
    
    args = parser.parse_args()
    
    # Create manager based on mode
    if args.mode == 'local':
        manager = create_local_uploader(args.bucket)
    else:
        manager = create_streaming_uploader(args.bucket)
    
    # Override config if needed
    manager.config.csv_file = args.csv
    manager.config.downloads_dir = args.downloads_dir
    manager.config.update_csv = not args.no_csv_update
    
    # Run upload process
    results = manager.run_upload_process()