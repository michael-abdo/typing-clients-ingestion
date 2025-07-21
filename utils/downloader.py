#!/usr/bin/env python3
"""
Unified Download Manager (DRY Consolidation)

Consolidates all download functionality from:
- download_all_links.py
- download_all_minimal.py
- download_no_timeout.py
- download_missing_people.py
- upload_to_s3.py
- upload_direct_to_s3.py

Provides a single, unified interface for all download operations.
"""

import os
import re
import sys
import json
import time
import subprocess
import requests
import pandas as pd
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional, Tuple, Union
from dataclasses import dataclass, field
from enum import Enum
import mimetypes

# Import existing utilities
try:
    from .config import get_config
    from .download_youtube import download_youtube_with_context
    from .download_drive import download_drive_with_context
    from .row_context import RowContext, DownloadResult
    from .logging_config import get_logger
    from .sanitization import sanitize_error_message
except ImportError:
    # Fallback for direct execution
    from config import get_config
    from download_youtube import download_youtube_with_context
    from download_drive import download_drive_with_context
    from row_context import RowContext, DownloadResult
    from logging_config import get_logger
    from sanitization import sanitize_error_message


class DownloadStrategy(Enum):
    """Download strategy options"""
    AUDIO_ONLY = "audio_only"
    VIDEO_BEST = "video_best"
    PLAYLIST_INFO = "playlist_info"
    DRIVE_FILE = "drive_file"
    DRIVE_FOLDER = "drive_folder"


class RetryStrategy(Enum):
    """Retry strategy options"""
    NO_RETRY = "no_retry"
    BASIC_RETRY = "basic_retry"
    MULTIPLE_STRATEGIES = "multiple_strategies"
    NO_TIMEOUT = "no_timeout"


@dataclass
class DownloadConfig:
    """Configuration for download operations"""
    output_dir: str = None  # Will be set from config
    timeout: int = None  # Will be set from config
    retry_strategy: RetryStrategy = RetryStrategy.BASIC_RETRY
    youtube_strategy: DownloadStrategy = DownloadStrategy.VIDEO_BEST
    youtube_quality: str = None  # Will be set from config
    youtube_format: str = None  # Will be set from config
    create_metadata: bool = True
    show_progress: bool = True
    max_retries: int = None  # Will be set from config
    retry_delay: int = None  # Will be set from config
    
    def __post_init__(self):
        """Initialize default values from config"""
        config = get_config()
        if self.output_dir is None:
            self.output_dir = config.get('paths.downloads_dir', 'downloads')
        if self.timeout is None:
            self.timeout = config.get('timeouts.video_download', 120)
        if self.youtube_quality is None:
            self.youtube_quality = config.get('downloads.youtube.quality', '128K')
        if self.youtube_format is None:
            self.youtube_format = config.get('downloads.youtube.default_format', 'mp4')
        if self.max_retries is None:
            self.max_retries = config.get('retry.max_attempts', 3)
        if self.retry_delay is None:
            self.retry_delay = config.get('retry.base_delay', 2)


@dataclass
class DownloadStats:
    """Statistics for download operations"""
    total_people: int = 0
    people_processed: int = 0
    youtube_success: int = 0
    youtube_failed: int = 0
    drive_success: int = 0
    drive_failed: int = 0
    skipped: int = 0
    errors: List[Dict] = field(default_factory=list)
    downloads: List[Dict] = field(default_factory=list)
    started_at: str = field(default_factory=lambda: datetime.now().isoformat())
    completed_at: Optional[str] = None


class UnifiedDownloader:
    """
    Unified downloader that consolidates all download functionality
    """
    
    def __init__(self, config: Optional[DownloadConfig] = None):
        self.config = config or DownloadConfig()
        self.logger = get_logger(__name__)
        self.stats = DownloadStats()
        self.output_dir = Path(self.config.output_dir)
        self.output_dir.mkdir(exist_ok=True)
        
        # Initialize session for reuse
        self.session = requests.Session()
        
    def sanitize_filename(self, name: str) -> str:
        """Create safe filename from any input"""
        safe = re.sub(r'[^\w\s-]', '', str(name))
        safe = re.sub(r'[-\s]+', '_', safe)
        return safe.strip('_')
    
    def get_person_directory(self, person_name: str, row_id: int) -> Path:
        """Get or create person-specific directory"""
        person_dir = self.output_dir / f"{row_id}_{self.sanitize_filename(person_name)}"
        person_dir.mkdir(exist_ok=True)
        return person_dir
    
    def download_youtube_robust(self, url: str, person_name: str, row_id: int) -> Tuple[bool, str]:
        """Download YouTube content with configurable strategies"""
        person_dir = self.get_person_directory(person_name, row_id)
        
        # Extract video ID
        video_id = self._extract_video_id(url)
        if not video_id:
            return False, "Invalid YouTube URL"
        
        # Check if already exists
        output_file = person_dir / f"youtube_{video_id}.{self.config.youtube_format}"
        if output_file.exists():
            self.stats.skipped += 1
            return True, f"Already exists: {output_file.name}"
        
        # Define download strategies based on configuration
        strategies = self._get_youtube_strategies(url, output_file)
        
        # Try each strategy
        for i, strategy in enumerate(strategies, 1):
            if self.config.show_progress:
                self.logger.info(f"   📥 {video_id} - Strategy {i} ({strategy['name']})...")
            
            try:
                if self.config.retry_strategy == RetryStrategy.NO_TIMEOUT:
                    # No timeout - let it complete naturally
                    result = subprocess.run(strategy["cmd"], capture_output=True, text=True)
                else:
                    # With timeout
                    result = subprocess.run(
                        strategy["cmd"], 
                        capture_output=True, 
                        text=True, 
                        timeout=self.config.timeout
                    )
                
                if result.returncode == 0 and output_file.exists():
                    size_mb = output_file.stat().st_size / (1024*1024)
                    self.stats.youtube_success += 1
                    
                    # Save metadata if requested
                    if self.config.create_metadata:
                        self._save_youtube_metadata(video_id, url, person_name, row_id, output_file)
                    
                    return True, f"Downloaded ({size_mb:.1f} MB)"
                else:
                    if i < len(strategies):
                        time.sleep(self.config.retry_delay)
                    
            except subprocess.TimeoutExpired:
                if self.config.show_progress:
                    self.logger.warning(f"Strategy {i} timed out")
                if i < len(strategies):
                    time.sleep(self.config.retry_delay)
            except Exception as e:
                if self.config.show_progress:
                    self.logger.warning(f"Strategy {i} failed: {str(e)[:30]}")
                if i < len(strategies):
                    time.sleep(self.config.retry_delay)
        
        self.stats.youtube_failed += 1
        return False, "All strategies failed"
    
    def _extract_video_id(self, url: str) -> Optional[str]:
        """Extract YouTube video ID from URL"""
        patterns = [
            r'v=([a-zA-Z0-9_-]{11})',
            r'youtu\.be/([a-zA-Z0-9_-]{11})',
            r'embed/([a-zA-Z0-9_-]{11})'
        ]
        
        for pattern in patterns:
            match = re.search(pattern, url)
            if match:
                return match.group(1)
        return None
    
    def _get_youtube_strategies(self, url: str, output_file: Path) -> List[Dict]:
        """Get YouTube download strategies based on configuration"""
        base_cmd = ["yt-dlp", "-o", str(output_file), "--no-playlist"]
        
        if self.config.youtube_strategy == DownloadStrategy.AUDIO_ONLY:
            strategies = [
                {
                    "name": "High Quality Audio",
                    "cmd": base_cmd + [
                        "-f", "bestaudio[ext=m4a]/bestaudio",
                        "--extract-audio",
                        "--audio-format", self.config.youtube_format,
                        "--audio-quality", self.config.youtube_quality,
                        "--retries", str(self.config.max_retries),
                        url
                    ]
                },
                {
                    "name": "Any Quality Audio",
                    "cmd": base_cmd + [
                        "-f", "bestaudio/worst",
                        "-x", "--audio-format", self.config.youtube_format,
                        "--retries", "3",
                        url
                    ]
                }
            ]
        else:
            strategies = [
                {
                    "name": "Best Video",
                    "cmd": base_cmd + [
                        "-f", "best[ext=mp4]/best",
                        "--retries", str(self.config.max_retries),
                        url
                    ]
                }
            ]
        
        if self.config.retry_strategy == RetryStrategy.MULTIPLE_STRATEGIES:
            # Add fallback strategy
            strategies.append({
                "name": "Basic Download",
                "cmd": base_cmd + [
                    "-f", "worst",
                    "--extract-audio" if self.config.youtube_strategy == DownloadStrategy.AUDIO_ONLY else "",
                    "--audio-format", self.config.youtube_format,
                    url
                ]
            })
            # Remove empty strings
            strategies[-1]["cmd"] = [cmd for cmd in strategies[-1]["cmd"] if cmd]
        
        return strategies
    
    def _save_youtube_metadata(self, video_id: str, url: str, person_name: str, row_id: int, output_file: Path):
        """Save YouTube download metadata"""
        person_dir = self.get_person_directory(person_name, row_id)
        metadata_file = person_dir / f"youtube_{video_id}_metadata.json"
        
        metadata = {
            "type": "youtube_video",
            "video_id": video_id,
            "url": url,
            "person": person_name,
            "row_id": row_id,
            "downloaded_file": output_file.name,
            "file_size_bytes": output_file.stat().st_size,
            "downloaded_at": datetime.now().isoformat(),
            "strategy": self.config.youtube_strategy.value,
            "format": self.config.youtube_format,
            "quality": self.config.youtube_quality
        }
        
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
    
    def save_drive_info(self, url: str, person_name: str, row_id: int) -> Tuple[bool, str]:
        """Save Google Drive file/folder information"""
        person_dir = self.get_person_directory(person_name, row_id)
        
        # Determine if file or folder
        is_folder = '/folders/' in url
        
        if is_folder:
            folder_id = re.search(r'folders/([a-zA-Z0-9_-]+)', url)
            if folder_id:
                info_file = person_dir / f"drive_folder_{folder_id.group(1)}_info.json"
                file_id = folder_id.group(1)
                file_type = "folder"
            else:
                return False, "Invalid folder URL"
        else:
            file_id_match = re.search(r'/d/([a-zA-Z0-9_-]+)', url)
            if file_id_match:
                info_file = person_dir / f"drive_file_{file_id_match.group(1)}_info.json"
                file_id = file_id_match.group(1)
                file_type = "file"
            else:
                return False, "Invalid Drive URL"
        
        # Skip if already exists
        if info_file.exists():
            self.stats.skipped += 1
            return True, f"Already exists: {info_file.name}"
        
        # Create comprehensive metadata
        metadata = {
            "type": f"drive_{file_type}",
            "id": file_id,
            "person": person_name,
            "row_id": row_id,
            "original_url": url,
            "alternative_urls": [
                url,
                f"https://drive.google.com/{'drive/folders' if is_folder else 'file/d'}/{file_id}",
                f"https://drive.google.com/{'drive/folders' if is_folder else 'file/d'}/{file_id}/view"
            ],
            "saved_at": datetime.now().isoformat(),
            "download_note": "Use gdown or Google Drive API for actual file download"
        }
        
        if not is_folder:
            metadata["alternative_urls"].append(f"https://drive.google.com/uc?id={file_id}")
        
        with open(info_file, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        # For files, attempt actual download
        if file_type == "file":
            return self.download_drive_file_actual(url, person_name, row_id)
        
        self.stats.drive_success += 1
        return True, f"Drive {file_type} info saved"
    
    def download_drive_file_actual(self, url: str, person_name: str, row_id: int) -> Tuple[bool, str]:
        """Download actual Google Drive file using gdown"""
        person_dir = self.get_person_directory(person_name, row_id)
        
        # Extract file ID
        file_id_match = re.search(r'/d/([a-zA-Z0-9_-]+)', url)
        if not file_id_match:
            return False, "Invalid Drive URL"
        
        file_id = file_id_match.group(1)
        
        # Check if any file with this ID already exists
        existing_files = list(person_dir.glob(f"*{file_id}*"))
        if existing_files:
            actual_files = [f for f in existing_files if not f.name.endswith(('.json', '_info.json', '_metadata.json'))]
            if actual_files:
                self.stats.skipped += 1
                return True, f"Already exists: {actual_files[0].name}"
        
        try:
            import gdown
            
            self.logger.info(f"      📥 Downloading Drive file {file_id}...")
            
            # Use gdown library directly
            gdown_url = f"https://drive.google.com/uc?id={file_id}"
            
            # Download to person directory (gdown will determine filename)
            output = gdown.download(gdown_url, output=str(person_dir) + '/', quiet=False, fuzzy=True)
            
            if output and Path(output).exists():
                output_file = Path(output)
                size_mb = output_file.stat().st_size / (1024*1024)
                self.stats.drive_success += 1
                
                # Save metadata if requested
                if self.config.create_metadata:
                    self._save_drive_metadata(file_id, url, person_name, row_id, output_file)
                
                self.logger.info(f"      ✅ Downloaded: {output_file.name} ({size_mb:.1f} MB)")
                return True, f"Downloaded: {output_file.name} ({size_mb:.1f} MB)"
            else:
                self.stats.drive_failed += 1
                return False, "Download failed - file may be private or deleted"
                
        except Exception as e:
            self.stats.drive_failed += 1
            error_msg = str(e)
            if "Access denied" in error_msg or "403" in error_msg:
                return False, "Access denied - file requires authentication"
            elif "404" in error_msg:
                return False, "File not found or deleted"
            else:
                return False, f"Download error: {error_msg}"
    
    def _save_drive_metadata(self, file_id: str, url: str, person_name: str, row_id: int, output_file: Path):
        """Save Drive download metadata"""
        person_dir = self.get_person_directory(person_name, row_id)
        metadata_file = person_dir / f"drive_file_{file_id}_metadata.json"
        
        metadata = {
            "type": "drive_file",
            "file_id": file_id,
            "url": url,
            "person": person_name,
            "row_id": row_id,
            "downloaded_file": output_file.name,
            "file_size_bytes": output_file.stat().st_size,
            "downloaded_at": datetime.now().isoformat(),
            "download_method": "gdown"
        }
        
        with open(metadata_file, 'w') as f:
            json.dump(metadata, f, indent=2)
    
    def process_person(self, row: pd.Series, download_files: bool = False) -> Dict:
        """Process downloads for a single person"""
        row_id = row['row_id']
        person_name = row['name']
        
        if self.config.show_progress:
            self.logger.info(f"\n{'='*70}")
            self.logger.info(f"Row {row_id}: {person_name}")
        
        # Extract links
        youtube_links = self._extract_links(row, 'youtube_playlist')
        drive_links = self._extract_links(row, 'google_drive')
        
        if not youtube_links and not drive_links:
            return {"person": person_name, "row_id": row_id, "status": "no_links"}
        
        self.stats.people_processed += 1
        
        if self.config.show_progress:
            self.logger.info(f"Links: {len(youtube_links)} YouTube, {len(drive_links)} Drive")
        
        results = {
            "person": person_name,
            "row_id": row_id,
            "youtube_results": [],
            "drive_results": []
        }
        
        # Process YouTube links
        for i, link in enumerate(youtube_links, 1):
            if self.config.show_progress:
                self.logger.info(f"\n  📥 YouTube {i}/{len(youtube_links)}: {link[:60]}...")
            
            success, message = self.download_youtube_robust(link, person_name, row_id)
            results["youtube_results"].append({
                "url": link,
                "success": success,
                "message": message
            })
            
            if self.config.show_progress:
                status = "✅" if success else "❌"
                self.logger.info(f"     {status} {message}")
        
        # Process Drive links
        for i, link in enumerate(drive_links, 1):
            if self.config.show_progress:
                self.logger.info(f"\n  📁 Drive {i}/{len(drive_links)}: {link[:60]}...")
            
            if download_files:
                success, message = self.download_drive_file_actual(link, person_name, row_id)
            else:
                success, message = self.save_drive_info(link, person_name, row_id)
            
            results["drive_results"].append({
                "url": link,
                "success": success,
                "message": message
            })
            
            if self.config.show_progress:
                status = "✅" if success else "❌"
                self.logger.info(f"     {status} {message}")
        
        return results
    
    def _extract_links(self, row: pd.Series, column: str) -> List[str]:
        """Extract links from CSV row"""
        links = str(row.get(column, '')).split('|') if pd.notna(row.get(column)) else []
        return [l.strip() for l in links if l and l != 'nan' and l.strip()]
    
    def process_people_from_database(self, db_manager, target_rows: Optional[List[int]] = None,
                                   max_rows: Optional[int] = None) -> Dict:
        """Process people directly from database"""
        self.logger.info("🚀 Starting Unified Download Process (DATABASE MODE)")
        self.logger.info(f"📁 Output Directory: {self.output_dir}")
        self.logger.info(f"🎯 Strategy: {self.config.youtube_strategy.value}")
        self.logger.info(f"🔄 Retry: {self.config.retry_strategy.value}")
        self.logger.info("=" * 70)
        
        all_results = []
        
        if target_rows:
            # Process specific rows
            self.logger.info(f"🎯 Processing {len(target_rows)} targeted people")
            for row_id in target_rows:
                person = db_manager.get_person_by_row_id(row_id)
                if person:
                    # Get files for this person to extract links
                    files = db_manager.get_person_files(row_id)
                    
                    # Convert to pandas Series for compatibility
                    row_data = pd.Series({
                        'row_id': person['row_id'],
                        'name': person['name'],
                        'email': person.get('email', ''),
                        'type': person.get('type', ''),
                        'link': '',  # Link extraction from files needed
                        'youtube_playlist': '',  # Need to extract from files
                        'google_drive': ''  # Need to extract from files
                    })
                    
                    # Extract links from file metadata
                    youtube_links = []
                    drive_links = []
                    for file in files:
                        if file.get('metadata'):
                            source_url = file['metadata'].get('source_url', '')
                            if 'youtube' in source_url or 'youtu.be' in source_url:
                                youtube_links.append(source_url)
                            elif 'drive.google' in source_url or 'docs.google' in source_url:
                                drive_links.append(source_url)
                    
                    row_data['youtube_playlist'] = '|'.join(youtube_links)
                    row_data['google_drive'] = '|'.join(drive_links)
                    
                    result = self.process_person(row_data, download_files=False)
                    all_results.append(result)
                    self.stats.downloads.append(result)
                else:
                    self.logger.warning(f"Person with row_id {row_id} not found in database")
        else:
            # Process all people
            people_df = db_manager.get_all_people()
            self.stats.total_people = len(people_df)
            
            if max_rows:
                people_df = people_df.head(max_rows)
                self.logger.info(f"🎯 Processing first {max_rows} people")
            
            for idx, person in people_df.iterrows():
                # Get files for this person
                files = db_manager.get_person_files(int(person['row_id']))
                
                # Convert to pandas Series for compatibility
                row_data = pd.Series({
                    'row_id': person['row_id'],
                    'name': person['name'],
                    'email': person.get('email', ''),
                    'type': person.get('type', ''),
                    'link': '',
                    'youtube_playlist': '',
                    'google_drive': ''
                })
                
                # Extract links from file metadata
                youtube_links = []
                drive_links = []
                for file in files:
                    if file.get('metadata'):
                        source_url = file['metadata'].get('source_url', '')
                        if 'youtube' in source_url or 'youtu.be' in source_url:
                            youtube_links.append(source_url)
                        elif 'drive.google' in source_url or 'docs.google' in source_url:
                            drive_links.append(source_url)
                
                row_data['youtube_playlist'] = '|'.join(youtube_links)
                row_data['google_drive'] = '|'.join(drive_links)
                
                result = self.process_person(row_data, download_files=False)
                all_results.append(result)
                self.stats.downloads.append(result)
        
        # Complete stats
        self.stats.completed_at = datetime.now().isoformat()
        
        # Save report
        self._save_report()
        
        # Print summary
        self._print_summary()
        
        return {
            "stats": self.stats,
            "results": all_results
        }
    
    def process_csv(self, csv_file: str = None, 
                   target_rows: Optional[List[int]] = None,
                   download_files: bool = False) -> Dict:
        """Process all people from CSV file"""
        if csv_file is None:
            csv_file = get_config().get('paths.output_csv', 'outputs/output.csv')
            
        self.logger.info("🚀 Starting Unified Download Process")
        self.logger.info(f"📁 Output Directory: {self.output_dir}")
        self.logger.info(f"🎯 Strategy: {self.config.youtube_strategy.value}")
        self.logger.info(f"🔄 Retry: {self.config.retry_strategy.value}")
        self.logger.info("=" * 70)
        
        # Read CSV
        df = pd.read_csv(csv_file)
        self.stats.total_people = len(df)
        
        # Filter to target rows if specified
        if target_rows:
            df = df[df['row_id'].isin(target_rows)]
            self.logger.info(f"🎯 Processing {len(df)} targeted people")
        
        # Process each person
        all_results = []
        for idx, row in df.iterrows():
            result = self.process_person(row, download_files)
            all_results.append(result)
            
            # Track in stats
            self.stats.downloads.append(result)
        
        # Complete stats
        self.stats.completed_at = datetime.now().isoformat()
        
        # Save report
        self._save_report()
        
        # Print summary
        self._print_summary()
        
        return {
            "stats": self.stats,
            "results": all_results
        }
    
    def _save_report(self):
        """Save comprehensive download report"""
        report_file = self.output_dir / "unified_download_report.json"
        
        report_data = {
            "config": {
                "output_dir": str(self.output_dir),
                "timeout": self.config.timeout,
                "retry_strategy": self.config.retry_strategy.value,
                "youtube_strategy": self.config.youtube_strategy.value,
                "youtube_quality": self.config.youtube_quality,
                "youtube_format": self.config.youtube_format,
                "create_metadata": self.config.create_metadata
            },
            "stats": {
                "started_at": self.stats.started_at,
                "completed_at": self.stats.completed_at,
                "total_people": self.stats.total_people,
                "people_processed": self.stats.people_processed,
                "youtube_success": self.stats.youtube_success,
                "youtube_failed": self.stats.youtube_failed,
                "drive_success": self.stats.drive_success,
                "drive_failed": self.stats.drive_failed,
                "skipped": self.stats.skipped
            },
            "downloads": self.stats.downloads
        }
        
        with open(report_file, 'w') as f:
            json.dump(report_data, f, indent=2)
        
        self.logger.info(f"\n📊 Report saved to: {report_file}")
    
    def _print_summary(self):
        """Print download summary"""
        self.logger.info(f"\n{'='*70}")
        self.logger.info("📊 DOWNLOAD SUMMARY")
        self.logger.info(f"{'='*70}")
        self.logger.info(f"Total people: {self.stats.total_people}")
        self.logger.info(f"People processed: {self.stats.people_processed}")
        self.logger.info(f"\nYouTube:")
        self.logger.info(f"  ✅ Success: {self.stats.youtube_success}")
        self.logger.info(f"  ❌ Failed: {self.stats.youtube_failed}")
        self.logger.info(f"\nGoogle Drive:")
        self.logger.info(f"  ✅ Success: {self.stats.drive_success}")
        self.logger.info(f"  ❌ Failed: {self.stats.drive_failed}")
        self.logger.info(f"\n⭕ Skipped (already exists): {self.stats.skipped}")
        
        # Calculate success rate
        total_attempts = (self.stats.youtube_success + self.stats.youtube_failed + 
                         self.stats.drive_success + self.stats.drive_failed)
        if total_attempts > 0:
            success_rate = ((self.stats.youtube_success + self.stats.drive_success) / total_attempts) * 100
            self.logger.info(f"\n🎯 Success Rate: {success_rate:.1f}%")


def create_audio_downloader(output_dir: str = None) -> UnifiedDownloader:
    """Create downloader configured for audio-only downloads"""
    config = DownloadConfig(
        output_dir=output_dir,
        youtube_strategy=DownloadStrategy.VIDEO_BEST,
        retry_strategy=RetryStrategy.BASIC_RETRY
    )
    return UnifiedDownloader(config)


def create_robust_downloader(output_dir: str = None) -> UnifiedDownloader:
    """Create downloader configured for robust downloads with multiple strategies"""
    config = DownloadConfig(
        output_dir=output_dir,
        youtube_strategy=DownloadStrategy.VIDEO_BEST,
        timeout=0,  # No timeout
        retry_strategy=RetryStrategy.MULTIPLE_STRATEGIES,
        max_retries=5
    )
    return UnifiedDownloader(config)


def create_targeted_downloader(output_dir: str = None) -> UnifiedDownloader:
    """Create downloader configured for targeted downloads"""
    config = DownloadConfig(
        output_dir=output_dir,
        youtube_strategy=DownloadStrategy.VIDEO_BEST,
        retry_strategy=RetryStrategy.BASIC_RETRY,
        show_progress=True
    )
    return UnifiedDownloader(config)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Unified Download Manager')
    parser.add_argument('--csv', default=None, help='CSV file to process (default from config)')
    parser.add_argument('--output-dir', default=None, help='Output directory (default from config)')
    parser.add_argument('--strategy', choices=['audio', 'robust', 'targeted'], 
                       default='audio', help='Download strategy')
    parser.add_argument('--target-rows', nargs='+', type=int, help='Specific row IDs to process')
    parser.add_argument('--download-files', action='store_true', 
                       help='Download actual Drive files (not just info)')
    
    args = parser.parse_args()
    
    # Create downloader based on strategy
    if args.strategy == 'audio':
        downloader = create_audio_downloader(args.output_dir)
    elif args.strategy == 'robust':
        downloader = create_robust_downloader(args.output_dir)
    else:
        downloader = create_targeted_downloader(args.output_dir)
    
    # Process downloads
    results = downloader.process_csv(
        csv_file=args.csv,
        target_rows=args.target_rows,
        download_files=args.download_files
    )