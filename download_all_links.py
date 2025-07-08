#!/usr/bin/env python3
"""
Download all extracted links from the most recent 30 people
"""

import os
import sys
import pandas as pd
import subprocess
import re
from pathlib import Path
import json
from datetime import datetime

# Add project root to path
sys.path.append(os.path.dirname(__file__))

class LinkDownloader:
    def __init__(self, output_dir="downloads"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.stats = {
            "total": 0,
            "youtube_success": 0,
            "youtube_failed": 0,
            "drive_success": 0,
            "drive_failed": 0,
            "errors": []
        }
        
    def sanitize_filename(self, name):
        """Create safe filename from person name"""
        # Remove special characters and replace spaces
        safe_name = re.sub(r'[^\w\s-]', '', name)
        safe_name = re.sub(r'[-\s]+', '_', safe_name)
        return safe_name.strip('_')
    
    def download_youtube_video(self, url, person_name, row_id):
        """Download YouTube video using yt-dlp"""
        print(f"\n📥 Downloading YouTube video for {person_name}...")
        
        # Create person-specific directory
        person_dir = self.output_dir / f"{row_id}_{self.sanitize_filename(person_name)}"
        person_dir.mkdir(exist_ok=True)
        
        # Extract video ID for filename
        video_id_match = re.search(r'v=([a-zA-Z0-9_-]{11})', url)
        if video_id_match:
            video_id = video_id_match.group(1)
        else:
            video_id = "unknown"
        
        try:
            # Use yt-dlp to download (audio only to save space/time)
            output_template = str(person_dir / f"youtube_{video_id}.%(ext)s")
            cmd = [
                "yt-dlp",
                "-f", "bestaudio[ext=m4a]/best[ext=mp4]/best",  # Prefer audio, fallback to video
                "-o", output_template,
                "--quiet",
                "--no-warnings",
                "--extract-audio",  # Extract audio only
                "--audio-format", "mp3",
                "--audio-quality", "128K",
                url
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                print(f"   ✅ Downloaded: youtube_{video_id}.mp3")
                self.stats["youtube_success"] += 1
                return True
            else:
                error_msg = result.stderr or result.stdout or "Unknown error"
                print(f"   ❌ Failed: {error_msg[:100]}")
                self.stats["youtube_failed"] += 1
                self.stats["errors"].append({
                    "person": person_name,
                    "url": url,
                    "error": error_msg
                })
                return False
                
        except FileNotFoundError:
            print(f"   ❌ Error: yt-dlp not installed. Install with: pip install yt-dlp")
            self.stats["youtube_failed"] += 1
            return False
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
            self.stats["youtube_failed"] += 1
            self.stats["errors"].append({
                "person": person_name,
                "url": url,
                "error": str(e)
            })
            return False
    
    def download_youtube_playlist(self, url, person_name, row_id):
        """Download YouTube playlist metadata"""
        print(f"\n📥 Downloading YouTube playlist info for {person_name}...")
        
        # Create person-specific directory
        person_dir = self.output_dir / f"{row_id}_{self.sanitize_filename(person_name)}"
        person_dir.mkdir(exist_ok=True)
        
        # Extract playlist ID
        playlist_id_match = re.search(r'list=([a-zA-Z0-9_-]+)', url)
        if playlist_id_match:
            playlist_id = playlist_id_match.group(1)
        else:
            playlist_id = "unknown"
        
        try:
            # Get playlist info without downloading videos
            cmd = [
                "yt-dlp",
                "--flat-playlist",
                "--dump-json",
                "--quiet",
                "--no-warnings",
                url
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0:
                # Save playlist metadata
                playlist_file = person_dir / f"playlist_{playlist_id}_info.json"
                playlist_data = []
                
                for line in result.stdout.strip().split('\n'):
                    if line:
                        try:
                            video_info = json.loads(line)
                            playlist_data.append({
                                "title": video_info.get("title", "Unknown"),
                                "id": video_info.get("id", ""),
                                "url": f"https://www.youtube.com/watch?v={video_info.get('id', '')}"
                            })
                        except:
                            pass
                
                with open(playlist_file, 'w') as f:
                    json.dump({
                        "playlist_id": playlist_id,
                        "playlist_url": url,
                        "video_count": len(playlist_data),
                        "videos": playlist_data,
                        "downloaded_at": datetime.now().isoformat()
                    }, f, indent=2)
                
                print(f"   ✅ Saved playlist info: {len(playlist_data)} videos")
                self.stats["youtube_success"] += 1
                return True
            else:
                print(f"   ❌ Failed to get playlist info")
                self.stats["youtube_failed"] += 1
                return False
                
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
            self.stats["youtube_failed"] += 1
            return False
    
    def download_drive_file(self, url, person_name, row_id):
        """Download Google Drive file using gdown"""
        print(f"\n📥 Downloading Drive file for {person_name}...")
        
        # Create person-specific directory
        person_dir = self.output_dir / f"{row_id}_{self.sanitize_filename(person_name)}"
        person_dir.mkdir(exist_ok=True)
        
        # Extract file ID
        file_id_match = re.search(r'file/d/([a-zA-Z0-9_-]+)', url)
        if file_id_match:
            file_id = file_id_match.group(1)
        else:
            file_id = "unknown"
        
        try:
            # Use gdown to download
            output_path = person_dir / f"drive_file_{file_id}"
            cmd = [
                "gdown",
                f"https://drive.google.com/uc?id={file_id}",
                "-O", str(output_path),
                "--quiet"
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode == 0 and output_path.exists():
                # Try to determine file extension
                import mimetypes
                mime_type = mimetypes.guess_type(str(output_path))[0]
                if mime_type:
                    ext = mimetypes.guess_extension(mime_type)
                    if ext:
                        new_path = output_path.with_suffix(ext)
                        output_path.rename(new_path)
                        output_path = new_path
                
                print(f"   ✅ Downloaded: {output_path.name}")
                self.stats["drive_success"] += 1
                return True
            else:
                print(f"   ❌ Failed: Access denied or file not found")
                self.stats["drive_failed"] += 1
                return False
                
        except FileNotFoundError:
            print(f"   ❌ Error: gdown not installed. Install with: pip install gdown")
            self.stats["drive_failed"] += 1
            return False
        except Exception as e:
            print(f"   ❌ Error: {str(e)}")
            self.stats["drive_failed"] += 1
            return False
    
    def download_drive_folder(self, url, person_name, row_id):
        """Save Drive folder info (downloading folders requires authentication)"""
        print(f"\n📁 Saving Drive folder info for {person_name}...")
        
        # Create person-specific directory
        person_dir = self.output_dir / f"{row_id}_{self.sanitize_filename(person_name)}"
        person_dir.mkdir(exist_ok=True)
        
        # Extract folder ID
        folder_id_match = re.search(r'folders/([a-zA-Z0-9_-]+)', url)
        if folder_id_match:
            folder_id = folder_id_match.group(1)
        else:
            folder_id = "unknown"
        
        # Save folder info
        folder_info_file = person_dir / f"drive_folder_{folder_id}_info.json"
        with open(folder_info_file, 'w') as f:
            json.dump({
                "folder_id": folder_id,
                "folder_url": url,
                "note": "Folder downloading requires Google Drive API authentication",
                "saved_at": datetime.now().isoformat()
            }, f, indent=2)
        
        print(f"   ✅ Saved folder info: {folder_info_file.name}")
        self.stats["drive_success"] += 1
        return True
    
    def download_all(self, csv_file="outputs/output.csv"):
        """Download all links from the CSV file"""
        print("DOWNLOADING ALL LINKS FROM MOST RECENT 30 PEOPLE")
        print("=" * 60)
        
        # Read the CSV
        df = pd.read_csv(csv_file)
        print(f"Processing {len(df)} records from {csv_file}\n")
        
        # Process each person
        for idx, row in df.iterrows():
            # Extract links
            youtube_links = str(row.get('youtube_playlist', '')).split('|') if pd.notna(row.get('youtube_playlist')) else []
            youtube_links = [l.strip() for l in youtube_links if l and l != 'nan' and l.strip()]
            
            drive_links = str(row.get('google_drive', '')).split('|') if pd.notna(row.get('google_drive')) else []
            drive_links = [l.strip() for l in drive_links if l and l != 'nan' and l.strip()]
            
            if youtube_links or drive_links:
                print(f"\n{'='*60}")
                print(f"Processing Row {row['row_id']}: {row['name']}")
                print(f"Links to download: {len(youtube_links)} YouTube, {len(drive_links)} Drive")
                
                # Download YouTube links
                for link in youtube_links:
                    self.stats["total"] += 1
                    if 'playlist' in link:
                        self.download_youtube_playlist(link, row['name'], row['row_id'])
                    else:
                        self.download_youtube_video(link, row['name'], row['row_id'])
                
                # Download Drive links
                for link in drive_links:
                    self.stats["total"] += 1
                    if '/folders/' in link:
                        self.download_drive_folder(link, row['name'], row['row_id'])
                    else:
                        self.download_drive_file(link, row['name'], row['row_id'])
        
        # Print summary
        print(f"\n{'='*60}")
        print("DOWNLOAD SUMMARY")
        print(f"{'='*60}")
        print(f"Total links processed: {self.stats['total']}")
        print(f"YouTube:")
        print(f"  ✅ Success: {self.stats['youtube_success']}")
        print(f"  ❌ Failed: {self.stats['youtube_failed']}")
        print(f"Drive:")
        print(f"  ✅ Success: {self.stats['drive_success']}")
        print(f"  ❌ Failed: {self.stats['drive_failed']}")
        
        if self.stats["errors"]:
            print(f"\n❌ ERRORS ({len(self.stats['errors'])}):")
            for error in self.stats["errors"][:5]:  # Show first 5 errors
                print(f"  - {error['person']}: {error['error'][:100]}")
        
        # Save summary report
        report_file = self.output_dir / "download_report.json"
        with open(report_file, 'w') as f:
            json.dump(self.stats, f, indent=2)
        print(f"\n📊 Full report saved to: {report_file}")

if __name__ == "__main__":
    downloader = LinkDownloader()
    downloader.download_all()