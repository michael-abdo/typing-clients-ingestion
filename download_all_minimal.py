#!/usr/bin/env python3
"""
Download all links from the most recent 30 people with better error handling
"""

import os
import sys
import pandas as pd
import subprocess
import re
from pathlib import Path
import json
from datetime import datetime
import signal

class TimeoutError(Exception):
    pass

def timeout_handler(signum, frame):
    raise TimeoutError("Download timed out")

class MinimalDownloader:
    def __init__(self, output_dir="downloads"):
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(exist_ok=True)
        self.report = {
            "started_at": datetime.now().isoformat(),
            "stats": {
                "total_people": 0,
                "people_with_links": 0,
                "youtube_success": 0,
                "youtube_failed": 0,
                "drive_saved": 0,
                "drive_attempted": 0
            },
            "downloads": []
        }
    
    def sanitize_filename(self, name):
        """Create safe filename"""
        safe = re.sub(r'[^\w\s-]', '', name)
        safe = re.sub(r'[-\s]+', '_', safe)
        return safe.strip('_')
    
    def download_youtube(self, url, person_name, row_id):
        """Download YouTube content (audio only to save space)"""
        person_dir = self.output_dir / f"{row_id}_{self.sanitize_filename(person_name)}"
        person_dir.mkdir(exist_ok=True)
        
        # Determine if video or playlist
        is_playlist = 'playlist' in url
        
        if is_playlist:
            # Just save playlist info
            playlist_id = re.search(r'list=([a-zA-Z0-9_-]+)', url)
            if playlist_id:
                info_file = person_dir / f"playlist_{playlist_id.group(1)}_info.json"
                with open(info_file, 'w') as f:
                    json.dump({
                        "type": "youtube_playlist",
                        "url": url,
                        "playlist_id": playlist_id.group(1),
                        "person": person_name,
                        "row_id": row_id,
                        "saved_at": datetime.now().isoformat()
                    }, f, indent=2)
                
                self.report["downloads"].append({
                    "person": person_name,
                    "row_id": row_id,
                    "type": "youtube_playlist",
                    "url": url,
                    "status": "info_saved",
                    "file": str(info_file)
                })
                self.report["stats"]["youtube_success"] += 1
                return True, "Playlist info saved"
        else:
            # Download video (audio only)
            video_id = re.search(r'v=([a-zA-Z0-9_-]{11})', url)
            if not video_id:
                return False, "Invalid video URL"
            
            output_file = person_dir / f"youtube_{video_id.group(1)}.%(ext)s"
            
            cmd = [
                "yt-dlp",
                "-f", "bestaudio[ext=m4a]/bestaudio",
                "-o", str(output_file),
                "--extract-audio",
                "--audio-format", "mp3",
                "--audio-quality", "128K",
                "--no-playlist",
                "--quiet",
                "--no-warnings",
                url
            ]
            
            try:
                # Set timeout of 60 seconds
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                
                if result.returncode == 0:
                    # Find the downloaded file
                    downloaded = list(person_dir.glob(f"youtube_{video_id.group(1)}.*"))
                    if downloaded:
                        self.report["downloads"].append({
                            "person": person_name,
                            "row_id": row_id,
                            "type": "youtube_video",
                            "url": url,
                            "status": "downloaded",
                            "file": str(downloaded[0]),
                            "size_kb": downloaded[0].stat().st_size / 1024
                        })
                        self.report["stats"]["youtube_success"] += 1
                        return True, f"Downloaded {downloaded[0].name}"
                    else:
                        self.report["stats"]["youtube_failed"] += 1
                        return False, "Download completed but file not found"
                else:
                    self.report["stats"]["youtube_failed"] += 1
                    error = result.stderr or result.stdout or "Unknown error"
                    return False, error[:100]
                    
            except subprocess.TimeoutExpired:
                self.report["stats"]["youtube_failed"] += 1
                return False, "Download timed out (60s)"
            except Exception as e:
                self.report["stats"]["youtube_failed"] += 1
                return False, str(e)
    
    def save_drive_info(self, url, person_name, row_id):
        """Save Drive link info (most require auth to download)"""
        person_dir = self.output_dir / f"{row_id}_{self.sanitize_filename(person_name)}"
        person_dir.mkdir(exist_ok=True)
        
        # Determine if file or folder
        is_folder = '/folders/' in url
        
        if is_folder:
            folder_id = re.search(r'folders/([a-zA-Z0-9_-]+)', url)
            if folder_id:
                info_file = person_dir / f"drive_folder_{folder_id.group(1)}_info.json"
        else:
            file_id = re.search(r'file/d/([a-zA-Z0-9_-]+)', url)
            if file_id:
                info_file = person_dir / f"drive_file_{file_id.group(1)}_info.json"
            else:
                return False, "Invalid Drive URL"
        
        # Save info
        with open(info_file, 'w') as f:
            json.dump({
                "type": "drive_folder" if is_folder else "drive_file",
                "url": url,
                "id": folder_id.group(1) if is_folder else file_id.group(1),
                "person": person_name,
                "row_id": row_id,
                "saved_at": datetime.now().isoformat(),
                "note": "Google Drive content typically requires authentication to download"
            }, f, indent=2)
        
        self.report["downloads"].append({
            "person": person_name,
            "row_id": row_id,
            "type": "drive_folder" if is_folder else "drive_file",
            "url": url,
            "status": "info_saved",
            "file": str(info_file)
        })
        self.report["stats"]["drive_saved"] += 1
        
        return True, f"Drive info saved"
    
    def process_all(self, csv_file="outputs/output.csv"):
        """Process all people and their links"""
        print("DOWNLOADING CONTENT FROM MOST RECENT 30 PEOPLE")
        print("=" * 70)
        print("NOTE: YouTube videos will be downloaded as audio (MP3)")
        print("      Drive links will have their info saved (auth required for download)")
        print("=" * 70)
        
        # Read CSV
        df = pd.read_csv(csv_file)
        self.report["stats"]["total_people"] = len(df)
        
        # Process each person
        for idx, row in df.iterrows():
            # Get links
            youtube_links = str(row.get('youtube_playlist', '')).split('|') if pd.notna(row.get('youtube_playlist')) else []
            youtube_links = [l.strip() for l in youtube_links if l and l != 'nan' and l.strip()]
            
            drive_links = str(row.get('google_drive', '')).split('|') if pd.notna(row.get('google_drive')) else []
            drive_links = [l.strip() for l in drive_links if l and l != 'nan' and l.strip()]
            
            if youtube_links or drive_links:
                self.report["stats"]["people_with_links"] += 1
                
                print(f"\n{'='*70}")
                print(f"Row {row['row_id']}: {row['name']}")
                print(f"Links: {len(youtube_links)} YouTube, {len(drive_links)} Drive")
                
                # Process YouTube
                for link in youtube_links:
                    print(f"\n  📥 YouTube: {link[:60]}...")
                    success, message = self.download_youtube(link, row['name'], row['row_id'])
                    if success:
                        print(f"     ✅ {message}")
                    else:
                        print(f"     ❌ {message}")
                
                # Process Drive
                for link in drive_links:
                    print(f"\n  📁 Drive: {link[:60]}...")
                    success, message = self.save_drive_info(link, row['name'], row['row_id'])
                    if success:
                        print(f"     ✅ {message}")
                    else:
                        print(f"     ❌ {message}")
        
        # Save report
        self.report["completed_at"] = datetime.now().isoformat()
        report_file = self.output_dir / "download_report.json"
        with open(report_file, 'w') as f:
            json.dump(self.report, f, indent=2)
        
        # Print summary
        print(f"\n{'='*70}")
        print("DOWNLOAD SUMMARY")
        print(f"{'='*70}")
        print(f"Total people: {self.report['stats']['total_people']}")
        print(f"People with links: {self.report['stats']['people_with_links']}")
        print(f"\nYouTube:")
        print(f"  ✅ Downloaded: {self.report['stats']['youtube_success']}")
        print(f"  ❌ Failed: {self.report['stats']['youtube_failed']}")
        print(f"\nGoogle Drive:")
        print(f"  📁 Info saved: {self.report['stats']['drive_saved']}")
        print(f"\n📊 Full report: {report_file}")
        print(f"📁 Downloads directory: {self.output_dir}")

if __name__ == "__main__":
    downloader = MinimalDownloader()
    downloader.process_all()