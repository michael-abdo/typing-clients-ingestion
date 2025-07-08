#!/usr/bin/env python3
"""
Download Brandon Donahue and Emilie with improved timeout handling
"""

import subprocess
import json
from pathlib import Path
import pandas as pd
import re
from datetime import datetime
import time

class NoTimeoutDownloader:
    def __init__(self):
        self.downloads_dir = Path("downloads")
        self.downloads_dir.mkdir(exist_ok=True)
        self.stats = {"success": 0, "failed": 0, "skipped": 0}
    
    def sanitize_filename(self, name):
        safe = re.sub(r'[^\w\s-]', '', name)
        safe = re.sub(r'[-\s]+', '_', safe)
        return safe.strip('_')
    
    def download_youtube_robust(self, url, person_name, row_id):
        """Download with multiple retry strategies and no timeout"""
        person_dir = self.downloads_dir / f"{row_id}_{self.sanitize_filename(person_name)}"
        person_dir.mkdir(exist_ok=True)
        
        video_id = re.search(r'v=([a-zA-Z0-9_-]{11})', url)
        if not video_id:
            return False, "Invalid URL"
        
        output_file = person_dir / f"youtube_{video_id.group(1)}.mp3"
        
        if output_file.exists():
            print(f"   ✅ {video_id.group(1)} (already exists)")
            self.stats["skipped"] += 1
            return True, "Already exists"
        
        # Try multiple download strategies
        strategies = [
            {
                "name": "High Quality",
                "cmd": [
                    "yt-dlp", "-f", "bestaudio[ext=m4a]/bestaudio",
                    "-x", "--audio-format", "mp3", "--audio-quality", "128K",
                    "-o", str(output_file), "--no-playlist", "--retries", "5", url
                ]
            },
            {
                "name": "Any Quality",
                "cmd": [
                    "yt-dlp", "-f", "bestaudio/worst",
                    "-x", "--audio-format", "mp3",
                    "-o", str(output_file), "--no-playlist", "--retries", "3", url
                ]
            },
            {
                "name": "Basic Download", 
                "cmd": [
                    "yt-dlp", "-f", "worst", "--extract-audio",
                    "--audio-format", "mp3", "-o", str(output_file), url
                ]
            }
        ]
        
        for i, strategy in enumerate(strategies, 1):
            print(f"   📥 {video_id.group(1)} - Strategy {i} ({strategy['name']})...", end='', flush=True)
            
            try:
                # No timeout - let it complete naturally
                result = subprocess.run(strategy["cmd"], capture_output=True, text=True)
                
                if result.returncode == 0 and output_file.exists():
                    size_mb = output_file.stat().st_size / (1024*1024)
                    print(f" ✅ ({size_mb:.1f} MB)")
                    self.stats["success"] += 1
                    return True, f"Downloaded ({size_mb:.1f} MB)"
                else:
                    print(" ❌")
                    if i < len(strategies):
                        print(f"      Trying next strategy...")
                        time.sleep(2)  # Brief pause between strategies
                    
            except Exception as e:
                print(f" ❌ ({str(e)[:30]})")
                if i < len(strategies):
                    print(f"      Trying next strategy...")
                    time.sleep(2)
        
        self.stats["failed"] += 1
        return False, "All strategies failed"
    
    def save_drive_info_robust(self, url, person_name, row_id):
        """Save Drive info with enhanced metadata"""
        person_dir = self.downloads_dir / f"{row_id}_{self.sanitize_filename(person_name)}"
        person_dir.mkdir(exist_ok=True)
        
        # Determine type and extract ID
        if '/folders/' in url:
            folder_id = re.search(r'folders/([a-zA-Z0-9_-]+)', url)
            if folder_id:
                info_file = person_dir / f"drive_folder_{folder_id.group(1)}_info.json"
                file_id = folder_id.group(1)
                file_type = "folder"
        else:
            file_id_match = re.search(r'file/d/([a-zA-Z0-9_-]+)', url)
            if file_id_match:
                info_file = person_dir / f"drive_file_{file_id_match.group(1)}_info.json"
                file_id = file_id_match.group(1)
                file_type = "file"
            else:
                return False, "Invalid Drive URL"
        
        if info_file.exists():
            print(f"   ✅ Drive {file_type} info (already exists)")
            self.stats["skipped"] += 1
            return True, "Already exists"
        
        # Enhanced info with multiple access URLs
        info_data = {
            "type": f"drive_{file_type}",
            "id": file_id,
            "person": person_name,
            "row_id": row_id,
            "original_url": url,
            "alternative_urls": [
                url,
                f"https://drive.google.com/{'drive/folders' if file_type == 'folder' else 'file/d'}/{file_id}",
                f"https://drive.google.com/{'drive/folders' if file_type == 'folder' else 'file/d'}/{file_id}/view",
                f"https://drive.google.com/uc?id={file_id}" if file_type == 'file' else None
            ],
            "saved_at": datetime.now().isoformat(),
            "download_note": "Use gdown or Google Drive API for actual file download"
        }
        
        # Remove None values
        info_data["alternative_urls"] = [url for url in info_data["alternative_urls"] if url]
        
        with open(info_file, 'w') as f:
            json.dump(info_data, f, indent=2)
        
        print(f"   ✅ Drive {file_type} info saved")
        self.stats["success"] += 1
        return True, "Info saved"
    
    def download_missing_two(self):
        """Download the 2 genuinely missing people"""
        print("DOWNLOADING MISSING PEOPLE (NO TIMEOUT)")
        print("=" * 60)
        
        # Read our data
        df = pd.read_csv('outputs/output.csv')
        
        # Target the specific missing people
        targets = [
            {"name": "Brandon Donahue", "row_id": 485},  # Has YouTube
            {"name": "Brandon Donahue", "row_id": 482},  # Different Brandon
            {"name": "Emilie", "row_id": 484}            # Has Drive folder
        ]
        
        for target in targets:
            person_row = df[df['row_id'] == target['row_id']]
            
            if person_row.empty:
                print(f"❌ Row {target['row_id']} not found in data")
                continue
                
            row = person_row.iloc[0]
            print(f"\n🎯 Row {row['row_id']}: {row['name']}")
            
            # Get links
            youtube_links = str(row.get('youtube_playlist', '')).split('|') if pd.notna(row.get('youtube_playlist')) else []
            youtube_links = [l.strip() for l in youtube_links if l and l != 'nan' and l.strip()]
            
            drive_links = str(row.get('google_drive', '')).split('|') if pd.notna(row.get('google_drive')) else []
            drive_links = [l.strip() for l in drive_links if l and l != 'nan' and l.strip()]
            
            print(f"   📺 {len(youtube_links)} YouTube | 📁 {len(drive_links)} Drive")
            
            # Download YouTube
            for i, link in enumerate(youtube_links, 1):
                print(f"\n   YouTube {i}/{len(youtube_links)}:")
                self.download_youtube_robust(link, row['name'], row['row_id'])
            
            # Save Drive info
            for i, link in enumerate(drive_links, 1):
                print(f"\n   Drive {i}/{len(drive_links)}:")
                self.save_drive_info_robust(link, row['name'], row['row_id'])
        
        # Summary
        print(f"\n{'='*60}")
        print("FINAL RESULTS")
        print(f"{'='*60}")
        print(f"✅ Successful: {self.stats['success']}")
        print(f"⭕ Skipped (already exists): {self.stats['skipped']}")
        print(f"❌ Failed: {self.stats['failed']}")
        
        if self.stats['failed'] == 0:
            print(f"\n🎉 ALL MISSING PEOPLE SUCCESSFULLY PROCESSED!")
        else:
            print(f"\n⚠️  {self.stats['failed']} items still failed")

if __name__ == "__main__":
    downloader = NoTimeoutDownloader()
    downloader.download_missing_two()