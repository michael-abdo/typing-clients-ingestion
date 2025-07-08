#!/usr/bin/env python3
"""
Download content for the missing 12 people
"""

import os
import subprocess
import json
from pathlib import Path
import pandas as pd
import re
from datetime import datetime

class MissingPeopleDownloader:
    def __init__(self):
        self.downloads_dir = Path("downloads")
        self.downloads_dir.mkdir(exist_ok=True)
        
        # List of missing people row IDs
        self.missing_rows = [486, 485, 484, 483, 482, 481, 480, 477, 476, 474, 473, 472]
        
        self.stats = {
            "youtube_success": 0,
            "youtube_failed": 0,
            "drive_saved": 0,
            "people_processed": 0
        }
    
    def sanitize_filename(self, name):
        """Create safe filename"""
        safe = re.sub(r'[^\w\s-]', '', name)
        safe = re.sub(r'[-\s]+', '_', safe)
        return safe.strip('_')
    
    def download_youtube(self, url, person_name, row_id):
        """Download YouTube video as MP3"""
        person_dir = self.downloads_dir / f"{row_id}_{self.sanitize_filename(person_name)}"
        person_dir.mkdir(exist_ok=True)
        
        # Extract video ID
        video_id = re.search(r'v=([a-zA-Z0-9_-]{11})', url)
        if not video_id:
            return False, "Invalid video URL"
        
        output_file = person_dir / f"youtube_{video_id.group(1)}.mp3"
        
        # Skip if already exists
        if output_file.exists():
            return True, "Already downloaded"
        
        cmd = [
            "yt-dlp",
            "-f", "bestaudio",
            "-x",  # Extract audio
            "--audio-format", "mp3",
            "--audio-quality", "128K",
            "-o", str(output_file),
            "--no-playlist",
            url
        ]
        
        try:
            print(f"      Downloading {video_id.group(1)}...", end='', flush=True)
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
            
            if result.returncode == 0 and output_file.exists():
                self.stats["youtube_success"] += 1
                print(" ✅")
                return True, "Downloaded"
            else:
                self.stats["youtube_failed"] += 1
                print(" ❌")
                return False, "Download failed"
                
        except subprocess.TimeoutExpired:
            self.stats["youtube_failed"] += 1
            print(" ❌ (timeout)")
            return False, "Timeout"
        except Exception as e:
            self.stats["youtube_failed"] += 1
            print(f" ❌ ({str(e)[:30]})")
            return False, str(e)
    
    def save_drive_info(self, url, person_name, row_id):
        """Save Drive link info"""
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
        
        # Skip if already exists
        if info_file.exists():
            return True, "Already saved"
        
        # Save info
        with open(info_file, 'w') as f:
            json.dump({
                "type": f"drive_{file_type}",
                "url": url,
                "id": file_id,
                "person": person_name,
                "row_id": row_id,
                "saved_at": datetime.now().isoformat()
            }, f, indent=2)
        
        self.stats["drive_saved"] += 1
        print(f"      ✅ Saved {file_type} info")
        return True, "Info saved"
    
    def process_missing_people(self):
        """Process only the missing people"""
        print("DOWNLOADING CONTENT FOR MISSING PEOPLE")
        print("=" * 70)
        
        # Read CSV
        df = pd.read_csv('outputs/output.csv')
        
        # Filter to only missing people
        missing_df = df[df['row_id'].isin(self.missing_rows)]
        
        print(f"Processing {len(missing_df)} missing people...")
        print(f"Focus on: Kaioxys DarkMacro (6 videos) and Joseph Cotroneo (2 videos)\n")
        
        # Sort by number of YouTube links (descending) to prioritize
        def count_youtube(row):
            links = str(row.get('youtube_playlist', '')).split('|') if pd.notna(row.get('youtube_playlist')) else []
            return len([l for l in links if l and l != 'nan'])
        
        missing_df['youtube_count'] = missing_df.apply(count_youtube, axis=1)
        missing_df = missing_df.sort_values('youtube_count', ascending=False)
        
        # Process each person
        for idx, row in missing_df.iterrows():
            # Get links
            youtube_links = str(row.get('youtube_playlist', '')).split('|') if pd.notna(row.get('youtube_playlist')) else []
            youtube_links = [l.strip() for l in youtube_links if l and l != 'nan' and l.strip()]
            
            drive_links = str(row.get('google_drive', '')).split('|') if pd.notna(row.get('google_drive')) else []
            drive_links = [l.strip() for l in drive_links if l and l != 'nan' and l.strip()]
            
            if youtube_links or drive_links:
                print(f"\n{'='*70}")
                print(f"Row {row['row_id']}: {row['name']}")
                print(f"   📺 {len(youtube_links)} YouTube | 📁 {len(drive_links)} Drive")
                
                self.stats["people_processed"] += 1
                
                # Download YouTube
                if youtube_links:
                    print(f"\n   YouTube downloads:")
                    for link in youtube_links:
                        self.download_youtube(link, row['name'], row['row_id'])
                
                # Save Drive info
                if drive_links:
                    print(f"\n   Drive files:")
                    for link in drive_links:
                        self.save_drive_info(link, row['name'], row['row_id'])
        
        # Summary
        print(f"\n{'='*70}")
        print("SUMMARY")
        print(f"{'='*70}")
        print(f"People processed: {self.stats['people_processed']}")
        print(f"YouTube downloads: ✅ {self.stats['youtube_success']} | ❌ {self.stats['youtube_failed']}")
        print(f"Drive info saved: {self.stats['drive_saved']}")
        
        # Check if we got the priority targets
        print(f"\n🎯 Priority targets:")
        
        # Check Kaioxys
        kaioxys_dir = self.downloads_dir / "472_Kaioxys_DarkMacro"
        if kaioxys_dir.exists():
            videos = list(kaioxys_dir.glob("youtube_*.mp3"))
            print(f"   Kaioxys DarkMacro: {len(videos)}/6 videos downloaded")
        else:
            print(f"   Kaioxys DarkMacro: Not processed")
        
        # Check Joseph
        joseph_dir = self.downloads_dir / "481_Joseph_Cotroneo"
        if joseph_dir.exists():
            videos = list(joseph_dir.glob("youtube_*.mp3"))
            print(f"   Joseph Cotroneo: {len(videos)}/2 videos downloaded")
        else:
            print(f"   Joseph Cotroneo: Not processed")

if __name__ == "__main__":
    downloader = MissingPeopleDownloader()
    downloader.process_missing_people()