#!/usr/bin/env python3
"""
Download just the priority targets: Kaioxys DarkMacro and Joseph Cotroneo
"""

import subprocess
import json
from pathlib import Path
import pandas as pd
import re

# Priority targets
PRIORITY_TARGETS = [
    {"row_id": 472, "name": "Kaioxys DarkMacro", "expected_youtube": 6},
    {"row_id": 481, "name": "Joseph Cotroneo", "expected_youtube": 2}
]

downloads_dir = Path("downloads")
downloads_dir.mkdir(exist_ok=True)

def sanitize_filename(name):
    safe = re.sub(r'[^\w\s-]', '', name)
    safe = re.sub(r'[-\s]+', '_', safe)
    return safe.strip('_')

def download_youtube_fast(url, person_name, row_id):
    """Fast YouTube download"""
    person_dir = downloads_dir / f"{row_id}_{sanitize_filename(person_name)}"
    person_dir.mkdir(exist_ok=True)
    
    video_id = re.search(r'v=([a-zA-Z0-9_-]{11})', url)
    if not video_id:
        return False
    
    output_file = person_dir / f"youtube_{video_id.group(1)}.mp3"
    
    if output_file.exists():
        print(f"   ✅ {video_id.group(1)} (already exists)")
        return True
    
    cmd = [
        "yt-dlp",
        "-f", "worst",  # Fastest download
        "-x", "--audio-format", "mp3",
        "-o", str(output_file),
        "--no-playlist",
        url
    ]
    
    try:
        print(f"   📥 {video_id.group(1)}...", end='', flush=True)
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
        
        if result.returncode == 0 and output_file.exists():
            print(" ✅")
            return True
        else:
            print(" ❌")
            return False
            
    except subprocess.TimeoutExpired:
        print(" ❌ (timeout)")
        return False
    except Exception as e:
        print(f" ❌ ({str(e)[:20]})")
        return False

print("DOWNLOADING PRIORITY TARGETS")
print("=" * 50)

# Read CSV
df = pd.read_csv('outputs/output.csv')

for target in PRIORITY_TARGETS:
    row = df[df['row_id'] == target['row_id']]
    if row.empty:
        print(f"❌ Row {target['row_id']} not found")
        continue
    
    row = row.iloc[0]
    print(f"\n🎯 {target['name']} (Row {target['row_id']})")
    
    # Get YouTube links
    youtube_links = str(row.get('youtube_playlist', '')).split('|') if pd.notna(row.get('youtube_playlist')) else []
    youtube_links = [l.strip() for l in youtube_links if l and l != 'nan' and l.strip()]
    
    print(f"   Expected: {target['expected_youtube']} videos")
    print(f"   Found: {len(youtube_links)} links")
    
    success_count = 0
    for i, link in enumerate(youtube_links, 1):
        print(f"   Video {i}/{len(youtube_links)}:")
        if download_youtube_fast(link, target['name'], target['row_id']):
            success_count += 1
    
    print(f"   📊 Result: {success_count}/{len(youtube_links)} downloaded")

# Final check
print(f"\n{'='*50}")
print("FINAL STATUS")
print(f"{'='*50}")

for target in PRIORITY_TARGETS:
    person_dir = downloads_dir / f"{target['row_id']}_{sanitize_filename(target['name'])}"
    if person_dir.exists():
        videos = list(person_dir.glob("youtube_*.mp3"))
        print(f"✅ {target['name']}: {len(videos)} videos downloaded")
        for video in videos:
            size_mb = video.stat().st_size / (1024*1024)
            print(f"   - {video.name} ({size_mb:.1f} MB)")
    else:
        print(f"❌ {target['name']}: No downloads")