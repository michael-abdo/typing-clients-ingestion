#!/usr/bin/env python3
import pandas as pd
import subprocess
import re
from pathlib import Path

# Get Joseph's links
df = pd.read_csv('outputs/output.csv')
joseph = df[df['row_id'] == 481]

if not joseph.empty:
    row = joseph.iloc[0]
    print(f"Found: {row['name']} (Row {row['row_id']})")
    
    youtube_links = str(row.get('youtube_playlist', '')).split('|')
    youtube_links = [l.strip() for l in youtube_links if l and l != 'nan' and l.strip()]
    
    print(f"YouTube links: {len(youtube_links)}")
    
    # Create directory
    person_dir = Path("downloads") / "481_Joseph_Cotroneo"
    person_dir.mkdir(exist_ok=True)
    
    # Download each video
    for i, link in enumerate(youtube_links, 1):
        video_id = re.search(r'v=([a-zA-Z0-9_-]{11})', link)
        if video_id:
            output_file = person_dir / f"youtube_{video_id.group(1)}.mp3"
            print(f"\nVideo {i}: {video_id.group(1)}")
            print(f"Link: {link}")
            
            if output_file.exists():
                print("✅ Already exists")
                continue
            
            cmd = ["yt-dlp", "-f", "worst", "-x", "--audio-format", "mp3", "-o", str(output_file), link]
            
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
                if result.returncode == 0 and output_file.exists():
                    print("✅ Downloaded")
                else:
                    print("❌ Failed")
            except:
                print("❌ Error/timeout")
else:
    print("Joseph Cotroneo not found")