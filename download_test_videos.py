#!/usr/bin/env python3
"""
Download specific test videos to validate our extraction
"""
import os
import subprocess
from pathlib import Path

def download_video(video_id, person_name):
    """Download a single YouTube video"""
    Path("downloads/test_videos").mkdir(parents=True, exist_ok=True)
    
    url = f"https://www.youtube.com/watch?v={video_id}"
    output = f"downloads/test_videos/{person_name}_{video_id}.mp4"
    
    # Skip if already downloaded
    if Path(output).exists():
        print(f"✅ Already downloaded: {output}")
        return True
    
    cmd = [
        'yt-dlp',
        '--quiet',
        '--no-warnings',
        '-f', 'best[height<=480]',  # Lower quality for faster download
        '-o', output,
        url
    ]
    
    print(f"📥 Downloading {person_name} - {video_id}...")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
        if result.returncode == 0:
            print(f"✅ Success: {output}")
            return True
        else:
            print(f"❌ Failed: {result.stderr[:100]}")
            return False
    except subprocess.TimeoutExpired:
        print(f"⏱️ Timeout downloading {video_id}")
        return False
    except Exception as e:
        print(f"❌ Error: {e}")
        return False

def main():
    """Download key test videos"""
    print("=== DOWNLOADING TEST VIDEOS ===\n")
    
    # Key videos to validate
    test_videos = [
        # James Kirton - Our extraction (valid)
        ("3zCkiF_o7zw", "James_Kirton"),
        ("0mqY6-vhPhY", "James_Kirton"),
        
        # John Williams - Our extraction (valid)
        ("BtSNvQ9Rc90", "John_Williams"),
        ("ZBuff3DGbUM", "John_Williams"),
        
        # Olivia Tomlinson - From operator data (valid)
        ("2iwahDWerSQ", "Olivia_Tomlinson"),
        
        # Carlos Arthur - Direct link
        ("UD2X2hJTq4Y", "Carlos_Arthur"),
    ]
    
    success = 0
    for video_id, person in test_videos:
        if download_video(video_id, person):
            success += 1
    
    print(f"\n✅ Downloaded {success}/{len(test_videos)} test videos")
    print(f"📁 Files saved in: downloads/test_videos/")
    
    # List downloaded files
    print("\nDownloaded files:")
    downloads_dir = Path("downloads/test_videos")
    if downloads_dir.exists():
        for f in sorted(downloads_dir.glob("*.mp4")):
            size_mb = f.stat().st_size / (1024 * 1024)
            print(f"  - {f.name} ({size_mb:.1f} MB)")

if __name__ == "__main__":
    main()