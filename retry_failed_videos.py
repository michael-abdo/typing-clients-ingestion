#!/usr/bin/env python3
"""
Retry downloading the 4 failed YouTube videos with updated yt-dlp
"""

import subprocess
import sys
import os

# The 4 failed video IDs from the previous run
failed_videos = [
    "8YFZVFXEKuQ",  # Rachel Kanter - failed
    "E31TDBarL_U",  # Rachel Kanter - failed  
    "tCCv_YvAD9Y",  # Rachel Kanter - failed
    "3IvUJvatrR8"   # Jesse Baker - failed
]

def test_yt_dlp_version():
    """Check yt-dlp version"""
    try:
        result = subprocess.run(["/home/Mike/.local/bin/yt-dlp", "--version"], 
                              capture_output=True, text=True, check=True)
        print(f"yt-dlp version: {result.stdout.strip()}")
        return True
    except Exception as e:
        print(f"Error checking yt-dlp version: {e}")
        return False

def download_video(video_id, output_dir="retry_downloads"):
    """Download a single YouTube video"""
    os.makedirs(output_dir, exist_ok=True)
    
    video_url = f"https://www.youtube.com/watch?v={video_id}"
    output_path = os.path.join(output_dir, f"{video_id}.%(ext)s")
    
    print(f"\n📹 Downloading {video_id}...")
    print(f"   URL: {video_url}")
    
    cmd = [
        "/home/Mike/.local/bin/yt-dlp",
        "-f", "best[ext=mp4]/best",
        "-o", output_path,
        "--no-playlist",
        "--verbose",
        video_url
    ]
    
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=300)
        
        if result.returncode == 0:
            print(f"   ✅ SUCCESS: Downloaded {video_id}")
            # Check if file was created
            for file in os.listdir(output_dir):
                if video_id in file:
                    file_path = os.path.join(output_dir, file)
                    file_size = os.path.getsize(file_path)
                    print(f"   📁 File: {file} ({file_size:,} bytes)")
            return True
        else:
            print(f"   ❌ FAILED: {video_id}")
            print(f"   Return code: {result.returncode}")
            if result.stdout:
                print(f"   STDOUT: {result.stdout.strip()}")
            if result.stderr:
                print(f"   STDERR: {result.stderr.strip()}")
            return False
            
    except subprocess.TimeoutExpired:
        print(f"   ⏰ TIMEOUT: {video_id} took too long")
        return False
    except Exception as e:
        print(f"   💥 ERROR: {video_id} - {str(e)}")
        return False

def main():
    print("🔄 Retrying 4 failed YouTube video downloads with updated yt-dlp")
    print("=" * 70)
    
    # Check yt-dlp version
    if not test_yt_dlp_version():
        print("❌ Cannot verify yt-dlp installation")
        sys.exit(1)
    
    print(f"\n📋 Videos to retry: {len(failed_videos)}")
    for i, video_id in enumerate(failed_videos, 1):
        print(f"   {i}. {video_id}")
    
    print("\n🚀 Starting downloads...")
    
    success_count = 0
    failed_count = 0
    
    for i, video_id in enumerate(failed_videos, 1):
        print(f"\n[{i}/{len(failed_videos)}] Processing {video_id}")
        
        if download_video(video_id):
            success_count += 1
        else:
            failed_count += 1
    
    print("\n" + "=" * 70)
    print("📊 SUMMARY")
    print(f"   Total videos: {len(failed_videos)}")
    print(f"   ✅ Successful: {success_count}")
    print(f"   ❌ Failed: {failed_count}")
    
    if success_count > 0:
        print(f"\n📁 Downloaded files are in: retry_downloads/")

if __name__ == "__main__":
    main()