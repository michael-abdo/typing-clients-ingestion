#!/usr/bin/env python3
"""
Download all valid YouTube videos from our extraction
"""
import sqlite3
import subprocess
from pathlib import Path
import re
import json
from datetime import datetime

def get_unique_youtube_links():
    """Get all unique YouTube video IDs from database"""
    conn = sqlite3.connect("minimal/xenodex.db")
    cursor = conn.cursor()
    
    query = """
    SELECT DISTINCT
        p.row_id,
        p.name,
        el.url
    FROM people p
    JOIN documents d ON p.id = d.person_id
    JOIN extracted_links el ON d.id = el.document_id
    WHERE (el.url LIKE '%youtube.com/watch%' OR el.url LIKE '%youtu.be%')
      AND el.url NOT LIKE '%playlist%'
    ORDER BY p.row_id
    """
    
    results = cursor.execute(query).fetchall()
    conn.close()
    
    # Extract unique video IDs
    videos = []
    seen_ids = set()
    
    for row_id, name, url in results:
        # Extract video ID
        video_id = None
        match = re.search(r'v=([a-zA-Z0-9_-]{11})', url)
        if not match:
            match = re.search(r'youtu\.be/([a-zA-Z0-9_-]{11})', url)
        
        if match:
            video_id = match.group(1)
            if video_id not in seen_ids and len(video_id) == 11:
                seen_ids.add(video_id)
                videos.append({
                    'row_id': row_id,
                    'name': name,
                    'video_id': video_id,
                    'url': f"https://www.youtube.com/watch?v={video_id}"
                })
    
    return videos

def download_with_best_format(video_data, output_dir):
    """Download video with automatic format selection"""
    video_id = video_data['video_id']
    name = video_data['name']
    safe_name = "".join(c for c in name if c.isalnum() or c in (' ', '-', '_')).rstrip()
    
    output = f"{output_dir}/{video_data['row_id']}_{safe_name}_{video_id}.mp4"
    
    # Skip if exists
    if Path(output).exists():
        print(f"  ✅ Already exists: {Path(output).name}")
        return True, "already_exists"
    
    # Try downloading with automatic format selection
    cmd = [
        'yt-dlp',
        '--quiet',
        '--no-warnings',
        '-f', 'best[ext=mp4]/best',  # Best mp4 or any best format
        '--max-filesize', '500M',    # Limit file size
        '-o', output,
        video_data['url']
    ]
    
    print(f"  📥 Downloading {video_id} for {name}...")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
        if result.returncode == 0:
            if Path(output).exists():
                size_mb = Path(output).stat().st_size / (1024 * 1024)
                print(f"  ✅ Success: {Path(output).name} ({size_mb:.1f} MB)")
                return True, "downloaded"
            else:
                print(f"  ⚠️  Command succeeded but file not found")
                return False, "file_not_created"
        else:
            error = result.stderr.strip()
            if "This video is not available" in error:
                print(f"  ❌ Video unavailable")
                return False, "unavailable"
            elif "Private video" in error:
                print(f"  ❌ Private video")
                return False, "private"
            else:
                print(f"  ❌ Error: {error[:100]}")
                return False, "download_error"
    except subprocess.TimeoutExpired:
        print(f"  ⏱️  Timeout")
        return False, "timeout"
    except Exception as e:
        print(f"  ❌ Exception: {str(e)[:100]}")
        return False, "exception"

def main():
    """Download all valid videos"""
    print("=== DOWNLOADING ALL VALID YOUTUBE VIDEOS ===\n")
    
    # Create download directory
    output_dir = "downloads/youtube_videos"
    Path(output_dir).mkdir(parents=True, exist_ok=True)
    
    # Get all unique videos
    videos = get_unique_youtube_links()
    print(f"Found {len(videos)} unique YouTube videos to download\n")
    
    # Download statistics
    stats = {
        'total': len(videos),
        'downloaded': 0,
        'already_exists': 0,
        'failed': 0,
        'errors': {}
    }
    
    # Group by person for better display
    by_person = {}
    for v in videos:
        if v['name'] not in by_person:
            by_person[v['name']] = []
        by_person[v['name']].append(v)
    
    # Download videos
    print(f"Processing {len(by_person)} people:\n")
    
    for person, person_videos in sorted(by_person.items()):
        print(f"\n{person} ({len(person_videos)} videos):")
        
        for video_data in person_videos:
            success, status = download_with_best_format(video_data, output_dir)
            
            if success:
                if status == "downloaded":
                    stats['downloaded'] += 1
                else:
                    stats['already_exists'] += 1
            else:
                stats['failed'] += 1
                if status not in stats['errors']:
                    stats['errors'][status] = 0
                stats['errors'][status] += 1
    
    # Summary
    print(f"\n{'='*60}")
    print("DOWNLOAD SUMMARY")
    print(f"{'='*60}")
    print(f"Total videos: {stats['total']}")
    print(f"✅ Downloaded: {stats['downloaded']}")
    print(f"📁 Already exists: {stats['already_exists']}")
    print(f"❌ Failed: {stats['failed']}")
    
    if stats['errors']:
        print("\nError breakdown:")
        for error_type, count in stats['errors'].items():
            print(f"  - {error_type}: {count}")
    
    # Save detailed report
    report = {
        'timestamp': datetime.now().isoformat(),
        'stats': stats,
        'videos': videos
    }
    
    report_path = f"{output_dir}/download_report.json"
    with open(report_path, 'w') as f:
        json.dump(report, f, indent=2)
    
    print(f"\n📊 Detailed report saved to: {report_path}")
    
    # List downloaded files
    print(f"\n📁 Downloaded files in {output_dir}:")
    total_size = 0
    for f in sorted(Path(output_dir).glob("*.mp4")):
        size_mb = f.stat().st_size / (1024 * 1024)
        total_size += size_mb
        print(f"  - {f.name} ({size_mb:.1f} MB)")
    
    print(f"\n💾 Total size: {total_size:.1f} MB")

if __name__ == "__main__":
    main()