#!/usr/bin/env python3
"""
Download all extracted YouTube and Drive links locally
"""
import os
import sqlite3
import subprocess
from pathlib import Path
import json
from datetime import datetime

def create_download_directories():
    """Create organized directory structure for downloads"""
    dirs = [
        "downloads/youtube",
        "downloads/drive", 
        "downloads/by_person"
    ]
    for d in dirs:
        Path(d).mkdir(parents=True, exist_ok=True)
    return dirs

def get_all_links_from_db():
    """Get all extracted links from database organized by person"""
    conn = sqlite3.connect("minimal/xenodex.db")
    cursor = conn.cursor()
    
    query = """
    SELECT DISTINCT
        p.row_id,
        p.name,
        p.email,
        p.type,
        el.url,
        el.link_type
    FROM people p
    JOIN documents d ON p.id = d.person_id
    JOIN extracted_links el ON d.id = el.document_id
    WHERE el.url LIKE '%youtube%' 
       OR el.url LIKE '%drive.google%'
    ORDER BY p.row_id, el.url
    """
    
    results = cursor.execute(query).fetchall()
    conn.close()
    
    # Organize by person
    links_by_person = {}
    for row in results:
        row_id, name, email, type_info, url, link_type = row
        
        # Clean up URL
        if 'youtube.com/watch?v=' in url and len(url) > 50:
            # Extract just the video ID
            import re
            match = re.search(r'v=([a-zA-Z0-9_-]{11})', url)
            if match:
                url = f"https://www.youtube.com/watch?v={match.group(1)}"
        
        if name not in links_by_person:
            links_by_person[name] = {
                'row_id': row_id,
                'email': email,
                'type': type_info,
                'youtube': [],
                'drive': []
            }
        
        if 'youtube' in url or 'youtu.be' in url:
            if url not in links_by_person[name]['youtube']:
                links_by_person[name]['youtube'].append(url)
        elif 'drive.google' in url:
            if url not in links_by_person[name]['drive']:
                links_by_person[name]['drive'].append(url)
    
    return links_by_person

def download_youtube_video(url, output_dir, person_name=None):
    """Download YouTube video using yt-dlp"""
    import re
    
    # Extract video ID
    video_id = None
    match = re.search(r'v=([a-zA-Z0-9_-]{11})', url)
    if not match:
        match = re.search(r'youtu\.be/([a-zA-Z0-9_-]{11})', url)
    
    if match:
        video_id = match.group(1)
    else:
        print(f"  ❌ Could not extract video ID from: {url}")
        return False
    
    # Set output filename
    if person_name:
        safe_name = "".join(c for c in person_name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        output_template = f"{output_dir}/{safe_name}_{video_id}_%(title)s.%(ext)s"
    else:
        output_template = f"{output_dir}/{video_id}_%(title)s.%(ext)s"
    
    # yt-dlp command
    cmd = [
        'yt-dlp',
        '--no-warnings',
        '-f', 'best[height<=720]',  # Limit to 720p for space
        '--write-sub',  # Download subtitles
        '--sub-lang', 'en',
        '-o', output_template,
        url
    ]
    
    print(f"  📥 Downloading {video_id}...")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"  ✅ Downloaded successfully")
            return True
        else:
            print(f"  ❌ Error: {result.stderr}")
            return False
    except Exception as e:
        print(f"  ❌ Exception: {e}")
        return False

def download_drive_file(url, output_dir, person_name=None):
    """Download Google Drive file using gdown"""
    import re
    
    # Extract file ID
    file_id = None
    patterns = [
        r'/file/d/([a-zA-Z0-9_-]+)',
        r'/folders/([a-zA-Z0-9_-]+)',
        r'id=([a-zA-Z0-9_-]+)'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            file_id = match.group(1)
            break
    
    if not file_id:
        print(f"  ❌ Could not extract file ID from: {url}")
        return False
    
    # For folders, we can't download directly
    if '/folders/' in url:
        print(f"  ⚠️  Folder link (manual download required): {url}")
        return False
    
    # gdown command
    output_path = f"{output_dir}/{person_name}_{file_id}" if person_name else f"{output_dir}/{file_id}"
    
    cmd = [
        'gdown',
        f'https://drive.google.com/uc?id={file_id}',
        '-O', output_path,
        '--quiet'
    ]
    
    print(f"  📥 Downloading Drive file {file_id}...")
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            print(f"  ✅ Downloaded to {output_path}")
            return True
        else:
            # Try alternate method
            print(f"  ⚠️  gdown failed, file may be private: {url}")
            return False
    except Exception as e:
        print(f"  ❌ Exception: {e}")
        return False

def main():
    """Download all extracted links"""
    print("=== DOWNLOADING ALL EXTRACTED LINKS ===\n")
    
    # Check for required tools
    print("Checking required tools...")
    tools_ok = True
    
    # Check yt-dlp
    try:
        subprocess.run(['yt-dlp', '--version'], capture_output=True, check=True)
        print("✅ yt-dlp installed")
    except:
        print("❌ yt-dlp not installed. Install with: pip install yt-dlp")
        tools_ok = False
    
    # Check gdown
    try:
        subprocess.run(['gdown', '--version'], capture_output=True, check=True)
        print("✅ gdown installed")
    except:
        print("❌ gdown not installed. Install with: pip install gdown")
        tools_ok = False
    
    if not tools_ok:
        print("\nPlease install missing tools first!")
        return
    
    # Create directories
    create_download_directories()
    
    # Get all links
    links_by_person = get_all_links_from_db()
    
    print(f"\nFound links for {len(links_by_person)} people")
    
    # Download statistics
    stats = {
        'youtube_success': 0,
        'youtube_failed': 0,
        'drive_success': 0,
        'drive_failed': 0,
        'people_processed': 0
    }
    
    # Process each person
    for name, data in links_by_person.items():
        print(f"\n{'='*60}")
        print(f"Processing: {name} (Row {data['row_id']})")
        print(f"Email: {data['email']}")
        print(f"Type: {data['type']}")
        
        # Create person directory
        safe_name = "".join(c for c in name if c.isalnum() or c in (' ', '-', '_')).rstrip()
        person_dir = f"downloads/by_person/{data['row_id']}_{safe_name}"
        Path(person_dir).mkdir(exist_ok=True)
        
        # Download YouTube videos
        if data['youtube']:
            print(f"\nYouTube Videos ({len(data['youtube'])}):")
            for url in data['youtube']:
                if download_youtube_video(url, person_dir, safe_name):
                    stats['youtube_success'] += 1
                else:
                    stats['youtube_failed'] += 1
        
        # Download Drive files
        if data['drive']:
            print(f"\nDrive Files ({len(data['drive'])}):")
            for url in data['drive']:
                if download_drive_file(url, person_dir, safe_name):
                    stats['drive_success'] += 1
                else:
                    stats['drive_failed'] += 1
        
        stats['people_processed'] += 1
    
    # Generate report
    print(f"\n{'='*60}")
    print("DOWNLOAD SUMMARY")
    print(f"{'='*60}")
    print(f"People processed: {stats['people_processed']}")
    print(f"YouTube videos:")
    print(f"  ✅ Success: {stats['youtube_success']}")
    print(f"  ❌ Failed: {stats['youtube_failed']}")
    print(f"Drive files:")
    print(f"  ✅ Success: {stats['drive_success']}") 
    print(f"  ❌ Failed: {stats['drive_failed']}")
    
    # Save report
    report = {
        'timestamp': datetime.now().isoformat(),
        'stats': stats,
        'people': links_by_person
    }
    
    with open('downloads/download_report.json', 'w') as f:
        json.dump(report, f, indent=2)
    
    print("\n✅ Download report saved to downloads/download_report.json")

if __name__ == "__main__":
    main()