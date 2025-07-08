#!/usr/bin/env python3
"""
Validate all extracted links from our system output
"""

import pandas as pd
import re
from collections import defaultdict

def validate_youtube_url(url):
    """Validate YouTube URL format and extract ID"""
    # Video patterns
    video_patterns = [
        r'(?:youtube\.com/watch\?v=|youtu\.be/)([a-zA-Z0-9_-]{11})(?:[?&]|$)',
        r'youtube\.com/embed/([a-zA-Z0-9_-]{11})(?:[?&]|$)'
    ]
    
    for pattern in video_patterns:
        match = re.search(pattern, url)
        if match:
            video_id = match.group(1)
            return True, f"Video ID: {video_id}", f"https://www.youtube.com/watch?v={video_id}"
    
    # Playlist pattern
    playlist_match = re.search(r'list=([a-zA-Z0-9_-]+)', url)
    if playlist_match:
        playlist_id = playlist_match.group(1)
        return True, f"Playlist ID: {playlist_id} (len={len(playlist_id)})", f"https://www.youtube.com/playlist?list={playlist_id}"
    
    return False, "Invalid YouTube URL format", None

def validate_drive_url(url):
    """Validate Google Drive URL format and extract ID"""
    # File pattern
    file_match = re.search(r'file/d/([a-zA-Z0-9_-]+)', url)
    if file_match:
        file_id = file_match.group(1)
        return True, f"File ID: {file_id} (len={len(file_id)})", f"https://drive.google.com/file/d/{file_id}/view"
    
    # Folder pattern
    folder_match = re.search(r'folders/([a-zA-Z0-9_-]+)', url)
    if folder_match:
        folder_id = folder_match.group(1)
        return True, f"Folder ID: {folder_id} (len={len(folder_id)})", f"https://drive.google.com/drive/folders/{folder_id}"
    
    return False, "Invalid Drive URL format", None

# Read the output file
df = pd.read_csv('outputs/output.csv')

print("COMPREHENSIVE LINK VALIDATION REPORT")
print("=" * 80)
print(f"Total records: {len(df)}")

# Collect all links
all_links = []
link_stats = defaultdict(lambda: {"valid": 0, "invalid": 0, "total": 0})
invalid_links = []
people_with_links = 0

for idx, row in df.iterrows():
    person_links = []
    
    # Extract YouTube links
    youtube_links = str(row.get('youtube_playlist', '')).split('|') if pd.notna(row.get('youtube_playlist')) else []
    youtube_links = [l.strip() for l in youtube_links if l and l != 'nan' and l.strip()]
    
    # Extract Drive links
    drive_links = str(row.get('google_drive', '')).split('|') if pd.notna(row.get('google_drive')) else []
    drive_links = [l.strip() for l in drive_links if l and l != 'nan' and l.strip()]
    
    if youtube_links or drive_links:
        people_with_links += 1
        
    # Validate YouTube links
    for link in youtube_links:
        link_stats["youtube"]["total"] += 1
        valid, info, canonical = validate_youtube_url(link)
        if valid:
            link_stats["youtube"]["valid"] += 1
            person_links.append((link, "YouTube", info, canonical))
        else:
            link_stats["youtube"]["invalid"] += 1
            invalid_links.append((row['row_id'], row['name'], link, "YouTube", info))
    
    # Validate Drive links
    for link in drive_links:
        link_stats["drive"]["total"] += 1
        valid, info, canonical = validate_drive_url(link)
        if valid:
            link_stats["drive"]["valid"] += 1
            person_links.append((link, "Drive", info, canonical))
        else:
            link_stats["drive"]["invalid"] += 1
            invalid_links.append((row['row_id'], row['name'], link, "Drive", info))
    
    all_links.extend(person_links)

# Print summary statistics
print(f"\n📊 SUMMARY STATISTICS:")
print(f"   People with links: {people_with_links}/{len(df)} ({people_with_links/len(df)*100:.1f}%)")
print(f"\n   YouTube Links:")
print(f"      Total: {link_stats['youtube']['total']}")
print(f"      Valid: {link_stats['youtube']['valid']} ({link_stats['youtube']['valid']/link_stats['youtube']['total']*100:.1f}%)" if link_stats['youtube']['total'] > 0 else "      Valid: 0")
print(f"      Invalid: {link_stats['youtube']['invalid']}")
print(f"\n   Drive Links:")
print(f"      Total: {link_stats['drive']['total']}")
print(f"      Valid: {link_stats['drive']['valid']} ({link_stats['drive']['valid']/link_stats['drive']['total']*100:.1f}%)" if link_stats['drive']['total'] > 0 else "      Valid: 0")
print(f"      Invalid: {link_stats['drive']['invalid']}")

# Show invalid links if any
if invalid_links:
    print(f"\n❌ INVALID LINKS FOUND ({len(invalid_links)}):")
    print("-" * 80)
    for row_id, name, link, link_type, error in invalid_links:
        print(f"   Row {row_id} - {name}:")
        print(f"      Type: {link_type}")
        print(f"      URL: {link}")
        print(f"      Error: {error}")
else:
    print(f"\n✅ ALL LINKS ARE VALID!")

# Show sample of valid links
print(f"\n📋 SAMPLE VALID LINKS (first 10):")
print("-" * 80)
for i, (link, link_type, info, canonical) in enumerate(all_links[:10]):
    print(f"{i+1}. {link_type}: {info}")
    print(f"   Original: {link}")
    if link != canonical:
        print(f"   Canonical: {canonical}")

# Analyze ID patterns
print(f"\n🔍 ID PATTERN ANALYSIS:")
print("-" * 80)

youtube_ids = []
playlist_ids = []
drive_file_ids = []
drive_folder_ids = []

for link, link_type, info, _ in all_links:
    if "Video ID:" in info:
        youtube_ids.append(info.split(": ")[1])
    elif "Playlist ID:" in info:
        playlist_ids.append(info.split(": ")[1].split(" ")[0])
    elif "File ID:" in info:
        drive_file_ids.append(info.split(": ")[1].split(" ")[0])
    elif "Folder ID:" in info:
        drive_folder_ids.append(info.split(": ")[1].split(" ")[0])

if youtube_ids:
    print(f"YouTube Video IDs: {len(youtube_ids)} (all should be 11 chars)")
    print(f"   Lengths: {set(len(id) for id in youtube_ids)}")
    
if playlist_ids:
    print(f"YouTube Playlist IDs: {len(playlist_ids)}")
    print(f"   Length distribution: {dict(sorted({l: playlist_ids.count(l) for l in set(len(id) for id in playlist_ids)}.items()))}")

if drive_file_ids:
    print(f"Drive File IDs: {len(drive_file_ids)}")
    print(f"   Length distribution: {dict(sorted({l: drive_file_ids.count(l) for l in set(len(id) for id in drive_file_ids)}.items()))}")

if drive_folder_ids:
    print(f"Drive Folder IDs: {len(drive_folder_ids)}")
    print(f"   Length distribution: {dict(sorted({l: drive_folder_ids.count(l) for l in set(len(id) for id in drive_folder_ids)}.items()))}")

print(f"\n✅ VALIDATION COMPLETE")
print(f"Total links validated: {len(all_links)}")