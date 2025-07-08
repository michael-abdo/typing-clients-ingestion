#!/usr/bin/env python3
"""
Check what was successfully downloaded
"""

import os
from pathlib import Path
import json
import pandas as pd

downloads_dir = Path("downloads")

print("DOWNLOAD STATUS REPORT")
print("=" * 70)

# Read original CSV to compare
df = pd.read_csv('outputs/output.csv')

# Collect download info
download_info = {}
total_youtube = 0
total_drive = 0

# Check each person folder
for person_dir in sorted(downloads_dir.iterdir()):
    if person_dir.is_dir():
        row_id = int(person_dir.name.split('_')[0])
        person_name = '_'.join(person_dir.name.split('_')[1:]).replace('_', ' ')
        
        # Count files
        youtube_files = list(person_dir.glob("youtube_*.mp3")) + list(person_dir.glob("youtube_*.m4a"))
        youtube_files = [f for f in youtube_files if not f.name.endswith('.part')]
        
        drive_info_files = list(person_dir.glob("*drive*info.json"))
        
        download_info[row_id] = {
            'name': person_name,
            'youtube_downloaded': len(youtube_files),
            'drive_info_saved': len(drive_info_files),
            'files': [f.name for f in youtube_files + drive_info_files]
        }
        
        total_youtube += len(youtube_files)
        total_drive += len(drive_info_files)

# Display results
print(f"\n📊 SUMMARY:")
print(f"   People processed: {len(download_info)}")
print(f"   YouTube files downloaded: {total_youtube}")
print(f"   Drive info files saved: {total_drive}")
print(f"   Total files: {total_youtube + total_drive}")

print(f"\n📋 DETAILED BREAKDOWN:")
print("-" * 70)

for row_id in sorted(download_info.keys()):
    info = download_info[row_id]
    # Get expected from CSV
    row = df[df['row_id'] == row_id]
    if not row.empty:
        row = row.iloc[0]
        youtube_expected = len(str(row.get('youtube_playlist', '')).split('|')) if pd.notna(row.get('youtube_playlist')) else 0
        drive_expected = len(str(row.get('google_drive', '')).split('|')) if pd.notna(row.get('google_drive')) else 0
        
        # Skip if no links expected
        if youtube_expected == 0 and drive_expected == 0:
            continue
            
        print(f"\nRow {row_id}: {info['name']}")
        
        if info['youtube_downloaded'] > 0:
            print(f"  ✅ YouTube: {info['youtube_downloaded']} files downloaded")
            for f in info['files']:
                if 'youtube' in f and not f.endswith('.json'):
                    print(f"     - {f}")
        
        if info['drive_info_saved'] > 0:
            print(f"  📁 Drive: {info['drive_info_saved']} info files saved")
            for f in info['files']:
                if 'drive' in f and f.endswith('.json'):
                    print(f"     - {f}")

print(f"\n✅ Downloads available in: {downloads_dir.absolute()}")

# Calculate space used
total_size = 0
for f in downloads_dir.rglob('*'):
    if f.is_file():
        total_size += f.stat().st_size

print(f"📦 Total space used: {total_size / (1024*1024):.1f} MB")