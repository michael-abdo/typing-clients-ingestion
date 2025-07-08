#!/usr/bin/env python3
"""
Compare operator data table with what we actually downloaded
"""

import pandas as pd
from pathlib import Path

# Operator data from the table (with corrected row numbers based on our findings)
OPERATOR_DATA = {
    501: {"name": "Dmitriy Golovko", "assets": 0},
    502: {"name": "Furva Nakamura-Saleem", "assets": 0}, 
    500: {"name": "Seth Dossett", "assets": 0},
    499: {"name": "Carlos Arthur", "assets": 1},  # YouTube video
    498: {"name": "Caroline Chiu", "assets": 0},  # Operator says "No asset"
    497: {"name": "James Kirton", "assets": 2},   # 2 YouTube playlists
    496: {"name": "Florence", "assets": 0},
    495: {"name": "John Williams", "assets": 4},  # 4 YouTube videos
    494: {"name": "Maddie Boyle", "assets": 1},   # 1 YouTube video
    493: {"name": "Kiko", "assets": 1},           # 1 Drive folder
    492: {"name": "Susan Surovik", "assets": 0},
    491: {"name": "Brett Shead", "assets": 0},
    490: {"name": "Dan Jane", "assets": 1},       # 1 Drive file
    489: {"name": "Jeremy May", "assets": 1},     # 1 YouTube video
    488: {"name": "Olivia Tomlinson", "assets": 7}, # 7 YouTube videos
    487: {"name": "Shelesea Evans", "assets": 1},   # 1 Drive file (our data: row 486 "Shelsea")
    486: {"name": "Brandon Donahue", "assets": 0},
    485: {"name": "Emilie", "assets": 1},         # 1 Drive folder
    484: {"name": "Taro", "assets": 0},
    483: {"name": "Brandon Donahue", "assets": 1}, # 1 YouTube video
    482: {"name": "Joseph Cortone", "assets": 2},  # 2 YouTube videos
    481: {"name": "Austyn Brown", "assets": 1},    # 1 YouTube video
    480: {"name": "ISTPS", "assets": 0},
    479: {"name": "Taryn Hanes", "assets": 0},
    478: {"name": "Shelly Chen", "assets": 0},
    477: {"name": "Patryk Makara", "assets": 0},
    476: {"name": "Michele Q", "assets": 0},
    475: {"name": "Brenden Ohlsson", "assets": 1}, # 1 Drive file
    474: {"name": "Nathalie Bauer", "assets": 1},  # 1 YouTube video
    473: {"name": "Kaioxx DarkMacro", "assets": 0}, # Operator says "No asset"
    472: {"name": "James Yu", "assets": 1}         # 1 YouTube video
}

downloads_dir = Path("downloads")

print("DOWNLOAD STATUS vs OPERATOR EXPECTATIONS")
print("=" * 80)

# Read our extracted data
df = pd.read_csv('outputs/output.csv')

downloaded_people = 0
total_people_with_assets = 0
matches = 0

for row_id, operator_info in sorted(OPERATOR_DATA.items(), reverse=True):
    expected_assets = operator_info["assets"]
    if expected_assets > 0:
        total_people_with_assets += 1
    
    # Check if we downloaded this person
    person_folders = list(downloads_dir.glob(f"{row_id}_*"))
    
    if person_folders:
        person_folder = person_folders[0]
        downloaded_people += 1
        
        # Count downloaded files
        youtube_files = len(list(person_folder.glob("youtube_*.mp3")))
        drive_info = len(list(person_folder.glob("*drive*info.json")))
        playlist_info = len(list(person_folder.glob("playlist_*info.json")))
        
        # Total downloaded items
        total_downloaded = youtube_files + drive_info + playlist_info
        
        status = "✅" if total_downloaded >= expected_assets else "⚠️"
        if expected_assets == 0 and total_downloaded == 0:
            status = "✅"
        elif expected_assets == 0 and total_downloaded > 0:
            status = "🔍"  # We found more than operator expected
            
        print(f"{status} Row {row_id}: {operator_info['name']}")
        print(f"    Expected: {expected_assets} assets")
        print(f"    Downloaded: {total_downloaded} items ({youtube_files} YT, {drive_info} Drive, {playlist_info} Playlists)")
        
        if total_downloaded >= expected_assets and expected_assets > 0:
            matches += 1
    else:
        if expected_assets > 0:
            print(f"❌ Row {row_id}: {operator_info['name']} - NOT DOWNLOADED")
            print(f"    Expected: {expected_assets} assets")
            print(f"    Downloaded: 0 items")
        else:
            print(f"⭕ Row {row_id}: {operator_info['name']} - No assets expected, none downloaded")

print(f"\n" + "=" * 80)
print("SUMMARY")
print(f"=" * 80)
print(f"Total people in operator list: {len(OPERATOR_DATA)}")
print(f"People with assets expected: {total_people_with_assets}")
print(f"People we downloaded: {downloaded_people}")
print(f"People with matching downloads: {matches}")

coverage = (downloaded_people / total_people_with_assets * 100) if total_people_with_assets > 0 else 0
print(f"\nDownload coverage: {coverage:.1f}% of people with expected assets")

# List missing people
missing = []
for row_id, info in OPERATOR_DATA.items():
    if info["assets"] > 0:
        person_folders = list(downloads_dir.glob(f"{row_id}_*"))
        if not person_folders:
            missing.append(f"Row {row_id}: {info['name']} ({info['assets']} assets)")

if missing:
    print(f"\n❌ MISSING DOWNLOADS ({len(missing)}):")
    for person in missing:
        print(f"   {person}")

print(f"\n🎯 ANSWER: We have downloaded content from {downloaded_people} out of {total_people_with_assets} people with expected assets.")
print(f"This represents {coverage:.1f}% completion of all people with downloadable content.")