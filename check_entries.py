#!/usr/bin/env python3
"""Check if specific entries are in CSV and their download status"""

import csv
import pandas as pd
import os
import sys
from pathlib import Path

# Import standardized CSV reading function
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))
from csv_tracker import safe_csv_read

# Load the CSV with tracking dtype specification
from utils.config import get_config
config = get_config()
df = safe_csv_read(config.get('paths.output_csv', 'outputs/output.csv'), 'tracking')

# List of names to check
names_to_check = [
    'Emilie', 'Taro', 'Brandon Donahue', 'Joseph Cotroneo', 'Austyn Brown',
    'ISTPs', 'Taryn Hanes', 'Shelly Chen', 'Patryk Makara', 'Michele Q',
    'Brenden Ohlsson', 'Nathalie Bauer', 'Kaioxys DarkMacro', 'James Yu',
    'Larry Champagne', 'Ifrah Mohamed Mohamoud', 'Melike Kerpic', 
    'Mariana Gleason Freidberg', 'Jeffrey Helton', 'Daniel F. Klein',
    'Jackson Sellers', 'Samuel Rose', 'Miranda Story Ruiz', 'Samuel Kotevski',
    'Erin Golder', 'Gugu'
]

print('CHECKING CSV AND DOWNLOADS STATUS:')
print('=' * 80)

youtube_success = 0
drive_success = 0
not_found = []

for name in names_to_check:
    # Find the person in CSV
    matches = df[df['name'].str.contains(name, case=False, na=False)]
    
    if len(matches) == 0:
        print(f'❌ {name}: NOT FOUND IN CSV')
        not_found.append(name)
        continue
    
    # Get the first match
    row = matches.iloc[0]
    row_id = row.get('row_id', 'N/A')
    youtube_status = row.get('youtube_status', '')
    drive_status = row.get('drive_status', '')
    youtube_files = row.get('youtube_files', '')
    drive_files = row.get('drive_files', '')
    youtube_url = row.get('youtube_playlist', '')
    drive_url = row.get('google_drive', '')
    
    # Check download status
    csv_status = '✅ IN CSV'
    
    # Check YouTube downloads
    youtube_downloaded = False
    if youtube_status == 'completed' and pd.notna(youtube_files) and str(youtube_files).strip() and str(youtube_files) != '0':
        youtube_downloaded = True
        youtube_display = f'✅ YouTube: completed ({youtube_files} files)'
        youtube_success += 1
    elif pd.notna(youtube_url) and str(youtube_url).strip() and str(youtube_url) != "'-":
        youtube_display = f'❌ YouTube: {youtube_status if youtube_status else "pending"}'
    else:
        youtube_display = '⚪ YouTube: no URL'
    
    # Check Drive downloads  
    drive_downloaded = False
    if drive_status == 'completed' and pd.notna(drive_files) and str(drive_files).strip() and str(drive_files) != '0':
        drive_downloaded = True
        drive_display = f'✅ Drive: completed ({drive_files} files)'
        drive_success += 1
    elif pd.notna(drive_url) and str(drive_url).strip() and str(drive_url) != "'-":
        drive_display = f'❌ Drive: {drive_status if drive_status else "pending"}'
    else:
        drive_display = '⚪ Drive: no URL'
    
    print(f'{name:<30} (Row {row_id:>3}): {csv_status} | {youtube_display} | {drive_display}')

# Summary stats
print('\n' + '=' * 80)
print('SUMMARY:')
total_in_csv = len(names_to_check) - len(not_found)
print(f'Total found in CSV: {total_in_csv}/{len(names_to_check)}')
print(f'YouTube downloads completed: {youtube_success}')
print(f'Drive downloads completed: {drive_success}')

if not_found:
    print(f'\nNot found in CSV: {", ".join(not_found)}')

# Check actual files on disk
print('\n' + '=' * 80)
print('CHECKING ACTUAL FILES ON DISK:')

youtube_dir = Path('youtube_downloads')
drive_dir = Path('drive_downloads')

if youtube_dir.exists():
    youtube_files = list(youtube_dir.glob('*'))
    print(f'Total files in youtube_downloads: {len(youtube_files)}')
else:
    print('youtube_downloads directory not found')

if drive_dir.exists():
    drive_files = list(drive_dir.glob('*'))
    print(f'Total files in drive_downloads: {len(drive_files)}')
else:
    print('drive_downloads directory not found')