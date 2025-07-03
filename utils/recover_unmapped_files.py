#!/usr/bin/env python3
"""
Recover unmapped files by matching video IDs and filenames to CSV data
"""

import os
import re
import pandas as pd
import glob
from typing import Dict, Optional


def extract_video_id(filename: str) -> Optional[str]:
    """Extract YouTube video ID from filename"""
    # YouTube video IDs are 11 characters: letters, numbers, dash, underscore
    pattern = r'([a-zA-Z0-9_-]{11})(?:\.|_)'
    match = re.search(pattern, filename)
    return match.group(1) if match else None


def extract_drive_id(filename: str) -> Optional[str]:
    """Extract Google Drive file ID from filename"""
    # Drive IDs are typically 28-33 alphanumeric characters
    pattern = r'([a-zA-Z0-9_-]{28,33})(?:\.|_)'
    match = re.search(pattern, filename)
    return match.group(1) if match else None


def match_unmapped_files(csv_path: str = 'outputs/output.csv') -> Dict:
    """Try to match unmapped files using various strategies"""
    
    df = pd.read_csv(csv_path)
    unmapped_files = []
    matched = []
    
    # Get list of files without clear mapping
    all_files = glob.glob('*_downloads/**/*', recursive=True)
    
    for file_path in all_files:
        if not os.path.isfile(file_path) or file_path.endswith('.json'):
            continue
            
        basename = os.path.basename(file_path)
        
        # Skip if already has row ID or type
        if '_row' in basename or any(x in basename for x in ['FF-', 'FM-', 'MF-', 'MM-']):
            continue
            
        # Try to match by video/file ID
        matched_row = None
        
        if 'youtube' in file_path:
            video_id = extract_video_id(basename)
            if video_id:
                # Search in youtube_files column
                for idx, row in df.iterrows():
                    if pd.notna(row.get('youtube_files')) and video_id in str(row['youtube_files']):
                        matched_row = row
                        break
                        
        elif 'drive' in file_path:
            drive_id = extract_drive_id(basename)
            if drive_id:
                # Search in drive_files column
                for idx, row in df.iterrows():
                    if pd.notna(row.get('drive_files')) and drive_id in str(row['drive_files']):
                        matched_row = row
                        break
        
        if matched_row is not None:
            matched.append({
                'file': file_path,
                'row_id': matched_row['row_id'],
                'type': matched_row['type'],
                'name': matched_row['name'],
                'match_method': 'id_matching'
            })
        else:
            unmapped_files.append(file_path)
    
    print(f"\n=== RECOVERY RESULTS ===")
    print(f"Recovered mappings for {len(matched)} previously unmapped files")
    print(f"Still unmapped: {len(unmapped_files)} files")
    
    if matched:
        print("\nSample recovered mappings:")
        for m in matched[:5]:
            print(f"  {os.path.basename(m['file'])} → {m['name']} ({m['type']})")
    
    # Save recovery mapping
    if matched:
        import json
        with open('recovered_file_mappings.json', 'w') as f:
            json.dump(matched, f, indent=2)
        print(f"\nRecovered mappings saved to: recovered_file_mappings.json")
    
    return {'matched': matched, 'unmapped': unmapped_files}


if __name__ == "__main__":
    results = match_unmapped_files()
    
    # Create suggested actions
    if results['matched']:
        print("\nTo apply recovered mappings:")
        print("1. Review recovered_file_mappings.json")
        print("2. Run: python utils/map_files_to_types.py --organize")