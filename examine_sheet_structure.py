#!/usr/bin/env python3
"""Examine the sheet structure to understand where YouTube links might be."""

import re
from bs4 import BeautifulSoup

def examine_sheet():
    """Look for patterns in how data is stored."""
    
    with open('/home/Mike/projects/xenodex/typing-clients-ingestion/data/sheet.html', 'r') as f:
        content = f.read()
    
    # First, let's search for the YouTube video IDs directly
    print("Searching for expected YouTube video IDs in the entire sheet...")
    print("="*60)
    
    expected_ids = {
        'James Kirton': ['vvPK5D7rZvs', '1aQoJb43d1g', 'vNOpLOL4KdM'],
        'John Williams': ['TJb6DFgJT98', 'YKPPnNiQfaI'],
        'Olivia Tomlinson': ['kQvjhJ8sNRI', 'D5jX0E0nUyY', 'lGjKMmWCw6I']
    }
    
    for name, video_ids in expected_ids.items():
        print(f"\n{name}:")
        for vid_id in video_ids:
            if vid_id in content:
                print(f"  ✓ Found {vid_id} in sheet")
                # Find context
                index = content.find(vid_id)
                context = content[max(0, index-100):index+100]
                print(f"    Context: ...{context}...")
            else:
                print(f"  ✗ {vid_id} NOT found in sheet")
    
    # Look for the Drive file ID
    print("\n\nSearching for Olivia's Drive file ID...")
    drive_id = '18OJA8Y6HRqxCTOZhVjmuqUxtDShtGg-R'
    if drive_id in content:
        print(f"  ✓ Found {drive_id} in sheet")
        index = content.find(drive_id)
        context = content[max(0, index-200):index+200]
        print(f"    Context: ...{context}...")
    else:
        print(f"  ✗ {drive_id} NOT found in sheet")
    
    # Let's check if maybe the links are in a different sheet or format
    print("\n\nChecking for any YouTube/Drive patterns in the sheet...")
    youtube_count = len(re.findall(r'youtube\.com/watch\?v=', content))
    drive_count = len(re.findall(r'drive\.google\.com/file/d/', content))
    
    print(f"Total YouTube links in sheet: {youtube_count}")
    print(f"Total Drive links in sheet: {drive_count}")
    
    # Sample some YouTube links
    if youtube_count > 0:
        print("\nSample YouTube links found:")
        matches = re.finditer(r'youtube\.com/watch\?v=([a-zA-Z0-9_-]+)', content)
        for i, match in enumerate(matches):
            if i >= 5:
                break
            vid_id = match.group(1)
            # Find nearby text to understand context
            start = max(0, match.start() - 50)
            end = min(len(content), match.end() + 50)
            context = content[start:end]
            print(f"  - {vid_id}")
            print(f"    Context: ...{context}...")

if __name__ == "__main__":
    examine_sheet()