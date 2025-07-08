#!/usr/bin/env python3
"""Analyze the actual data for our target operators."""

import json
import re

def analyze_data():
    """Find all data related to our target operators."""
    
    # Load the test export data
    with open('/home/Mike/projects/xenodex/typing-clients-ingestion/data/test_export.json', 'r') as f:
        data = json.load(f)
    
    # Target operators
    targets = ['James Kirton', 'John Williams', 'Olivia Tomlinson']
    
    # Group data by operator
    operator_data = {}
    for item in data:
        if item.get('name') in targets:
            name = item['name']
            if name not in operator_data:
                operator_data[name] = []
            operator_data[name].append(item)
    
    # Analyze each operator
    for name, items in operator_data.items():
        print(f"\n{'='*60}")
        print(f"{name} (Row {items[0]['row_id']})")
        print(f"Type: {items[0]['type']}")
        print(f"Email: {items[0]['email']}")
        print(f"\nFiles ({len(items)}):")
        
        youtube_videos = []
        youtube_transcripts = []
        drive_files = []
        google_docs = []
        
        for item in items:
            filepath = item['file_path']
            filename = item['filename']
            
            # Extract YouTube video IDs
            youtube_match = re.search(r'youtube_downloads/([a-zA-Z0-9_-]+)', filepath)
            if youtube_match:
                video_id = youtube_match.group(1).split('_')[0]  # Remove _transcript suffix
                if '_transcript' in filepath:
                    youtube_transcripts.append(video_id)
                else:
                    youtube_videos.append(video_id)
            
            # Check for drive files
            if 'drive_downloads/' in filepath:
                drive_files.append(filename)
            
            # Check for Google docs
            if 'google_docs/' in filepath:
                google_docs.append(filename)
            
            print(f"  - {filepath}")
        
        # Summary
        print(f"\nSummary:")
        if youtube_videos:
            print(f"  YouTube videos: {len(set(youtube_videos))}")
            for vid_id in set(youtube_videos):
                print(f"    - https://www.youtube.com/watch?v={vid_id}")
        
        if youtube_transcripts:
            print(f"  YouTube transcripts: {len(set(youtube_transcripts))}")
        
        if drive_files:
            print(f"  Drive files: {len(drive_files)}")
            for filename in drive_files:
                print(f"    - {filename}")
        
        if google_docs:
            print(f"  Google Docs: {len(google_docs)}")
            for filename in google_docs:
                print(f"    - {filename}")
    
    # Now let's check if the expected YouTube IDs match
    print(f"\n{'='*60}")
    print("VERIFICATION AGAINST EXPECTED LINKS")
    print("="*60)
    
    expected = {
        'James Kirton': ['vvPK5D7rZvs', '1aQoJb43d1g', 'vNOpLOL4KdM'],
        'John Williams': ['TJb6DFgJT98', 'YKPPnNiQfaI'],
        'Olivia Tomlinson': ['kQvjhJ8sNRI', 'D5jX0E0nUyY', 'lGjKMmWCw6I']
    }
    
    for name, expected_ids in expected.items():
        if name in operator_data:
            print(f"\n{name}:")
            found_ids = []
            for item in operator_data[name]:
                youtube_match = re.search(r'youtube_downloads/([a-zA-Z0-9_-]+)', item['file_path'])
                if youtube_match and not '_transcript' in item['file_path']:
                    found_ids.append(youtube_match.group(1).split('.')[0])
            
            found_ids = list(set(found_ids))
            
            for exp_id in expected_ids:
                if exp_id in found_ids:
                    print(f"  ✓ {exp_id} - FOUND")
                else:
                    print(f"  ✗ {exp_id} - NOT FOUND")
            
            # Show what we actually found
            if found_ids:
                print(f"  Actually found: {found_ids}")

if __name__ == "__main__":
    analyze_data()