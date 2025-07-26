#!/usr/bin/env python3
"""
Process Sam Torode's document and extract YouTube links for download
"""

import sys
import os
import re
import pandas as pd

# Setup path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from utils.extract_links import extract_google_doc_text
from utils.csv_manager import CSVManager
from utils.patterns import PatternRegistry

def process_sam_document():
    """Process Sam's document and create proper CSV with YouTube links"""
    
    print("=== PROCESSING SAM TORODE'S DOCUMENT ===")
    
    # Sam's info
    sam_info = {
        'row_id': 502,
        'name': 'Sam Torode',
        'email': 'sam.torode@gmail.com',
        'type': 'FF-Fi/Se-CS/P(B) #4',
        'doc_link': 'https://docs.google.com/document/d/1jB8vTdDSWK5iOwCk90MEgsPil6GHURS1XS5m_c7PRqI/edit?tab=t.0'
    }
    
    # Extract document content
    print("1. Extracting document content...")
    content = extract_google_doc_text(sam_info['doc_link'], prefer_http=True)
    print(f"   Extracted: {len(content)} characters")
    
    # Extract YouTube URLs
    print("2. Extracting YouTube URLs...")
    youtube_patterns = [
        PatternRegistry.YOUTUBE_VIDEO_FULL,
        PatternRegistry.YOUTUBE_SHORT_FULL,
        PatternRegistry.YOUTUBE_PLAYLIST_FULL
    ]
    
    youtube_links = []
    for pattern in youtube_patterns:
        matches = pattern.findall(content)
        for match in matches:
            if pattern == PatternRegistry.YOUTUBE_PLAYLIST_FULL:
                clean_link = f'https://www.youtube.com/playlist?list={match}'
            else:
                clean_link = f'https://www.youtube.com/watch?v={match}'
            
            if clean_link not in youtube_links:
                youtube_links.append(clean_link)
    
    # Also extract full YouTube URLs directly
    full_youtube_urls = re.findall(r'https://www\.youtube\.com/watch\?v=[a-zA-Z0-9_-]{11}[^\s<>"]*', content)
    for url in full_youtube_urls:
        if url not in youtube_links:
            youtube_links.append(url)
    
    print(f"   Found {len(youtube_links)} YouTube links:")
    for i, link in enumerate(youtube_links, 1):
        print(f"     {i}. {link}")
    
    # Create CSV record
    print("3. Creating CSV record...")
    
    links_data = {
        'youtube': youtube_links,
        'drive_files': [],
        'drive_folders': [],
        'all_links': youtube_links
    }
    
    record = CSVManager.create_record(sam_info, mode='full', doc_text=content, links=links_data)
    
    print(f"   YouTube playlist field: {record.get('youtube_playlist', 'Not set')}")
    print(f"   Google drive field: {record.get('google_drive', 'Not set')}")
    
    # Create DataFrame and save
    df = pd.DataFrame([record])
    output_file = '/tmp/sam_for_download.csv'
    df.to_csv(output_file, index=False)
    
    print(f"4. Saved to: {output_file}")
    print(f"   Record columns: {list(df.columns)}")
    
    return output_file, youtube_links

if __name__ == "__main__":
    csv_file, youtube_links = process_sam_document()
    
    print(f"\n=== READY FOR DOWNLOAD ===")
    print(f"CSV file: {csv_file}")
    print(f"YouTube URLs to download: {len(youtube_links)}")
    print("Next step: Run download_all_minimal.py with this CSV")