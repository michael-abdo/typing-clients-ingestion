#!/usr/bin/env python3
"""
Debug the actual content of James Kirton's Google Doc to see what links are inside
"""

import sys
import os
import re
sys.path.append(os.path.dirname(__file__))

from simple_workflow import step3_scrape_doc_contents
from utils.patterns import PatternRegistry

def debug_google_doc_content():
    """Debug the actual content of the Google Doc"""
    print("🔍 DEBUGGING GOOGLE DOC CONTENT")
    print("=" * 50)
    
    # James Kirton's Google Doc URL
    doc_url = "https://docs.google.com/document/d/106UzVKTBceNnihO711snD-AeJOK9-fki9EINggkLQU8/edit?tab=t.0"
    
    print(f"Scraping: {doc_url}")
    print()
    
    # Scrape the content
    try:
        doc_content, doc_text = step3_scrape_doc_contents(doc_url)
        
        print(f"📊 CONTENT SUMMARY:")
        print(f"  HTML content length: {len(doc_content)}")
        print(f"  Text content length: {len(doc_text)}")
        print()
        
        # Search for YouTube-related content
        combined_content = doc_content + " " + doc_text
        
        print(f"🔍 SEARCHING FOR YOUTUBE CONTENT:")
        print("-" * 40)
        
        # Look for various YouTube patterns
        youtube_patterns = [
            r'youtube\.com[^"\s]*',
            r'youtu\.be[^"\s]*', 
            r'playlist\?list=[^"\s&]*',
            r'PLp0u93QMy5vCYzE4OQydGEMEJjRizGDA',
            r'PLu9i8x5U9PHhmD9K-5WY4EB12vyhL'
        ]
        
        for pattern_desc, pattern in [
            ("YouTube.com URLs", youtube_patterns[0]),
            ("Youtu.be URLs", youtube_patterns[1]), 
            ("Playlist patterns", youtube_patterns[2]),
            ("Expected Playlist 1", youtube_patterns[3]),
            ("Expected Playlist 2", youtube_patterns[4])
        ]:
            matches = re.findall(pattern, combined_content, re.IGNORECASE)
            print(f"  {pattern_desc}: {len(matches)} matches")
            if matches:
                for i, match in enumerate(matches[:5]):  # Show first 5 matches
                    print(f"    {i+1}: {match}")
                if len(matches) > 5:
                    print(f"    ... and {len(matches) - 5} more")
            print()
        
        # Try the actual patterns from PatternRegistry
        print(f"🔍 TESTING PATTERN REGISTRY:")
        print("-" * 40)
        
        try:
            # Test YouTube patterns
            youtube_patterns = [
                ("YOUTUBE_VIDEO_FULL", PatternRegistry.YOUTUBE_VIDEO_FULL),
                ("YOUTUBE_SHORT_FULL", PatternRegistry.YOUTUBE_SHORT_FULL),
                ("YOUTUBE_PLAYLIST_FULL", PatternRegistry.YOUTUBE_PLAYLIST_FULL)
            ]
            
            for name, pattern in youtube_patterns:
                matches = pattern.findall(combined_content)
                print(f"  {name}: {len(matches)} matches")
                if matches:
                    for i, match in enumerate(matches[:3]):
                        print(f"    {i+1}: {match}")
                print()
                
        except Exception as e:
            print(f"  Error testing patterns: {e}")
        
        # Look for raw HTML around potential links
        print(f"🔍 SEARCHING FOR PLAYLIST CONTEXT:")
        print("-" * 40)
        
        # Search for context around our expected playlist IDs
        playlist_ids = ["PLp0u93QMy5vCYzE4OQydGEMEJjRizGDA", "PLu9i8x5U9PHhmD9K-5WY4EB12vyhL"]
        
        for playlist_id in playlist_ids:
            # Find the position of this playlist ID in the content
            pos = combined_content.find(playlist_id)
            if pos != -1:
                print(f"  Found {playlist_id} at position {pos}")
                # Show context around this position
                start = max(0, pos - 100)
                end = min(len(combined_content), pos + 200)
                context = combined_content[start:end]
                print(f"  Context: ...{context}...")
                print()
            else:
                print(f"  {playlist_id}: NOT FOUND")
        
        # Look for any href attributes that might contain the links
        print(f"🔍 SEARCHING FOR HREF ATTRIBUTES:")
        print("-" * 40)
        
        href_pattern = r'href="([^"]*youtube[^"]*)"'
        href_matches = re.findall(href_pattern, combined_content, re.IGNORECASE)
        print(f"  Found {len(href_matches)} href attributes with YouTube")
        for i, href in enumerate(href_matches[:10]):
            print(f"    {i+1}: {href}")
        
        print()
        
    except Exception as e:
        print(f"❌ ERROR: {e}")

if __name__ == "__main__":
    debug_google_doc_content()