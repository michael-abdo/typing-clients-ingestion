#!/usr/bin/env python3
"""Analyze the public document we found and search for more docs."""

import requests
import re

def analyze_public_doc():
    """Analyze the public document in detail."""
    doc_id = '1rea25itJO0AMwtoWwo_QbU5XylwnhGWwNVhXah8srUw'
    export_url = f"https://docs.google.com/document/d/{doc_id}/export?format=txt"
    
    print(f"Analyzing public document: {doc_id}")
    print("="*60)
    
    response = requests.get(export_url)
    if response.status_code == 200:
        content = response.text
        print(f"Full content ({len(content)} chars):")
        print("-"*40)
        print(content)
        print("-"*40)
        
        # Search for any URLs
        url_pattern = r'https?://[^\s<>"{}|\\^`\[\]\'()]+'
        all_urls = re.findall(url_pattern, content)
        
        print(f"\nAll URLs found: {len(all_urls)}")
        for url in all_urls:
            print(f"  - {url}")
    else:
        print(f"Failed to fetch: {response.status_code}")

def find_more_docs():
    """Search for Google Docs with different patterns in the sheet."""
    print("\n\nSearching for more Google Doc patterns...")
    print("="*60)
    
    with open('/home/Mike/projects/xenodex/typing-clients-ingestion/data/sheet.html', 'r') as f:
        sheet_content = f.read()
    
    # Find rows for our target operators
    for name in ['James Kirton', 'John Williams', 'Olivia Tomlinson']:
        print(f"\nSearching for {name}...")
        
        # Find the row
        row_pattern = f'<tr[^>]*>.*?{name}.*?</tr>'
        row_match = re.search(row_pattern, sheet_content, re.DOTALL | re.IGNORECASE)
        
        if row_match:
            row_html = row_match.group(0)
            
            # Look for any Google Docs patterns
            doc_patterns = [
                r'https://docs\.google\.com/document/d/([a-zA-Z0-9-_]+)[^"\'<>\s]*',
                r'docs\.google\.com/document/[^/]+/d/([a-zA-Z0-9-_]+)',
                r'/document/d/([a-zA-Z0-9-_]+)',
            ]
            
            all_doc_ids = set()
            for pattern in doc_patterns:
                matches = re.findall(pattern, row_html)
                all_doc_ids.update(matches)
            
            print(f"Found {len(all_doc_ids)} unique document IDs")
            
            # Test a few for public access
            tested = 0
            for doc_id in list(all_doc_ids)[:10]:  # Test up to 10
                export_url = f"https://docs.google.com/document/d/{doc_id}/export?format=txt"
                try:
                    response = requests.get(export_url, timeout=5)
                    if response.status_code == 200:
                        content = response.text
                        # Check for YouTube/Drive links
                        youtube_count = len(re.findall(r'youtube\.com/watch\?v=', content))
                        drive_count = len(re.findall(r'drive\.google\.com/file/d/', content))
                        
                        if youtube_count > 0 or drive_count > 0:
                            print(f"  ✓ PUBLIC Doc {doc_id}: {youtube_count} YouTube, {drive_count} Drive links")
                            # Show sample of content
                            print(f"    Sample: {content[:100]}...")
                        else:
                            print(f"  ✓ PUBLIC Doc {doc_id}: No YouTube/Drive links found")
                    tested += 1
                except:
                    pass
            
            if tested == 0:
                print("  No documents could be tested")

if __name__ == "__main__":
    analyze_public_doc()
    find_more_docs()