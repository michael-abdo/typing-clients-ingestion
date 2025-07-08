#!/usr/bin/env python3
"""Find the actual YouTube links in the sheet data for our operators."""

import re
from bs4 import BeautifulSoup

def find_operator_data():
    """Extract the actual data for our target operators from the sheet."""
    
    with open('/home/Mike/projects/xenodex/typing-clients-ingestion/data/sheet.html', 'r') as f:
        soup = BeautifulSoup(f.read(), 'html.parser')
    
    # Target operators
    targets = {
        'James Kirton': {
            'expected_youtube': ['vvPK5D7rZvs', '1aQoJb43d1g', 'vNOpLOL4KdM']
        },
        'John Williams': {
            'expected_youtube': ['TJb6DFgJT98', 'YKPPnNiQfaI']
        },
        'Olivia Tomlinson': {
            'expected_youtube': ['kQvjhJ8sNRI', 'D5jX0E0nUyY', 'lGjKMmWCw6I'],
            'expected_drive': ['18OJA8Y6HRqxCTOZhVjmuqUxtDShtGg-R']
        }
    }
    
    # Find all rows
    rows = soup.find_all('tr')
    
    for row in rows:
        row_text = row.get_text()
        
        for name, expected in targets.items():
            if name in row_text:
                print(f"\n{'='*60}")
                print(f"Found row for {name}")
                print(f"Row number: {rows.index(row)}")
                
                # Get all cells
                cells = row.find_all('td')
                print(f"Number of cells: {len(cells)}")
                
                # Look for YouTube and Drive links in the row
                row_html = str(row)
                
                # Find YouTube links
                youtube_links = re.findall(r'youtube\.com/watch\?v=([a-zA-Z0-9_-]+)', row_html)
                youtube_links += re.findall(r'youtu\.be/([a-zA-Z0-9_-]+)', row_html)
                
                print(f"\nYouTube video IDs found: {len(youtube_links)}")
                for vid_id in youtube_links:
                    expected_match = vid_id in expected.get('expected_youtube', [])
                    print(f"  {'✓' if expected_match else '✗'} {vid_id} {'(EXPECTED)' if expected_match else ''}")
                
                # Find Drive links
                drive_links = re.findall(r'drive\.google\.com/file/d/([a-zA-Z0-9_-]+)', row_html)
                
                if drive_links:
                    print(f"\nDrive file IDs found: {len(drive_links)}")
                    for file_id in drive_links:
                        expected_match = file_id in expected.get('expected_drive', [])
                        print(f"  {'✓' if expected_match else '✗'} {file_id} {'(EXPECTED)' if expected_match else ''}")
                
                # Find Google Doc links in this row
                doc_ids = re.findall(r'docs\.google\.com/document/d/([a-zA-Z0-9-_]+)', row_html)
                unique_doc_ids = list(set(doc_ids))
                
                print(f"\nGoogle Doc IDs in this row: {len(unique_doc_ids)}")
                
                # Check if any of these docs contain the YouTube links
                if unique_doc_ids and (expected.get('expected_youtube') or expected.get('expected_drive')):
                    print("\nChecking if these docs contain the expected links...")
                    
                    import requests
                    checked = 0
                    for doc_id in unique_doc_ids[:5]:  # Check first 5
                        export_url = f"https://docs.google.com/document/d/{doc_id}/export?format=txt"
                        try:
                            response = requests.get(export_url, timeout=5)
                            if response.status_code == 200:
                                content = response.text
                                
                                # Check for expected YouTube links
                                found_expected = []
                                for yt_id in expected.get('expected_youtube', []):
                                    if yt_id in content:
                                        found_expected.append(yt_id)
                                
                                # Check for expected Drive links
                                for drive_id in expected.get('expected_drive', []):
                                    if drive_id in content:
                                        found_expected.append(drive_id)
                                
                                if found_expected:
                                    print(f"  ✓ Doc {doc_id}: Contains {len(found_expected)} expected links!")
                                    for link_id in found_expected:
                                        print(f"    - {link_id}")
                                else:
                                    print(f"  ✗ Doc {doc_id}: Public but no expected links")
                            checked += 1
                        except:
                            pass
                    
                    if checked == 0:
                        print("  Could not check any documents (all private)")
                
                # Print a sample of the row HTML to see the structure
                print(f"\nRow HTML sample (first 500 chars):")
                print(row_html[:500])
                break

if __name__ == "__main__":
    find_operator_data()