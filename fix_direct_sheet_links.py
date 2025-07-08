#!/usr/bin/env python3
"""
Fix for extracting direct YouTube/Drive links from Google Sheet cells
"""

def extract_people_with_direct_links(html_content: str, target_div_id: str = "1159146182"):
    """
    Enhanced extraction that captures ALL links from sheet, not just Google Docs
    """
    from bs4 import BeautifulSoup
    import re
    
    soup = BeautifulSoup(html_content, "html.parser")
    target_div = soup.find("div", {"id": target_div_id})
    
    if not target_div:
        return []
    
    table = target_div.find("table")
    if not table:
        return []
    
    people_data = []
    rows = table.find_all("tr")
    
    for row_index in range(1, len(rows)):  # Skip header
        row = rows[row_index]
        cells = row.find_all("td")
        
        if len(cells) < 5:
            continue
        
        # Extract basic data
        row_id = cells[0].get_text(strip=True)
        name = cells[2].get_text(strip=True)
        email = cells[3].get_text(strip=True)
        type_val = cells[4].get_text(strip=True)
        
        # Skip headers
        if row_id == "#" or name.lower() == "name":
            continue
        
        # Look for ALL links in the row (not just column 2)
        doc_link = None
        direct_links = {
            'youtube': [],
            'drive_files': [],
            'drive_folders': []
        }
        
        # Check ALL cells for links
        for cell in cells:
            a_tags = cell.find_all("a")
            for a_tag in a_tags:
                if a_tag.has_attr("href"):
                    href = a_tag["href"]
                    
                    # Decode Google redirect URL
                    if href.startswith("https://www.google.com/url?q="):
                        actual_url = extract_actual_url(href)
                    else:
                        actual_url = href
                    
                    # Categorize the link
                    if 'docs.google.com/document' in actual_url:
                        # Google Doc - store for later processing
                        if not doc_link:  # Take first doc link
                            doc_link = actual_url
                    elif 'youtube.com' in actual_url or 'youtu.be' in actual_url:
                        # Direct YouTube link in sheet
                        direct_links['youtube'].append(actual_url)
                    elif 'drive.google.com/file' in actual_url:
                        # Direct Drive file link
                        direct_links['drive_files'].append(actual_url)
                    elif 'drive.google.com/drive/folders' in actual_url:
                        # Direct Drive folder link
                        direct_links['drive_folders'].append(actual_url)
        
        person_data = {
            "row_id": row_id,
            "name": name,
            "email": email,
            "type": type_val,
            "doc_link": doc_link if doc_link else "",
            "direct_youtube": direct_links['youtube'],
            "direct_drive_files": direct_links['drive_files'],
            "direct_drive_folders": direct_links['drive_folders']
        }
        
        people_data.append(person_data)
    
    return people_data

def extract_actual_url(google_url: str) -> str:
    """Extract actual URL from Google redirect"""
    import urllib.parse
    
    if not google_url.startswith("https://www.google.com/url?q="):
        return google_url
    
    # Extract the 'q' parameter
    start_idx = google_url.find("q=") + 2
    end_idx = google_url.find("&", start_idx)
    if end_idx == -1:
        actual_url = google_url[start_idx:]
    else:
        actual_url = google_url[start_idx:end_idx]
    
    return urllib.parse.unquote(actual_url)

# Test on specific rows
if __name__ == "__main__":
    import sys
    sys.path.append('.')
    
    # Load sheet HTML
    with open('minimal/sheet.html', 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    # Extract with enhanced logic
    people_data = extract_people_with_direct_links(html_content)
    
    # Check specific problem rows
    problem_rows = [499, 493, 490]
    
    print("CHECKING DIRECT LINK EXTRACTION:")
    print("=" * 50)
    
    for person in people_data:
        try:
            row_id = int(person.get('row_id', 0))
            if row_id in problem_rows:
                name = person['name']
                direct_youtube = person.get('direct_youtube', [])
                direct_drive_files = person.get('direct_drive_files', [])
                direct_drive_folders = person.get('direct_drive_folders', [])
                
                print(f"\nRow {row_id}: {name}")
                
                if direct_youtube:
                    print(f"  ✓ Direct YouTube: {len(direct_youtube)} links")
                    for link in direct_youtube:
                        print(f"    - {link}")
                
                if direct_drive_files:
                    print(f"  ✓ Direct Drive Files: {len(direct_drive_files)} links")
                    for link in direct_drive_files:
                        print(f"    - {link}")
                
                if direct_drive_folders:
                    print(f"  ✓ Direct Drive Folders: {len(direct_drive_folders)} links")
                    for link in direct_drive_folders:
                        print(f"    - {link}")
                
                if not any([direct_youtube, direct_drive_files, direct_drive_folders]):
                    print("  ❌ No direct links found")
                    
        except (ValueError, TypeError):
            continue