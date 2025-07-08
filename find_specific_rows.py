#!/usr/bin/env python3
"""Find specific rows in the sheet.html file."""

from bs4 import BeautifulSoup
import re

def find_rows_with_ids():
    """Find rows containing specific YouTube/Drive IDs."""
    
    # IDs to search for
    search_ids = {
        'Carlos Arthur (YouTube)': 'UD2X2hJTq4Y',
        'Kiko (Drive folder)': '1-4mmoEIuZKq4xOuJpzeBdnxbkYZQJZTl',
        'Dan Jane (Drive file)': '1LRw22Qv0RS-12vJ61PauCWQHGaga7JEd'
    }
    
    # Read the HTML file
    with open('minimal/sheet.html', 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    print(f"HTML file size: {len(html_content):,} bytes")
    
    # Parse with BeautifulSoup
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Find all tables
    tables = soup.find_all('table')
    print(f"Found {len(tables)} tables")
    
    # Search for each ID
    for name, search_id in search_ids.items():
        print(f"\n{'='*80}")
        print(f"Searching for {name}: {search_id}")
        print('='*80)
        
        # Search in the entire HTML first
        if search_id in html_content:
            print(f"✓ Found {search_id} in HTML")
            
            # Find all occurrences
            pattern = re.compile(re.escape(search_id))
            matches = list(pattern.finditer(html_content))
            print(f"  Found {len(matches)} occurrences")
            
            # Show context around first match
            if matches:
                pos = matches[0].start()
                start = max(0, pos - 500)
                end = min(len(html_content), pos + 500)
                context = html_content[start:end]
                
                # Replace the search_id with highlighted version for visibility
                highlighted = context.replace(search_id, f">>>>{search_id}<<<<")
                print(f"\nContext around first occurrence:")
                print("-" * 80)
                print(highlighted)
                print("-" * 80)
                
                # Try to find the containing row
                # Look for the nearest <tr> tag before this position
                tr_start = html_content.rfind('<tr', start, pos)
                if tr_start != -1:
                    # Find the end of this row
                    tr_end = html_content.find('</tr>', pos)
                    if tr_end != -1:
                        tr_end += 5  # Include </tr>
                        row_html = html_content[tr_start:tr_end]
                        print(f"\nContaining row HTML:")
                        print("-" * 80)
                        print(row_html[:2000])  # Limit output
                        print("-" * 80)
                        
                        # Parse the row to extract cell data
                        row_soup = BeautifulSoup(row_html, 'html.parser')
                        cells = row_soup.find_all(['td', 'th'])
                        print(f"\nRow has {len(cells)} cells:")
                        for i, cell in enumerate(cells[:10]):  # Show first 10 cells
                            text = cell.get_text(strip=True)[:100]
                            print(f"  Cell {i}: {text}")
        else:
            print(f"✗ {search_id} NOT found in HTML")

if __name__ == "__main__":
    find_rows_with_ids()