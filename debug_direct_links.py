#!/usr/bin/env python3
"""
Debug why direct links aren't being extracted from rows 499, 493, 490
"""
import sys
import os
sys.path.append('.')

print("🔍 DEBUGGING DIRECT LINKS IN HTML")
print("=" * 60)

try:
    from bs4 import BeautifulSoup
    
    # Load sheet HTML
    with open('minimal/sheet.html', 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    soup = BeautifulSoup(html_content, "html.parser")
    
    # Find all instances of our target links in the HTML
    print("\n1️⃣ SEARCHING FOR TARGET LINKS IN RAW HTML:")
    
    # Carlos Arthur YouTube
    if 'UD2X2hJTq4Y' in html_content:
        print("✅ Carlos Arthur YouTube ID found in HTML")
        # Find the context
        idx = html_content.find('UD2X2hJTq4Y')
        context = html_content[max(0, idx-200):idx+200]
        print(f"   Context: ...{context}...")
    
    # Kiko Drive folder
    if '1-4mmoEIuZKq4xOuJpzeBdnxbkYZQJZTl' in html_content:
        print("✅ Kiko Drive folder ID found in HTML")
    
    # Dan Jane Drive file
    if '1LRw22Qv0RS-12vJ61PauCWQHGaga7JEd' in html_content:
        print("✅ Dan Jane Drive file ID found in HTML")
    
    print("\n2️⃣ SEARCHING IN PARSED HTML STRUCTURE:")
    
    # Find the target div
    target_div = soup.find("div", {"id": "1159146182"})
    if target_div:
        print("✅ Target div found")
        table = target_div.find("table")
        if table:
            rows = table.find_all("tr")
            print(f"✅ Table found with {len(rows)} rows")
            
            # Search for specific rows
            found_rows = 0
            for row in rows:
                cells = row.find_all("td")
                if cells:
                    row_id = cells[0].get_text(strip=True) if cells else ""
                    
                    if row_id in ['499', '493', '490']:
                        found_rows += 1
                        name = cells[2].get_text(strip=True) if len(cells) > 2 else "N/A"
                        print(f"\n📍 Row {row_id}: {name}")
                        print(f"   Total cells: {len(cells)}")
                        
                        # Check ALL cells for links
                        links_found = False
                        for i, cell in enumerate(cells):
                            cell_links = cell.find_all("a")
                            if cell_links:
                                print(f"   Cell {i}: {len(cell_links)} link(s) found")
                                for link in cell_links:
                                    href = link.get('href', '')
                                    if any(id in href for id in ['UD2X2hJTq4Y', '1-4mmoEIuZKq4xOuJpzeBdnxbkYZQJZTl', '1LRw22Qv0RS-12vJ61PauCWQHGaga7JEd']):
                                        print(f"      ✅ TARGET LINK FOUND: {href[:80]}...")
                                        links_found = True
                        
                        if not links_found:
                            print("   ❌ No target links found in any cell")
                            # Show cell contents
                            for i, cell in enumerate(cells[:6]):  # First 6 cells
                                text = cell.get_text(strip=True)[:50]
                                print(f"   Cell {i} text: {text}...")
            
            print(f"\n✅ Found {found_rows} of 3 target rows")
    else:
        print("❌ Target div not found")
    
    print("\n3️⃣ TESTING EXTRACTION FUNCTION:")
    
    from minimal.extraction_utils import extract_people_from_sheet_html
    people_data = extract_people_from_sheet_html(html_content)
    
    # Check our specific rows
    for person in people_data:
        try:
            row_id = int(person.get('row_id', 0))
            if row_id in [499, 493, 490]:
                print(f"\n📍 Extracted Row {row_id}: {person.get('name')}")
                
                # Show all fields
                for key, value in person.items():
                    if value and key not in ['row_id', 'name', 'email', 'type']:
                        print(f"   {key}: {value}")
                
                # Check if direct links were captured
                total_direct = len(person.get('direct_youtube', [])) + \
                               len(person.get('direct_drive_files', [])) + \
                               len(person.get('direct_drive_folders', []))
                
                if total_direct > 0:
                    print(f"   ✅ SUCCESS: {total_direct} direct link(s) extracted!")
                else:
                    print(f"   ❌ FAILURE: No direct links extracted")
                    
        except:
            continue
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()