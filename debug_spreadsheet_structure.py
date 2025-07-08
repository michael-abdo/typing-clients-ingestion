#!/usr/bin/env python3
"""
Debug the spreadsheet structure to understand the actual data format
"""

import sys
import os
sys.path.append(os.path.dirname(__file__))

from bs4 import BeautifulSoup
from utils.config import get_config

def debug_spreadsheet_structure():
    """Debug the actual spreadsheet structure"""
    print("🔍 DEBUGGING SPREADSHEET STRUCTURE")
    print("=" * 50)
    
    # Read cached sheet
    try:
        with open("sheet.html", "r", encoding="utf-8") as f:
            html_content = f.read()
        print("✓ Using cached sheet.html")
    except:
        print("❌ No cached sheet found")
        return
    
    soup = BeautifulSoup(html_content, 'html.parser')
    config = get_config()
    
    # Find the target div
    target_div_id = config.get("google_sheets.target_div_id", 1159146182)
    target_div = soup.find('div', {'id': str(target_div_id)})
    
    if not target_div:
        print(f"❌ Could not find target div with ID {target_div_id}")
        return
    
    # Find the table
    table = target_div.find('table')
    if not table:
        print("❌ Could not find table in target div")
        return
    
    rows = table.find_all('tr')
    print(f"Found {len(rows)} rows in the table")
    
    # Look at rows around 501-472 to understand the structure
    print(f"\n📋 EXAMINING ROWS WITH ROW_IDs 501-495:")
    print("-" * 80)
    
    for i, row in enumerate(rows[1:], 1):  # Skip header row
        cells = row.find_all('td')
        if len(cells) >= 6:
            
            # Extract data using correct column mapping
            sequence = cells[0].get_text(strip=True)
            row_id_text = cells[1].get_text(strip=True)
            col2 = cells[2].get_text(strip=True)
            name = cells[3].get_text(strip=True)
            email = cells[4].get_text(strip=True)
            type_val = cells[5].get_text(strip=True)
            
            # Try to find row IDs in target range
            try:
                row_id = int(row_id_text) if row_id_text else None
                if row_id and 495 <= row_id <= 501:
                    print(f"\nRow {row_id}: {name}")
                    print(f"  Email: {email}")
                    print(f"  Type: {type_val}")
                    
                    # Check for any links in the name cell (column 3)
                    name_cell = cells[3]
                    a_tags = name_cell.find_all('a')
                    if a_tags:
                        print(f"  🔗 Found {len(a_tags)} link(s) in name cell:")
                        for j, a_tag in enumerate(a_tags):
                            if a_tag.has_attr('href'):
                                href = a_tag['href']
                                link_text = a_tag.get_text(strip=True)
                                print(f"    Link {j+1}: {href}")
                                print(f"    Text: '{link_text}'")
                    else:
                        print(f"  ❌ No links found in name cell")
                        # Show the raw HTML of the name cell
                        print(f"  Name cell HTML: {str(name_cell)[:200]}...")
                        
                    # Check all other cells for links too
                    for k, cell in enumerate(cells):
                        if k == 3:  # Skip name cell (already checked)
                            continue
                        cell_a_tags = cell.find_all('a')
                        if cell_a_tags:
                            print(f"  🔗 Found {len(cell_a_tags)} link(s) in column {k}:")
                            for a_tag in cell_a_tags:
                                if a_tag.has_attr('href'):
                                    href = a_tag['href']
                                    print(f"    Column {k} link: {href}")
                    
            except ValueError:
                continue
    
    # Also check if we can find people by name directly
    target_names = ["Dmitriy Golovko", "Carlos Arthur", "James Kirton"]
    print(f"\n🎯 SEARCHING FOR SPECIFIC NAMES:")
    print("-" * 60)
    
    for i, row in enumerate(rows[1:], 1):
        cells = row.find_all('td')
        for j, cell in enumerate(cells):
            cell_text = cell.get_text(strip=True)
            for target_name in target_names:
                if target_name.lower() in cell_text.lower():
                    print(f"\nFound '{target_name}' in row {i}, column {j}")
                    print(f"  Cell text: {cell_text}")
                    # Check if this cell has links
                    a_tags = cell.find_all('a')
                    if a_tags:
                        print(f"  🔗 This cell has {len(a_tags)} link(s):")
                        for a_tag in a_tags:
                            if a_tag.has_attr('href'):
                                print(f"    {a_tag['href']}")

if __name__ == "__main__":
    debug_spreadsheet_structure()