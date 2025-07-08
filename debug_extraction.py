#!/usr/bin/env python3
"""
Debug the extraction to understand the data structure
"""

import pandas as pd
import sys
import os

# Add current directory to path
sys.path.append(os.path.dirname(__file__))

from utils.config import get_config
from utils.http_pool import get as http_get
from bs4 import BeautifulSoup

def debug_sheet_structure():
    """Debug the sheet structure to understand the data"""
    print("🔍 DEBUGGING SHEET STRUCTURE")
    print("=" * 50)
    
    config = get_config()
    
    # Read cached sheet
    try:
        with open("sheet.html", "r", encoding="utf-8") as f:
            html_content = f.read()
        print("✓ Using cached sheet.html")
    except:
        print("❌ No cached sheet found")
        return
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
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
    
    # Look at first 10 rows to understand structure
    print("\n📋 FIRST 10 ROWS:")
    for i, row in enumerate(rows[:10]):
        cells = row.find_all(['td', 'th'])
        row_data = [cell.get_text(strip=True)[:30] for cell in cells[:6]]  # First 6 columns, truncated
        print(f"Row {i}: {row_data}")
    
    # Look for rows with IDs 501-472 specifically
    print("\n🎯 SEARCHING FOR ROWS 501-472:")
    target_rows = []
    
    for i, row in enumerate(rows[1:], 1):  # Skip header, start counting from 1
        cells = row.find_all(['td', 'th'])
        if len(cells) >= 5:
            row_id_text = cells[0].get_text(strip=True)
            
            # Try to find numeric row IDs
            try:
                row_id = int(row_id_text)
                if 472 <= row_id <= 501:
                    name = cells[1].get_text(strip=True)
                    target_rows.append((row_id, name, i))
            except ValueError:
                # Check if any cell contains our target row numbers
                for j, cell in enumerate(cells[:6]):
                    cell_text = cell.get_text(strip=True)
                    try:
                        cell_id = int(cell_text)
                        if 472 <= cell_id <= 501:
                            print(f"Found {cell_id} in column {j} at table row {i}")
                    except ValueError:
                        continue
    
    if target_rows:
        print(f"\n✅ Found {len(target_rows)} target rows:")
        target_rows.sort(reverse=True)  # Sort by row_id descending
        for row_id, name, table_row in target_rows[:10]:  # Show first 10
            print(f"Row {row_id}: {name} (table row {table_row})")
    else:
        print("❌ No rows found in range 472-501")
        
        # Let's look for any names from the operator data
        operator_names = [
            "Dmitriy Golovko", "Furva Nakamura-Saleem", "Seth Dossett", 
            "Carlos Arthur", "Caroline Chiu", "James Kirton", "Florence",
            "John Williams", "Maddie Boyle", "Kiko", "Susan Surovik"
        ]
        
        print(f"\n🔍 SEARCHING FOR OPERATOR NAMES:")
        for i, row in enumerate(rows[1:], 1):
            cells = row.find_all(['td', 'th'])
            for cell in cells:
                cell_text = cell.get_text(strip=True)
                for op_name in operator_names:
                    if op_name.lower() in cell_text.lower():
                        print(f"Found '{op_name}' at table row {i}: {cell_text}")

if __name__ == "__main__":
    debug_sheet_structure()