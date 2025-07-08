#!/usr/bin/env python3
"""
Corrected extraction to get the actual 30 most recent people (rows 501-472)
"""

import pandas as pd
import sys
import os

# Add current directory to path
sys.path.append(os.path.dirname(__file__))

from utils.config import get_config
from utils.csv_manager import CSVManager
from bs4 import BeautifulSoup

def extract_correct_30():
    """Extract the correct 30 most recent people (rows 501-472)"""
    print("🎯 EXTRACTING CORRECT 30 MOST RECENT PEOPLE (Rows 501-472)")
    print("=" * 60)
    
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
    
    # Extract people data with correct column mapping
    people_data = []
    
    for i, row in enumerate(rows[1:], 1):  # Skip header row
        cells = row.find_all(['td', 'th'])
        if len(cells) >= 6:  # Need at least 6 columns
            
            # Correct column mapping based on debug output:
            # Column 0: sequence number
            # Column 1: actual row_id 
            # Column 2: empty
            # Column 3: name
            # Column 4: email  
            # Column 5: type
            
            sequence = cells[0].get_text(strip=True)
            row_id_text = cells[1].get_text(strip=True)
            name = cells[3].get_text(strip=True)
            email = cells[4].get_text(strip=True)
            personality_type = cells[5].get_text(strip=True)
            
            # Try to get link from additional columns if available
            link = ""
            if len(cells) >= 7:
                link = cells[6].get_text(strip=True)
            
            # Skip empty rows
            if not row_id_text or not name:
                continue
                
            # Try to convert row_id to integer
            try:
                row_id_int = int(row_id_text)
            except ValueError:
                continue  # Skip non-numeric row IDs
            
            # Only include rows 472-501 (the 30 most recent)
            if 472 <= row_id_int <= 501:
                person_record = {
                    'row_id': row_id_text,
                    'name': name,
                    'email': email,
                    'type': personality_type,
                    'link': link
                }
                
                people_data.append(person_record)
    
    print(f"✓ Found {len(people_data)} people records in range 472-501")
    
    # Sort by row_id descending to get most recent first (501 -> 472)
    people_data.sort(key=lambda x: int(x['row_id']), reverse=True)
    
    print(f"✓ Sorted records from row {people_data[0]['row_id']} to {people_data[-1]['row_id']}")
    
    # Step 3: Save to CSV
    print("Step 3: Saving to CSV...")
    
    # Create DataFrame
    df = pd.DataFrame(people_data)
    
    # Save to CSV
    output_file = "operator_comparison_30.csv"
    csv_manager = CSVManager(csv_path=output_file)
    
    if csv_manager.safe_csv_write(df, operation_name="operator_30_comparison"):
        print(f"✅ Data saved to {output_file}")
        
        # Display results for comparison
        print("\n📊 EXTRACTED RECORDS FOR COMPARISON:")
        print("-" * 80)
        for i, person in enumerate(people_data, 1):
            asset_status = "No asset" if not person['link'].strip() else "Has asset"
            print(f"{i:2d}. Row {person['row_id']}: {person['name']} - {asset_status}")
        
        print(f"\n✅ SUCCESS: Extracted {len(people_data)} people for operator comparison")
        return output_file
        
    else:
        print(f"❌ Failed to save data to {output_file}")
        return None

if __name__ == "__main__":
    extract_correct_30()