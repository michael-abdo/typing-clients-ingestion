#!/usr/bin/env python3
"""
Direct execution to extract 30 most recent people for comparison
"""

import pandas as pd
import sys
import os
import json
from datetime import datetime

# Add current directory to path
sys.path.append(os.path.dirname(__file__))

# Import required modules
from utils.config import get_config
from utils.csv_manager import CSVManager
from utils.http_pool import get as http_get
from bs4 import BeautifulSoup

def extract_30_people():
    """Extract most recent 30 people directly"""
    print("🚀 EXTRACTING 30 MOST RECENT PEOPLE")
    print("=" * 50)
    
    config = get_config()
    
    # Step 1: Download Google Sheet
    print("Step 1: Downloading Google Sheet...")
    sheet_url = config.get("google_sheets.url")
    
    try:
        response = http_get(sheet_url)
        response.raise_for_status()
        html_content = response.text
        
        # Save to cache
        with open("sheet.html", "w", encoding="utf-8") as f:
            f.write(html_content)
        print("✓ Sheet downloaded")
        
    except Exception as e:
        print(f"❌ Failed to download sheet: {e}")
        return
    
    # Step 2: Parse HTML and extract people
    print("Step 2: Extracting people data...")
    
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
    
    # Extract people data
    people_data = []
    
    for i, row in enumerate(rows[1:]):  # Skip header row
        cells = row.find_all(['td', 'th'])
        if len(cells) >= 5:  # Ensure we have enough columns
            
            # Extract cell data
            row_id = cells[0].get_text(strip=True)
            name = cells[1].get_text(strip=True)
            email = cells[2].get_text(strip=True)
            personality_type = cells[3].get_text(strip=True)
            link = cells[4].get_text(strip=True)
            
            # Skip empty rows
            if not row_id or not name:
                continue
                
            # Try to convert row_id to integer for sorting
            try:
                row_id_int = int(row_id)
            except ValueError:
                continue  # Skip non-numeric row IDs
            
            person_record = {
                'row_id': row_id,
                'name': name,
                'email': email,
                'type': personality_type,
                'link': link
            }
            
            people_data.append(person_record)
    
    print(f"✓ Found {len(people_data)} people records")
    
    # Sort by row_id descending to get most recent first
    people_data.sort(key=lambda x: int(x['row_id']), reverse=True)
    
    # Take first 30
    top_30 = people_data[:30]
    
    print(f"✓ Selected top 30 most recent people (rows {top_30[0]['row_id']}-{top_30[-1]['row_id']})")
    
    # Step 3: Save to CSV
    print("Step 3: Saving to CSV...")
    
    # Create DataFrame
    df = pd.DataFrame(top_30)
    
    # Save to CSV
    output_file = "comparison_30_people.csv"
    csv_manager = CSVManager(csv_path=output_file)
    
    if csv_manager.safe_csv_write(df, operation_name="30_people_comparison"):
        print(f"✅ Data saved to {output_file}")
        
        # Display results
        print("\n📊 EXTRACTED RECORDS:")
        print("-" * 80)
        for person in top_30:
            asset_status = "No asset" if not person['link'].strip() else "Has asset"
            print(f"Row {person['row_id']}: {person['name']} - {asset_status}")
        
        print(f"\n✅ SUCCESS: Extracted {len(top_30)} people")
        return output_file
        
    else:
        print(f"❌ Failed to save data to {output_file}")
        return None

if __name__ == "__main__":
    extract_30_people()