#!/usr/bin/env python3
import sys
import os
sys.path.append('.')

# Direct inline execution
try:
    from minimal.extraction_utils import extract_people_from_sheet_html
    
    # Load sheet HTML
    with open('minimal/sheet.html', 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    people_data = extract_people_from_sheet_html(html_content)
    
    # Check specific rows
    found_carlos = False
    found_kiko = False
    found_dan = False
    
    for person in people_data:
        try:
            row_id = int(person.get('row_id', 0))
            
            if row_id == 499:  # Carlos Arthur
                found_carlos = True
                print(f"Row 499: {person.get('name')}")
                print(f"  Direct YouTube: {person.get('direct_youtube', [])}")
                print(f"  Has YouTube link: {len(person.get('direct_youtube', [])) > 0}")
                
            elif row_id == 493:  # Kiko
                found_kiko = True
                print(f"Row 493: {person.get('name')}")
                print(f"  Direct Drive Folders: {person.get('direct_drive_folders', [])}")
                print(f"  Has Drive folder: {len(person.get('direct_drive_folders', [])) > 0}")
                
            elif row_id == 490:  # Dan Jane
                found_dan = True
                print(f"Row 490: {person.get('name')}")
                print(f"  Direct Drive Files: {person.get('direct_drive_files', [])}")
                print(f"  Has Drive file: {len(person.get('direct_drive_files', [])) > 0}")
                
        except:
            continue
    
    print(f"\nFound Carlos (499): {found_carlos}")
    print(f"Found Kiko (493): {found_kiko}")
    print(f"Found Dan (490): {found_dan}")
    
except Exception as e:
    print(f"Error: {e}")
    import traceback
    traceback.print_exc()