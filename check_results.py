#!/usr/bin/env python3
import sys
import os
sys.path.append('.')

print("🚀 CHECKING DIRECT LINKS FIX RESULTS")
print("=" * 50)

try:
    from minimal.extraction_utils import extract_people_from_sheet_html
    
    # Load sheet HTML
    with open('minimal/sheet.html', 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    people_data = extract_people_from_sheet_html(html_content)
    print(f"Found {len(people_data)} people total\n")
    
    # Check the 3 target rows
    targets = {499: "Carlos Arthur", 493: "Kiko", 490: "Dan Jane"}
    
    for person in people_data:
        try:
            row_id = int(person.get('row_id', 0))
            if row_id in targets:
                name = person.get('name', '')
                print(f"Row {row_id}: {name}")
                
                # Check YouTube
                youtube_links = person.get('direct_youtube', [])
                if youtube_links:
                    print(f"  🎥 YouTube: {youtube_links}")
                else:
                    print(f"  🎥 YouTube: None")
                
                # Check Drive files
                drive_files = person.get('direct_drive_files', [])
                if drive_files:
                    print(f"  📁 Drive Files: {drive_files}")
                else:
                    print(f"  📁 Drive Files: None")
                
                # Check Drive folders
                drive_folders = person.get('direct_drive_folders', [])
                if drive_folders:
                    print(f"  📂 Drive Folders: {drive_folders}")
                else:
                    print(f"  📂 Drive Folders: None")
                
                # Overall assessment
                total_links = len(youtube_links) + len(drive_files) + len(drive_folders)
                if total_links > 0:
                    print(f"  ✅ SUCCESS: {total_links} direct link(s) found!")
                else:
                    print(f"  ❌ FAILED: No direct links found")
                
                print()
                
        except:
            continue
    
    print("✅ Direct links check completed")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()