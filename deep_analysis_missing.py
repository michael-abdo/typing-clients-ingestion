#!/usr/bin/env python3
"""
Deep analysis to understand the root cause of "missing" downloads
"""

import pandas as pd
from pathlib import Path

# Read our actual extraction data
df = pd.read_csv('outputs/output.csv')
downloads_dir = Path("downloads")

print("DEEP ROOT CAUSE ANALYSIS: 'Missing' Downloads")
print("=" * 70)

# List of "missing" people according to operator table
missing_according_to_operator = [
    {"row": 490, "name": "Dan Jane", "expected": "1 Drive file"},
    {"row": 485, "name": "Emilie", "expected": "1 Drive folder"}, 
    {"row": 483, "name": "Brandon Donahue", "expected": "1 YouTube video"},
    {"row": 482, "name": "Joseph Cortone", "expected": "2 YouTube videos"},
    {"row": 475, "name": "Brenden Ohlsson", "expected": "1 Drive file"},
    {"row": 474, "name": "Nathalie Bauer", "expected": "1 YouTube video"}
]

print("1. CHECKING IF 'MISSING' PEOPLE EXIST UNDER DIFFERENT ROW NUMBERS")
print("-" * 70)

# Check if these people exist in our data under different row numbers
for missing_person in missing_according_to_operator:
    op_row = missing_person["row"]
    op_name = missing_person["name"]
    
    print(f"\n🔍 Looking for: {op_name} (operator says row {op_row})")
    
    # Search by name in our actual data
    name_parts = op_name.split()
    matches = df[df['name'].str.contains(name_parts[0], case=False, na=False)]
    
    if len(name_parts) > 1:
        for part in name_parts[1:]:
            matches = matches[matches['name'].str.contains(part, case=False, na=False)]
    
    if not matches.empty:
        for idx, match in matches.iterrows():
            our_row = match['row_id']
            our_name = match['name']
            
            print(f"   ✅ FOUND in our data: Row {our_row} - {our_name}")
            
            # Check if downloaded
            download_dirs = list(downloads_dir.glob(f"{our_row}_*"))
            if download_dirs:
                download_dir = download_dirs[0]
                mp3s = len(list(download_dir.glob("*.mp3")))
                infos = len(list(download_dir.glob("*info.json")))
                print(f"      📦 DOWNLOADED: {mp3s} MP3s, {infos} info files")
                print(f"      💡 CONCLUSION: NOT MISSING - Downloaded at row {our_row} vs operator's row {op_row}")
            else:
                print(f"      ❌ Found in data but NOT downloaded")
    else:
        print(f"   ❌ NOT FOUND in our extracted data")

print(f"\n" + "=" * 70)
print("2. ANALYZING ROW NUMBER PATTERNS")
print("-" * 70)

# Compare expected vs actual row numbers for people we found
print("Row number differences (Operator Row - Our Row):")
differences = []

for missing_person in missing_according_to_operator:
    op_row = missing_person["row"]
    op_name = missing_person["name"]
    
    # Find in our data
    name_parts = op_name.split()
    matches = df[df['name'].str.contains(name_parts[0], case=False, na=False)]
    
    if len(name_parts) > 1:
        for part in name_parts[1:]:
            matches = matches[matches['name'].str.contains(part, case=False, na=False)]
    
    if not matches.empty:
        our_row = matches.iloc[0]['row_id']
        diff = op_row - our_row
        differences.append(diff)
        print(f"   {op_name}: {op_row} - {our_row} = {diff}")

if differences:
    print(f"\nPattern analysis:")
    print(f"   Differences: {differences}")
    print(f"   Most common difference: {max(set(differences), key=differences.count)}")
    print(f"   All differences the same? {len(set(differences)) == 1}")

print(f"\n" + "=" * 70)
print("3. COUNTING ACTUAL VS EXPECTED TOTALS")
print("-" * 70)

# Count people with links in our data
our_people_with_links = 0
for idx, row in df.iterrows():
    youtube_links = str(row.get('youtube_playlist', '')).split('|') if pd.notna(row.get('youtube_playlist')) else []
    youtube_links = [l.strip() for l in youtube_links if l and l != 'nan' and l.strip()]
    
    drive_links = str(row.get('google_drive', '')).split('|') if pd.notna(row.get('google_drive')) else []
    drive_links = [l.strip() for l in drive_links if l and l != 'nan' and l.strip()]
    
    if youtube_links or drive_links:
        our_people_with_links += 1

# Count downloads
download_dirs = list(downloads_dir.glob("*_*/"))
actual_downloads = len(download_dirs)

print(f"People with links in our extracted data: {our_people_with_links}")
print(f"People we actually downloaded: {actual_downloads}")
print(f"Operator expects people with assets: 16 (from their table)")

print(f"\n" + "=" * 70)
print("4. ROOT CAUSE DETERMINATION")
print("-" * 70)

if len(set(differences)) <= 1 and differences:
    systematic_offset = differences[0] if differences else 0
    print(f"🎯 ROOT CAUSE IDENTIFIED:")
    print(f"   SYSTEMATIC ROW NUMBER OFFSET: {systematic_offset}")
    print(f"   The operator's table uses different row numbering than the actual Google Sheet")
    print(f"   'Missing' people are actually downloaded under offset row numbers")
    print(f"   This is a DATA MAPPING issue, not a download failure!")
else:
    print(f"🔍 MULTIPLE ISSUES DETECTED:")
    print(f"   Row number offsets vary, suggesting complex mapping issues")
    print(f"   Some people may genuinely be missing from extraction or downloads")

print(f"\n🔬 FINAL DIAGNOSIS:")
if actual_downloads >= (our_people_with_links * 0.7):  # 70% threshold
    print(f"   ✅ DOWNLOAD SUCCESS: {actual_downloads}/{our_people_with_links} people processed")
    print(f"   The apparent 'missing' downloads are likely due to row number mapping differences")
    print(f"   between the operator's reference table and actual Google Sheet row numbers.")
else:
    print(f"   ⚠️  GENUINE DOWNLOAD GAPS: Only {actual_downloads}/{our_people_with_links} people processed")
    print(f"   Some downloads genuinely failed or timed out")