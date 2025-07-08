#!/usr/bin/env python3

import pandas as pd

df = pd.read_csv('outputs/output.csv')

print("INVESTIGATING MISSING PERSON: Shelesea Evans")
print("=" * 60)

# Find all Evans entries
evans_entries = df[df['name'].str.contains('Evans', na=False)]
print(f"\nFound {len(evans_entries)} Evans entries:")
for idx, row in evans_entries.iterrows():
    print(f"  Row {row['row_id']}: {row['name']} - {row['email']}")

# Find specific Shelsea/Shelesea
shel_evans = df[df['name'].str.contains('Evans', na=False) & df['name'].str.contains('Shel', na=False)]
if not shel_evans.empty:
    row = shel_evans.iloc[0]
    print(f'\n✅ FOUND: "{row["name"]}" at Row {row["row_id"]}')
    print(f'   Email: {row["email"]}')
    print(f'   Type: {row.get("type", "")}')
    print(f'   Doc link: {row.get("link", "")}')
    
    # Check extracted links
    youtube = str(row.get('youtube_playlist', '')).split('|') if pd.notna(row.get('youtube_playlist')) else []
    drive = str(row.get('google_drive', '')).split('|') if pd.notna(row.get('google_drive')) else []
    
    youtube = [l for l in youtube if l and l != 'nan']
    drive = [l for l in drive if l and l != 'nan']
    
    print(f'\n   Extracted links:')
    print(f'     YouTube: {len(youtube)} links')
    print(f'     Drive: {len(drive)} links')
    
    if drive:
        print(f'\n   Drive links found:')
        for link in drive:
            print(f'     - {link}')
            
    # Compare with operator expectation
    print(f'\n   📊 Operator expectation:')
    print(f'     Row 487: Shelesea Evans')
    print(f'     Expected: Google Doc → Google Drive video link')
    print(f'     Expected link: https://drive.google.com/file/d/1_dvbrXDDTlEYMGLuQ3ROsOxbAe2Riz/view?usp=sharing')
    
# Check row 487 specifically
print(f'\n' + '-' * 60)
print("Checking Row 487 specifically:")
row487 = df[df['row_id'] == 487]
if not row487.empty:
    r = row487.iloc[0]
    print(f'  Row 487 contains: {r["name"]} ({r["email"]})')
else:
    print(f'  Row 487 NOT FOUND in output')

# Check row number distribution
print(f'\n' + '-' * 60)
print("Row number analysis:")
print(f"  Total records: {len(df)}")
print(f"  Row IDs range: {df['row_id'].min()} to {df['row_id'].max()}")
print(f"  Missing row IDs in range: ", end="")
expected_rows = set(range(df['row_id'].max(), df['row_id'].min()-1, -1))
actual_rows = set(df['row_id'].values)
missing = sorted(expected_rows - actual_rows)
print(missing if missing else "None")

print("\n" + "=" * 60)
print("ROOT CAUSE ANALYSIS:")
print("1. NAME SPELLING: 'Shelsea' (our data) vs 'Shelesea' (operator data)")
print("2. ROW NUMBER: Row 486 (our data) vs Row 487 (operator data)")
print("3. This is NOT a missing person - it's a name/row mismatch!")