#!/usr/bin/env python3
"""Check if specific entries have Google Doc links"""

import pandas as pd

# Load the CSV
from utils.config import get_config
config = get_config()
df = pd.read_csv(config.get('paths.output_csv', 'outputs/output.csv'))

# List of names to check
names_to_check = [
    'Emilie', 'Taro', 'Brandon Donahue', 'Joseph Cotroneo', 'Austyn Brown',
    'ISTPs', 'Taryn Hanes', 'Shelly Chen', 'Patryk Makara', 'Michele Q',
    'Brenden Ohlsson', 'Nathalie Bauer', 'Kaioxys DarkMacro', 'James Yu',
    'Larry Champagne', 'Ifrah Mohamed Mohamoud', 'Melike Kerpic', 
    'Mariana Gleason Freidberg', 'Jeffrey Helton', 'Daniel F. Klein',
    'Jackson Sellers', 'Samuel Rose', 'Miranda Story Ruiz', 'Samuel Kotevski',
    'Erin Golder', 'Gugu'
]

print('CHECKING GOOGLE DOC LINKS:')
print('=' * 80)

has_doc_link = 0
no_doc_link = []
has_drive_file = 0

for name in names_to_check:
    # Find the person in CSV
    matches = df[df['name'].str.contains(name, case=False, na=False)]
    
    if len(matches) == 0:
        print(f'❌ {name}: NOT FOUND IN CSV')
        continue
    
    # Get the first match
    row = matches.iloc[0]
    row_id = row.get('row_id', 'N/A')
    main_link = row.get('link', '')
    
    # Check what type of link they have
    if pd.notna(main_link) and str(main_link).strip():
        link_str = str(main_link).strip()
        
        if 'docs.google.com/document' in link_str:
            print(f'✅ {name:<30} (Row {row_id:>3}): Google Doc - {link_str[:80]}...')
            has_doc_link += 1
        elif 'drive.google.com/file' in link_str:
            print(f'📁 {name:<30} (Row {row_id:>3}): Drive File - {link_str[:80]}...')
            has_drive_file += 1
        elif 'drive.google.com' in link_str:
            print(f'📂 {name:<30} (Row {row_id:>3}): Drive Link - {link_str[:80]}...')
        else:
            print(f'❓ {name:<30} (Row {row_id:>3}): Other - {link_str[:80]}...')
    else:
        print(f'❌ {name:<30} (Row {row_id:>3}): NO LINK')
        no_doc_link.append(name)

# Summary
print('\n' + '=' * 80)
print('SUMMARY:')
print(f'Total entries checked: {len(names_to_check)}')
print(f'Has Google Doc link: {has_doc_link}')
print(f'Has Drive File link: {has_drive_file}')
print(f'No link or other: {len(no_doc_link)}')

if no_doc_link:
    print(f'\nNo document link: {", ".join(no_doc_link)}')