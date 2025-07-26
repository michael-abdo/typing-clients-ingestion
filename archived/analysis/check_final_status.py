#!/usr/bin/env python3

import pandas as pd

df = pd.read_csv('outputs/output.csv')

total_people = len(df)

# Check for files using multiple conditions
empty_conditions = [
    df['file_uuids'].isna(),
    df['file_uuids'] == '[]',
    df['file_uuids'] == '',
    df['file_uuids'] == 'null'
]

has_no_files = pd.concat(empty_conditions, axis=1).any(axis=1)
people_with_files = total_people - has_no_files.sum()

print('FINAL STATUS VERIFICATION:')
print('=' * 50)
print(f'Total people in CSV: {total_people}')
print(f'People with media files: {people_with_files}')
print(f'Coverage: {(people_with_files/total_people)*100:.1f}%')
print()

# Show people without files
missing_files = df[has_no_files]
if len(missing_files) > 0:
    print(f'People still missing files: {len(missing_files)}')
    for _, row in missing_files.head(10).iterrows():
        print(f'  Row {row["row_id"]} - {row["name"]}')
    if len(missing_files) > 10:
        print(f'  ... and {len(missing_files) - 10} more')
else:
    print('✅ ALL PEOPLE HAVE MEDIA FILES MAPPED TO CSV!')
    print('🎯 MAIN GOAL ACHIEVED: Every person has a reference in the CSV/database!')