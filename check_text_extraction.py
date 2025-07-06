#!/usr/bin/env python3
import pandas as pd

# Read the CSV
df = pd.read_csv('minimal/simple_output.csv')

print(f'Total rows: {len(df)}')

# Check for document_text
if 'document_text' in df.columns:
    non_empty = df['document_text'].notna() & (df['document_text'] != '')
    print(f'Rows with document_text: {non_empty.sum()}')
    
    if non_empty.sum() > 0:
        print('\nSample rows with text:')
        for idx, row in df[non_empty].head(5).iterrows():
            text_preview = str(row['document_text'])[:100] + '...' if len(str(row['document_text'])) > 100 else str(row['document_text'])
            print(f"  Row {row['row_id']}: {row['name']} - {len(str(row['document_text']))} chars")
            print(f"    Preview: {text_preview}")
else:
    print("No document_text column found!")

# Check specific row 484 (Emilie)
emilie = df[df['row_id'] == 484]
if not emilie.empty:
    print(f"\nEmilie's record (row 484):")
    print(f"  Has Google Doc link: {'Yes' if emilie.iloc[0]['link'] else 'No'}")
    if 'document_text' in df.columns:
        text = emilie.iloc[0]['document_text']
        print(f"  Document text: {len(str(text)) if pd.notna(text) and text != '' else 0} chars")