#!/usr/bin/env python3
import pandas as pd

# Read CSV
df = pd.read_csv('/home/Mike/projects/xenodex/typing-clients-ingestion/minimal/simple_output.csv')

print('CSV ANALYSIS')
print('=' * 80)
print(f'Total rows: {len(df)}')
print(f'Total columns: {len(df.columns)}')
print()

print('COLUMN INFORMATION:')
print('-' * 80)
for col in df.columns:
    # Get basic info
    dtype = df[col].dtype
    non_null = df[col].notna().sum()
    null_count = df[col].isna().sum()
    unique_count = df[col].nunique()
    
    print(f'\nColumn: {col}')
    print(f'  Data type: {dtype}')
    print(f'  Non-null values: {non_null} ({non_null/len(df)*100:.1f}%)')
    print(f'  Null values: {null_count} ({null_count/len(df)*100:.1f}%)')
    print(f'  Unique values: {unique_count}')
    
    # Sample values
    non_empty = df[df[col].notna() & (df[col] != '')]
    if len(non_empty) > 0:
        samples = non_empty[col].head(3).tolist()
        print(f'  Sample values:')
        for s in samples:
            if isinstance(s, str) and len(s) > 100:
                print(f'    - {s[:100]}...')
            else:
                print(f'    - {s}')

# Check for relationships
print('\n\nPOTENTIAL RELATIONSHIPS:')
print('=' * 80)

# Check row_id as potential primary key
if df['row_id'].nunique() == len(df):
    print('✓ row_id appears to be a unique identifier (primary key)')
else:
    print('✗ row_id is NOT unique - has duplicates')

# Check email uniqueness
if df['email'].nunique() == len(df):
    print('✓ email is unique across all rows')
else:
    print(f'✗ email has duplicates - {len(df) - df["email"].nunique()} duplicate entries')

# Check correlation between having a link and having extracted content
has_link = df['link'].notna() & (df['link'] != '')
has_youtube = df['youtube_playlist'].notna() & (df['youtube_playlist'] != '')
has_drive = df['google_drive'].notna() & (df['google_drive'] != '')

print(f'\nRows with document links: {has_link.sum()} ({has_link.sum()/len(df)*100:.1f}%)')
print(f'Rows with YouTube content: {has_youtube.sum()} ({has_youtube.sum()/len(df)*100:.1f}%)')
print(f'Rows with Drive content: {has_drive.sum()} ({has_drive.sum()/len(df)*100:.1f}%)')

# Check processing modes
print('\n\nPROCESSING PATTERNS:')
print('=' * 80)
print(f'Rows marked as processed: {(df["processed"] == "yes").sum()}')
print(f'Rows with document_text: {(df["document_text"].notna() & (df["document_text"] != "")).sum()}')
print(f'Rows with extracted_links: {(df["extracted_links"].notna() & (df["extracted_links"] != "")).sum()}')