#!/usr/bin/env python3
import csv
import os
import sys
from extract_links import process_url

# DRY: Use consolidated error formatting and file reading from utils/config.py
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from utils.config import format_error, read_csv_rows

def update_csv_with_extracts(csv_path, rows_to_process=None):
    """Update the CSV file with extracted links, YouTube playlists, and Google Drive links"""
    temp_file = csv_path + '.temp'
    
    processed = 0
    updated_rows = 0
    
    # DRY: Use consolidated CSV reading from utils/config.py
    # Get headers first to set up the output file
    first_row = None
    for row_num, row in read_csv_rows(csv_path):
        first_row = row
        break
    
    if not first_row:
        print("No data found in CSV file")
        return
        
    fieldnames = list(first_row.keys())
    
    with open(temp_file, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for row_num, row in read_csv_rows(csv_path):
            # If rows_to_process is specified and we've processed enough rows, just write remaining rows
            if rows_to_process is not None and processed >= rows_to_process:
                writer.writerow(row)
                continue
                
            link = row.get('link', '')
            if link:
                try:
                    print(f"Processing {row_num}: {row['name']} - {link}")
                    
                    # Process URL with limit=10 to get more links
                    links, yt_playlist, drive_links = process_url(link, limit=10, debug=False)
                    
                    # Update row with results
                    row['extracted_links'] = '|'.join(links) if links else ''
                    if yt_playlist:
                        row['youtube_playlist'] = yt_playlist
                    if drive_links:
                        row['google_drive'] = '|'.join(drive_links)
                    
                    print(f"  Found {len(links)} links" + 
                          (f", YouTube playlist" if yt_playlist else "") + 
                          (f", {len(drive_links)} Drive links" if drive_links else ""))
                    
                    processed += 1
                    updated_rows += 1
                    
                except Exception as e:
                    # DRY: Use consolidated error formatting from utils/config.py
                    error_msg = format_error("processing", link, e)
                    print(f"  {error_msg}")
            
            writer.writerow(row)
    
    # Replace original file with temp file
    os.replace(temp_file, csv_path)
    print(f"\nProcessed {processed} rows, updated {updated_rows} rows in {csv_path}")

if __name__ == "__main__":
    import sys
    
    csv_path = '/Users/Mike/ops_typing_log/ongoing_clients/outputs/output.csv'
    
    # Get number of rows to process
    rows_to_process = None
    if len(sys.argv) > 1:
        try:
            rows_to_process = int(sys.argv[1])
        except ValueError:
            pass
    
    if rows_to_process:
        print(f"Processing first {rows_to_process} rows from {csv_path}")
    else:
        print(f"Processing all rows from {csv_path}")
        
    update_csv_with_extracts(csv_path, rows_to_process)