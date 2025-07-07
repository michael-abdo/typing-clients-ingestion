#!/usr/bin/env python3
"""
Update all rows in the CSV with Google Drive links extracted from HTML.
"""
import csv
import os
import sys
from extract_links import process_url

# DRY: Use consolidated error formatting, file reading, and string manipulation from utils/config.py
sys.path.append(os.path.join(os.path.dirname(__file__), '..', '..'))
from utils.config import format_error, read_csv_rows, safe_csv_write, parse_pipe_separated

def update_drive_links(csv_path, rows_to_process=None):
    """Update Google Drive links for all rows in the CSV file"""
    temp_file = csv_path + '.temp'
    
    processed = 0
    updated_count = 0
    
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
    
    temp_file = csv_path + '.temp'
    
    with open(temp_file, 'w', newline='', encoding='utf-8') as outfile:
        writer = csv.DictWriter(outfile, fieldnames=fieldnames)
        writer.writeheader()
        
        for row_num, row in read_csv_rows(csv_path):
            # Check if we should stop processing
            if rows_to_process is not None and processed >= rows_to_process:
                writer.writerow(row)
                continue
            
            link = row.get('link', '')
            if link and 'docs.google.com/document' in link:
                # Only process Google Docs links
                print(f"Processing {row_num}: {row['name']} - {link}")
                
                try:
                    # Get links, YouTube playlist, and Drive links
                    _, _, drive_links = process_url(link, limit=0, debug=False)
                    
                    # Only update if we found Drive links
                    if drive_links:
                        # DRY: Use consolidated pipe-separated parsing from utils/config.py
                        old_drive_links = parse_pipe_separated(row.get('google_drive', ''))
                        
                        # Combine existing and new drive links
                        combined_links = list(old_drive_links)
                        for link in drive_links:
                            if link not in combined_links:
                                combined_links.append(link)
                        
                        # Update row
                        row['google_drive'] = '|'.join(combined_links)
                        updated_count += 1
                        
                        print(f"  Found {len(drive_links)} Drive links, updated to {len(combined_links)} total")
                    else:
                        print(f"  No Drive links found")
                    
                    processed += 1
                    
                except Exception as e:
                    # DRY: Use consolidated error formatting from utils/config.py
                    error_msg = format_error("processing", link, e)
                    print(f"  {error_msg}")
            
            writer.writerow(row)
    
    # Replace original file with temp file
    os.replace(temp_file, csv_path)
    print(f"\nProcessed {processed} Google Docs, updated {updated_count} rows in {csv_path}")

if __name__ == "__main__":
    # Get parameters from command line
    csv_path = '/Users/Mike/ops_typing_log/ongoing_clients/outputs/output.csv'
    
    # Get number of rows to process
    rows_to_process = None
    if len(sys.argv) > 1:
        try:
            rows_to_process = int(sys.argv[1])
            print(f"Processing first {rows_to_process} Google Docs in {csv_path}")
        except ValueError:
            pass
    else:
        print(f"Processing all Google Docs in {csv_path}")
    
    # Run the update
    update_drive_links(csv_path, rows_to_process)