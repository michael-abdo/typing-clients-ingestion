#!/usr/bin/env python3
import csv
import os
import sys

# Add utils directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))

from scrape_google_sheets import fetch_table_data, update_csv
from extract_links import process_url

def process_links_from_csv(max_rows=None, reset_processed=False, force_download=False):
    """
    1. Run update_csv() to get fresh data from Google Sheets
    2. Read the CSV file and process each link
    3. Save extracted links, YouTube playlist URLs, and Google Drive links back to the same row
    
    Args:
        max_rows (int, optional): Maximum number of rows to process. Defaults to None (process all).
        reset_processed (bool, optional): If True, reprocess rows even if they have already been processed. Defaults to False.
        force_download (bool, optional): If True, force a new download of the Google Sheet. Defaults to False.
    """
    # First update the CSV with new data from Google Sheets
    print("Updating CSV with latest Google Sheets data...")
    
    # Remove the cached HTML file if force_download is True
    if force_download:
        cache_file = "google_sheet_cache.html"
        if os.path.exists(cache_file):
            print(f"Removing cached Google Sheet HTML: {cache_file}")
            os.remove(cache_file)
            
    update_csv()
    
    input_filename = "output.csv"
    temp_filename = "output_with_links.csv"
    
    if not os.path.exists(input_filename):
        print(f"Error: {input_filename} not found. Make sure the Google Sheets scraper runs correctly.")
        return
    
    rows = []
    processed_count = 0
    print("Processing links from CSV...")
    with open(input_filename, "r", newline="", encoding="utf-8") as csvfile:
        reader = csv.DictReader(csvfile)
        # Add the new columns if they don't exist
        fieldnames = list(reader.fieldnames) if reader.fieldnames else ["name", "email", "type", "link"]
        # Filter out None values if any
        fieldnames = [f for f in fieldnames if f is not None]
        if "extracted_links" not in fieldnames:
            fieldnames.append("extracted_links")
        if "youtube_playlist" not in fieldnames:
            fieldnames.append("youtube_playlist")
        if "google_drive" not in fieldnames:
            fieldnames.append("google_drive")
        
        for row in reader:
            link = row.get("link", "")
            
            # Check if this row already has data in any of the target columns
            extracted_links = row.get("extracted_links", "")
            youtube_playlist = row.get("youtube_playlist", "")
            google_drive = row.get("google_drive", "")
            
            has_extracted_links = extracted_links and str(extracted_links).strip() != ""
            has_youtube_playlist = youtube_playlist and str(youtube_playlist).strip() != ""
            has_google_drive = google_drive and str(google_drive).strip() != ""
            
            # If row already has processed data and reset_processed is False, skip
            if (has_extracted_links or has_youtube_playlist or has_google_drive) and not reset_processed:
                print(f"Skipping already processed row for {row.get('name', 'unknown')}")
                rows.append(row)
                continue
                
            if link:
                # Check if we've reached the maximum rows to process
                if max_rows is not None and processed_count >= max_rows:
                    print(f"Reached maximum rows limit ({max_rows}), skipping remaining rows")
                    rows.append(row)  # Add the row without processing
                    continue
                    
                # Create directory for HTML cache if it doesn't exist
                cache_dir = "html_cache"
                if not os.path.exists(cache_dir):
                    os.makedirs(cache_dir)
                    
                print(f"Processing {link} for {row.get('name', 'unknown')}...")
                try:
                    # Process the URL and get links, YouTube playlist, and Google Drive links
                    # With use_dash_for_empty=True, empty playlist or drive links will be "-"
                    links, youtube_playlist, drive_links = process_url(link, limit=10, use_dash_for_empty=True)
                    row["extracted_links"] = "|".join(links) if links else ""
                    row["youtube_playlist"] = youtube_playlist  # Already "-" if empty
                    
                    # Check if drive_links contains only a dash
                    if drive_links == ["-"]:
                        row["google_drive"] = "-"
                    else:
                        row["google_drive"] = "|".join(drive_links)
                    
                    print(f"  Found {len(links)} links, {'a' if youtube_playlist else 'no'} YouTube playlist, " +
                          f"and {len(drive_links)} Google Drive links")
                    
                    processed_count += 1
                except Exception as e:
                    print(f"  Error processing {link}: {str(e)}")
                    row["extracted_links"] = ""
                    row["youtube_playlist"] = ""
                    row["google_drive"] = ""
                    processed_count += 1
            else:
                # Mark empty link rows as processed with empty values
                row["extracted_links"] = ""
                row["youtube_playlist"] = ""
                row["google_drive"] = ""
            rows.append(row)
    
    # Clean up any None keys in the dictionaries
    clean_rows = []
    for row in rows:
        clean_row = {k: v for k, v in row.items() if k is not None}
        # Make sure all required fields are present
        for field in fieldnames:
            if field not in clean_row:
                clean_row[field] = ""
        clean_rows.append(clean_row)
    
    # Write results to new CSV
    with open(temp_filename, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(clean_rows)
    
    # Replace original file with updated one
    os.replace(temp_filename, input_filename)
    print(f"Successfully updated {input_filename} with extracted links and YouTube playlists")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Process links from CSV file')
    parser.add_argument('--max-rows', type=int, default=None, 
                        help='Maximum number of rows to process')
    parser.add_argument('--reset', action='store_true',
                        help='Reset processed status and reprocess all rows')
    parser.add_argument('--force-download', action='store_true',
                        help='Force new download of Google Sheet instead of using cached version')
    
    args = parser.parse_args()
    
    process_links_from_csv(max_rows=args.max_rows, reset_processed=args.reset, force_download=args.force_download)