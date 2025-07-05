#!/usr/bin/env python3
import csv
import os
import sys

# Add utils directory to the Python path
sys.path.append(os.path.join(os.path.dirname(__file__), 'utils'))

try:
    from scrape_google_sheets import fetch_table_data, update_csv
    from extract_links import process_url
    from logging_config import get_logger
    from atomic_csv import atomic_csv_update, write_csv_atomic
    from streaming_csv import streaming_csv_update
    from csv_tracker import reset_all_download_status

# Setup module logger
logger = get_logger(__name__)
except ImportError:
    from .scrape_google_sheets import fetch_table_data, update_csv
    from .extract_links import process_url
    from .logging_config import get_logger
    from .atomic_csv import atomic_csv_update, write_csv_atomic
    from .streaming_csv import streaming_csv_update
    from .csv_tracker import reset_all_download_status

def process_links_from_csv(max_rows=None, reset_processed=False, force_download=False):
    """
    1. Run update_csv() to get fresh data from Google Sheets
    2. Read the CSV file and process each link
    3. Save extracted links, YouTube playlist URLs, and Google Drive links back to the same row
    
    Args:
        max_rows (int, optional): Maximum number of rows to process. Defaults to None (process all).
        reset_processed (bool, optional): If True, reprocess rows even if they have already been processed. Defaults to False.
        force_download (bool, optional): If True, force a new download of the Google Sheet. Defaults to False.
    
    Note:
        This function only resets the 'processed' column for link extraction.
        To reset download status (youtube_status, drive_status), use --reset-downloads flag.
    """
    # Use module-level logger
    # logger is already available at module level
    
    # First update the CSV with new data from Google Sheets
    logger.info("Updating CSV with latest Google Sheets data...")
    
    # Remove the cached HTML file if force_download is True
    if force_download:
        cache_file = "cache/google_sheet_cache.html"
        if os.path.exists(cache_file):
            logger.info(f"Removing cached Google Sheet HTML: {cache_file}")
            os.remove(cache_file)
            
    update_csv()
    
    input_filename = "outputs/output.csv"
    
    if not os.path.exists(input_filename):
        logger.error(f"Error: {input_filename} not found. Make sure the Google Sheets scraper runs correctly.")
        return
    
    # Check file size to decide between atomic or streaming operations
    file_size = os.path.getsize(input_filename)
    use_streaming = file_size > 5 * 1024 * 1024  # Use streaming for files > 5MB
    
    processed_count = 0
    logger.info(f"Processing links from CSV (file size: {file_size / 1024 / 1024:.1f}MB, using {'streaming' if use_streaming else 'atomic'} mode)...")
    
    if use_streaming:
        # Use streaming for large files
        def process_chunk(rows):
            nonlocal processed_count
            processed_rows = []
            
            for row in rows:
                link = row.get("link", "")
                
                # Check if this row has been processed (streaming version)
                processed_status = row.get("processed", "")
                extracted_links = row.get("extracted_links", "")
                youtube_playlist = row.get("youtube_playlist", "")
                google_drive = row.get("google_drive", "")
                
                # Backward compatibility: if no processed status but has link data, mark as processed
                has_link_data = (extracted_links and str(extracted_links).strip() != "") or \
                               (youtube_playlist and str(youtube_playlist).strip() != "") or \
                               (google_drive and str(google_drive).strip() != "")
                
                if not processed_status and has_link_data:
                    # Migrate existing data: mark as processed if has link data
                    row["processed"] = "yes"
                    processed_status = "yes"
                
                # If row is already processed and reset_processed is False, skip
                if processed_status in ["yes", "failed"] and not reset_processed:
                    logger.debug(f"Skipping already processed row for {row.get('name', 'unknown')}")
                    processed_rows.append(row)
                    continue
                    
                if link:
                    # Check if we've reached the maximum rows to process
                    if max_rows is not None and processed_count >= max_rows:
                        logger.info(f"Reached maximum rows limit ({max_rows}), skipping remaining rows")
                        processed_rows.append(row)  # Add the row without processing
                        continue
                        
                    # Create directory for HTML cache if it doesn't exist
                    cache_dir = "html_cache"
                    if not os.path.exists(cache_dir):
                        os.makedirs(cache_dir)
                        
                    logger.info(f"Processing {link} for {row.get('name', 'unknown')}...")
                    try:
                        # Process the URL and get links, YouTube playlist, and Google Drive links
                        links, youtube_playlist, drive_links = process_url(link, limit=10)
                        row["extracted_links"] = "|".join(links) if links else ""
                        row["youtube_playlist"] = youtube_playlist if youtube_playlist else None
                        
                        # Handle drive_links properly
                        if drive_links:
                            row["google_drive"] = "|".join(drive_links)
                        else:
                            row["google_drive"] = None
                        
                        logger.info(f"  Found {len(links)} links, {'a' if youtube_playlist else 'no'} YouTube playlist, " +
                              f"and {len(drive_links) if drive_links else 0} Google Drive links")
                        
                        # Mark as successfully processed
                        row["processed"] = "yes"
                        processed_count += 1
                    except Exception as e:
                        logger.error(f"  Error processing {link}: {str(e)}")
                        row["extracted_links"] = ""
                        row["youtube_playlist"] = ""
                        row["google_drive"] = ""
                        # Mark as failed processing
                        row["processed"] = "failed"
                        processed_count += 1
                else:
                    # Mark empty link rows as processed with empty values
                    row["extracted_links"] = ""
                    row["youtube_playlist"] = ""
                    row["google_drive"] = ""
                    row["processed"] = "yes"  # Empty links are still considered processed
                
                # Validate field sizes before adding to processed rows
                max_field_size = 100000  # 100KB limit to be safe (CSV default is 131KB)
                for field, value in row.items():
                    if value and len(str(value)) > max_field_size:
                        logger.warning(f"Field '{field}' for {row.get('name', 'unknown')} exceeds {max_field_size} bytes, truncating...")
                        row[field] = str(value)[:max_field_size] + "... [TRUNCATED]"
                
                processed_rows.append(row)
            
            return processed_rows
        
        with streaming_csv_update(input_filename, process_chunk, chunk_size=100) as processor:
            processor.process()
    else:
        # Use atomic operations for smaller files
        with atomic_csv_update(input_filename) as (rows, writer):
            if not rows:
                logger.error("Error: CSV file is empty or has no data")
                return
            
            # Get fieldnames from first row or use defaults
            if rows:
                fieldnames = list(rows[0].keys())
            else:
                fieldnames = ["name", "email", "type", "link"]
            
            # Filter out None values if any
            fieldnames = [f for f in fieldnames if f is not None]
            
            # Add the new columns if they don't exist
            if "extracted_links" not in fieldnames:
                fieldnames.append("extracted_links")
            if "youtube_playlist" not in fieldnames:
                fieldnames.append("youtube_playlist")
            if "google_drive" not in fieldnames:
                fieldnames.append("google_drive")
            if "processed" not in fieldnames:
                fieldnames.append("processed")
            
            # Update writer fieldnames
            writer.fieldnames = fieldnames
            writer.writeheader()
            
            for row in rows:
                link = row.get("link", "")
                
                # Check if this row has been processed
                processed_status = row.get("processed", "")
                extracted_links = row.get("extracted_links", "")
                youtube_playlist = row.get("youtube_playlist", "")
                google_drive = row.get("google_drive", "")
                
                # Backward compatibility: if no processed status but has link data, mark as processed
                has_link_data = (extracted_links and str(extracted_links).strip() != "") or \
                               (youtube_playlist and str(youtube_playlist).strip() != "") or \
                               (google_drive and str(google_drive).strip() != "")
                
                if not processed_status and has_link_data:
                    # Migrate existing data: mark as processed if has link data
                    row["processed"] = "yes"
                    processed_status = "yes"
                
                # If row is already processed and reset_processed is False, skip
                if processed_status in ["yes", "failed"] and not reset_processed:
                    logger.debug(f"Skipping already processed row for {row.get('name', 'unknown')} (status: {processed_status})")
                    # Write the row without processing
                    clean_row = {k: v for k, v in row.items() if k is not None}
                    for field in fieldnames:
                        if field not in clean_row:
                            clean_row[field] = ""
                    writer.writerow(clean_row)
                    continue
                    
                if link:
                    # Check if we've reached the maximum rows to process
                    if max_rows is not None and processed_count >= max_rows:
                        logger.info(f"Reached maximum rows limit ({max_rows}), skipping remaining rows")
                        # Write the row without processing
                        clean_row = {k: v for k, v in row.items() if k is not None}
                        for field in fieldnames:
                            if field not in clean_row:
                                clean_row[field] = ""
                        writer.writerow(clean_row)
                        continue
                        
                    # Create directory for HTML cache if it doesn't exist
                    cache_dir = "html_cache"
                    if not os.path.exists(cache_dir):
                        os.makedirs(cache_dir)
                        
                    logger.info(f"Processing {link} for {row.get('name', 'unknown')}...")
                    try:
                        # Process the URL and get links, YouTube playlist, and Google Drive links
                        # Empty playlist or drive links will be None
                        links, youtube_playlist, drive_links = process_url(link, limit=10)
                        row["extracted_links"] = "|".join(links) if links else ""
                        row["youtube_playlist"] = youtube_playlist if youtube_playlist else None
                        
                        # Handle drive_links properly
                        if drive_links:
                            row["google_drive"] = "|".join(drive_links)
                        else:
                            row["google_drive"] = None
                        
                        logger.info(f"  Found {len(links)} links, {'a' if youtube_playlist else 'no'} YouTube playlist, " +
                              f"and {len(drive_links) if drive_links else 0} Google Drive links")
                        
                        # Mark as successfully processed
                        row["processed"] = "yes"
                        processed_count += 1
                    except Exception as e:
                        logger.error(f"  Error processing {link}: {str(e)}")
                        row["extracted_links"] = ""
                        row["youtube_playlist"] = ""
                        row["google_drive"] = ""
                        # Mark as failed processing
                        row["processed"] = "failed"
                        processed_count += 1
                else:
                    # Mark empty link rows as processed with empty values
                    row["extracted_links"] = ""
                    row["youtube_playlist"] = ""
                    row["google_drive"] = ""
                    row["processed"] = "yes"  # Empty links are still considered processed
                
                # Clean up any None keys and ensure all fields are present
                clean_row = {k: v for k, v in row.items() if k is not None}
                for field in fieldnames:
                    if field not in clean_row:
                        clean_row[field] = ""
                
                # Validate field sizes before writing
                max_field_size = 100000  # 100KB limit to be safe (CSV default is 131KB)
                for field, value in clean_row.items():
                    if value and len(str(value)) > max_field_size:
                        logger.warning(f"Field '{field}' for {clean_row.get('name', 'unknown')} exceeds {max_field_size} bytes, truncating...")
                        clean_row[field] = str(value)[:max_field_size] + "... [TRUNCATED]"
                
                # Write the row
                writer.writerow(clean_row)
    logger.success(f"Successfully updated {input_filename} with extracted links and YouTube playlists")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Process links from CSV file')
    parser.add_argument('--max-rows', type=int, default=None, 
                        help='Maximum number of rows to process')
    parser.add_argument('--reset', action='store_true',
                        help='Reset processed status and reprocess all rows')
    parser.add_argument('--reset-downloads', action='store_true',
                        help='Reset download status (youtube_status, drive_status) for all rows')
    parser.add_argument('--force-download', action='store_true',
                        help='Force new download of Google Sheet instead of using cached version')
    
    args = parser.parse_args()
    
    # Handle download status reset if requested
    # This resets youtube_status, drive_status, files, media_ids, errors, and attempt timestamps
    # but preserves permanent_failure flags for safety
    if args.reset_downloads:
        print("Resetting download status for all rows...")
        reset_counts = reset_all_download_status(download_type='both', csv_path='outputs/output.csv')
        print(f"Reset complete: {reset_counts['youtube']} YouTube, {reset_counts['drive']} Drive downloads reset")
    
    # Process links from CSV (this handles the --reset flag for link extraction)
    process_links_from_csv(max_rows=args.max_rows, reset_processed=args.reset, force_download=args.force_download)