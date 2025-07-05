#!/usr/bin/env python3
import os
import sys
import argparse
import subprocess
import csv
from pathlib import Path

# Add parent directory to path to access utils
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.logger import pipeline_run, get_pipeline_logger
from utils.logging_config import get_logger
from utils.parallel_processor import parallel_download_youtube_videos
from utils.config import get_config, get_drive_downloads_dir
from utils.csv_tracker import ensure_tracking_columns, get_pending_downloads, update_csv_download_status, safe_csv_read
from utils.row_context import create_row_context_from_csv_row
from utils.download_youtube import download_youtube_with_context
from utils.download_drive import download_drive_with_context

# Import enhanced download wrapper for real-time mapping database updates
try:
    import enhanced_download_wrapper
    print("✅ Enhanced mapping database integration activated")
except ImportError as e:
    print(f"⚠️  Enhanced download wrapper not available: {e}")
except Exception as e:
    print(f"⚠️  Enhanced download wrapper failed to load: {e}")

# Get configuration and set CSV field size limit
config = get_config()
csv.field_size_limit(config.get('file_processing.max_csv_field_size', sys.maxsize))

def run_process(command, description=None, component='main', logger=None):
    """Run a process with the given command and print its output in real-time"""
    # If logger is provided, use it; otherwise fall back to print
    if logger:
        return logger.log_subprocess(command, description, component)
    
    # Original implementation for when logger is not available
    if description:
        print(f"\n{'=' * 80}\n{description}\n{'=' * 80}")
    
    # Get the virtual environment's Python interpreter path
    venv_python = sys.executable
    
    # If command is a list starting with sys.executable or 'python', replace it with venv_python
    if isinstance(command, list) and (command[0] == sys.executable or command[0].endswith('python') or command[0].endswith('python3')):
        command[0] = venv_python
    
    process = subprocess.Popen(
        command,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
        bufsize=1,
        universal_newlines=True
    )
    
    # Print output in real-time
    for line in process.stdout:
        print(line, end='')
    
    process.wait()
    return process.returncode

def main():
    parser = argparse.ArgumentParser(description="Run the complete workflow: scrape Google Sheet, download Google Drive files and YouTube videos")
    parser.add_argument("--max-rows", type=int, default=None, help="Maximum number of rows to process for Google Sheet scraping")
    parser.add_argument("--reset", action="store_true", help="Reset processed status and reprocess all rows")
    parser.add_argument("--reset-downloads", action="store_true", help="Reset download status (youtube_status, drive_status) for all rows")
    parser.add_argument("--skip-sheet", action="store_true", help="Skip Google Sheet scraping")
    parser.add_argument("--skip-drive", action="store_true", help="Skip Google Drive downloads")
    parser.add_argument("--skip-youtube", action="store_true", help="Skip YouTube downloads")
    parser.add_argument("--max-youtube", type=int, default=None, help="Maximum number of YouTube videos to download")
    parser.add_argument("--max-drive", type=int, default=None, help="Maximum number of Google Drive files to download")
    parser.add_argument("--no-logging", action="store_true", help="Disable logging to files")
    parser.add_argument("--use-cache", action="store_true", help="Use cached Google Sheet data instead of force downloading (default: always force download)")
    
    args = parser.parse_args()
    
    # Use logging unless disabled
    if args.no_logging:
        return main_workflow(args)
    else:
        with pipeline_run() as pipeline_logger:
            return main_workflow(args, pipeline_logger)

def main_workflow(args, logger=None):
    """Main workflow logic with row-centric tracking"""
    
    # Step 0: Ensure CSV has tracking columns
    output_csv_path = config.get('paths.output_csv', 'output.csv')
    if os.path.exists(output_csv_path):
        tracking_added = ensure_tracking_columns(output_csv_path)
        if tracking_added and logger:
            logger.get_logger('main').info("Enhanced CSV with tracking columns for row-centric downloads")
    
    # Step 1: Scrape Google Sheet and extract links
    if not args.skip_sheet:
        command = [sys.executable, "utils/master_scraper.py"]
        # Force download by default, unless --use-cache is specified
        if not args.use_cache:
            command.append("--force-download")
        if args.max_rows is not None:
            command.extend(["--max-rows", str(args.max_rows)])
        if args.reset:
            command.append("--reset")
        if args.reset_downloads:
            command.append("--reset-downloads")
        
        exit_code = run_process(command, "Step 1: Scraping Google Sheet and extracting links", 'scraper', logger)
        if exit_code != 0:
            if logger:
                logger.log_error(f"Error in Step 1: Google Sheet scraping failed with exit code {exit_code}")
            else:
                print(f"Error in Step 1: Google Sheet scraping failed with exit code {exit_code}")
            return exit_code
    
    # Step 2: Download Google Drive files using row-centric tracking
    if not args.skip_drive:
        # Get pending Drive downloads from CSV tracking system
        pending_drive = get_pending_downloads(output_csv_path, 'drive')
        
        if pending_drive:
            # Apply max-drive limit if specified
            if args.max_drive is not None and args.max_drive > 0:
                pending_drive = pending_drive[:args.max_drive]
                if logger:
                    logger.get_logger('main').info(f"Step 2: Downloading {len(pending_drive)} Google Drive files (limited by --max-drive)")
                else:
                    print(f"\n{'=' * 80}\nStep 2: Downloading {len(pending_drive)} Google Drive files (limited by --max-drive)\n{'=' * 80}")
            else:
                if logger:
                    logger.get_logger('main').info(f"Step 2: Downloading {len(pending_drive)} Google Drive files")
                else:
                    print(f"\n{'=' * 80}\nStep 2: Downloading {len(pending_drive)} Google Drive files\n{'=' * 80}")
            
            # Create the drive_downloads directory if it doesn't exist
            os.makedirs(get_drive_downloads_dir(), exist_ok=True)
            
            # Process downloads with row context tracking
            import pandas as pd
            df = safe_csv_read(output_csv_path, 'tracking')
            
            drive_count = 0
            for row_context in pending_drive:
                if drive_count >= len(pending_drive):
                    break
                    
                # Get the actual row data
                row = df.iloc[row_context.row_index]
                drive_url = row.get('google_drive')
                main_link = row.get('link')
                
                # Check both google_drive column and main link column for Google Drive URLs
                urls_to_check = []
                
                # Add google_drive column URLs
                if pd.notna(drive_url) and str(drive_url).strip() not in ['', "'-"]:
                    # Handle multiple drive links separated by |
                    drive_urls = str(drive_url).split('|') if '|' in str(drive_url) else [str(drive_url)]
                    urls_to_check.extend(drive_urls)
                
                # Check if main link is a Google Drive file URL
                if pd.notna(main_link) and "drive.google.com/file" in str(main_link):
                    urls_to_check.append(str(main_link))
                    
                # Process all found Google Drive URLs
                for drive_link in urls_to_check:
                    drive_link = drive_link.strip()
                    if drive_link and "drive.google.com" in drive_link:
                        if logger:
                            logger.get_logger('drive').info(f"Downloading Google Drive file for {row_context.name} (Type: {row_context.type}): {drive_link}")
                        else:
                            print(f"\nDownloading Google Drive file for {row_context.name} (Type: {row_context.type}): {drive_link}")
                        
                        # Use new row-centric download function
                        result = download_drive_with_context(drive_link, row_context)
                        
                        # Update CSV with result
                        update_csv_download_status(row_context.row_index, 'drive', result)
                        
                        if result.success:
                            if logger:
                                logger.update_stats(drive_downloads=logger.run_stats['drive_downloads'] + 1)
                            drive_count += 1
                        else:
                            if logger:
                                logger.log_error(f"Failed to download Google Drive file for {row_context.name}: {result.error_message}")
                            else:
                                print(f"Warning: Failed to download Google Drive file for {row_context.name}: {result.error_message}")
            
            if logger:
                logger.get_logger('main').info(f"Completed {drive_count} Google Drive downloads with row tracking")
    
    # Step 3: Download YouTube videos using row-centric tracking
    if not args.skip_youtube:
        # Get pending YouTube downloads from CSV tracking system
        pending_youtube = get_pending_downloads(output_csv_path, 'youtube')
        
        if pending_youtube:
            # Apply max-youtube limit if specified
            if args.max_youtube is not None and args.max_youtube > 0:
                pending_youtube = pending_youtube[:args.max_youtube]
                if logger:
                    logger.get_logger('main').info(f"Step 3: Downloading {len(pending_youtube)} YouTube videos (limited by --max-youtube)")
                else:
                    print(f"\n{'=' * 80}\nStep 3: Downloading {len(pending_youtube)} YouTube videos (limited by --max-youtube)\n{'=' * 80}")
            else:
                if logger:
                    logger.get_logger('main').info(f"Step 3: Downloading {len(pending_youtube)} YouTube videos")
                else:
                    print(f"\n{'=' * 80}\nStep 3: Downloading {len(pending_youtube)} YouTube videos\n{'=' * 80}")
            
            # Create the youtube_downloads directory if it doesn't exist
            os.makedirs("youtube_downloads", exist_ok=True)
            
            # Process downloads with row context tracking
            import pandas as pd
            df = safe_csv_read(output_csv_path, 'tracking')
            
            youtube_count = 0
            for row_context in pending_youtube:
                if youtube_count >= len(pending_youtube):
                    break
                    
                # Get the actual row data
                row = df.iloc[row_context.row_index]
                youtube_url = row.get('youtube_playlist')
                
                if pd.notna(youtube_url) and str(youtube_url).strip() not in ['', "'-"]:
                    youtube_link = str(youtube_url).strip()
                    
                    if logger:
                        logger.get_logger('youtube').info(f"Downloading YouTube content for {row_context.name} (Type: {row_context.type}): {youtube_link}")
                    else:
                        print(f"\nDownloading YouTube content for {row_context.name} (Type: {row_context.type}): {youtube_link}")
                    
                    # Use new row-centric download function
                    result = download_youtube_with_context(youtube_link, row_context)
                    
                    # Update CSV with result
                    update_csv_download_status(row_context.row_index, 'youtube', result)
                    
                    if result.success:
                        if logger:
                            logger.update_stats(youtube_downloads=logger.run_stats['youtube_downloads'] + 1)
                        youtube_count += 1
                    else:
                        if logger:
                            logger.log_error(f"Failed to download YouTube content for {row_context.name}: {result.error_message}")
                        else:
                            print(f"Warning: Failed to download YouTube content for {row_context.name}: {result.error_message}")
            
            if logger:
                logger.get_logger('main').info(f"Completed {youtube_count} YouTube downloads with row tracking")
    
    # Update total rows processed if logger is available
    if logger and not args.skip_sheet:
        csv_rows = 0
        output_csv_path = config.get('paths.output_csv', 'output.csv')
        if os.path.exists(output_csv_path):
            with open(output_csv_path, 'r', newline='', encoding='utf-8') as f:
                csv_rows = sum(1 for _ in csv.DictReader(f))
        logger.update_stats(rows_processed=csv_rows)
    
    if logger:
        logger.get_logger('main').success("All steps completed successfully!")
    else:
        print(f"\n{'=' * 80}\nAll steps completed successfully!\n{'=' * 80}")
    return 0

if __name__ == "__main__":
    sys.exit(main())