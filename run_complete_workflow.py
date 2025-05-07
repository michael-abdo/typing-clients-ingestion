#!/usr/bin/env python3
import os
import sys
import argparse
import subprocess
import csv
from pathlib import Path

def run_process(command, description=None):
    """Run a process with the given command and print its output in real-time"""
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

def get_unprocessed_links(csv_file, link_type="google_drive"):
    """
    Extract links that haven't been processed yet from the CSV file
    
    Args:
        csv_file: Path to the CSV file
        link_type: Type of links to extract ("google_drive" or "youtube_playlist")
    
    Returns:
        List of links that haven't been processed yet
    """
    if not os.path.exists(csv_file):
        print(f"CSV file not found: {csv_file}")
        return []
    
    links = []
    
    with open(csv_file, 'r', newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for row in reader:
            if link_type == "google_drive" and row.get("google_drive") and row.get("google_drive") != "-":
                # Split multiple drive links
                drive_links = row.get("google_drive").split("|")
                links.extend(drive_links)
            
            elif link_type == "youtube_playlist" and row.get("youtube_playlist") and row.get("youtube_playlist") != "-":
                links.append(row.get("youtube_playlist"))
    
    # Filter out links that have already been processed
    if link_type == "google_drive":
        # Check for downloaded files in drive_downloads directory
        download_dir = Path("drive_downloads")
        if download_dir.exists():
            downloaded_ids = set()
            for item in download_dir.glob("*_metadata.json"):
                file_id = item.name.split("_metadata.json")[0]
                downloaded_ids.add(file_id)
            
            # Filter links that haven't been downloaded yet
            filtered_links = []
            for link in links:
                # Extract file ID from the URL
                if "/file/d/" in link:
                    file_id = link.split("/file/d/")[1].split("/")[0]
                elif "id=" in link:
                    file_id = link.split("id=")[1].split("&")[0]
                else:
                    file_id = None
                
                if file_id and file_id not in downloaded_ids:
                    filtered_links.append(link)
            
            links = filtered_links
    
    elif link_type == "youtube_playlist":
        # Check for downloaded videos in youtube_downloads directory
        download_dir = Path("youtube_downloads")
        if download_dir.exists():
            downloaded_ids = set()
            for item in download_dir.glob("*.mp4"):
                video_id = item.name.split(".mp4")[0]
                downloaded_ids.add(video_id)
            
            # For YouTube, we'll pass along all the playlist links
            # but we track which videos have already been downloaded for logging
            num_downloaded = len(downloaded_ids)
            if num_downloaded > 0:
                print(f"Found {num_downloaded} already downloaded YouTube videos")
                
            # The download_youtube.py script now checks for existing files before downloading
            # so we don't need to filter links here, but we'll keep track of them
    
    return links

def main():
    parser = argparse.ArgumentParser(description="Run the complete workflow: scrape Google Sheet, download Google Drive files and YouTube videos")
    parser.add_argument("--max-rows", type=int, default=None, help="Maximum number of rows to process for Google Sheet scraping")
    parser.add_argument("--reset", action="store_true", help="Reset processed status and reprocess all rows")
    parser.add_argument("--skip-sheet", action="store_true", help="Skip Google Sheet scraping")
    parser.add_argument("--skip-drive", action="store_true", help="Skip Google Drive downloads")
    parser.add_argument("--skip-youtube", action="store_true", help="Skip YouTube downloads")
    parser.add_argument("--max-youtube", type=int, default=None, help="Maximum number of YouTube videos to download")
    parser.add_argument("--max-drive", type=int, default=None, help="Maximum number of Google Drive files to download")
    
    args = parser.parse_args()
    
    # Step 1: Scrape Google Sheet and extract links
    if not args.skip_sheet:
        command = [sys.executable, "master_scraper.py", "--force-download"]
        if args.max_rows is not None:
            command.extend(["--max-rows", str(args.max_rows)])
        if args.reset:
            command.append("--reset")
        
        exit_code = run_process(command, "Step 1: Scraping Google Sheet and extracting links")
        if exit_code != 0:
            print(f"Error in Step 1: Google Sheet scraping failed with exit code {exit_code}")
            return exit_code
    
    # Step 2: Download Google Drive files for unprocessed links
    if not args.skip_drive:
        drive_links = get_unprocessed_links("output.csv", "google_drive")
        if drive_links:
            # Apply max-drive limit if specified
            if args.max_drive is not None and args.max_drive > 0:
                drive_links = drive_links[:args.max_drive]
                print(f"\n{'=' * 80}\nStep 2: Downloading {len(drive_links)} Google Drive files (limited by --max-drive)\n{'=' * 80}")
            else:
                print(f"\n{'=' * 80}\nStep 2: Downloading {len(drive_links)} Google Drive files\n{'=' * 80}")
            
            # Create the drive_downloads directory if it doesn't exist
            os.makedirs("drive_downloads", exist_ok=True)
            
            for i, link in enumerate(drive_links):
                if link and "drive.google.com" in link:
                    print(f"\nDownloading Google Drive file {i+1}/{len(drive_links)}: {link}")
                    command = [sys.executable, "download_drive.py", link, "--metadata"]
                    exit_code = run_process(command)
                    if exit_code != 0:
                        print(f"Warning: Failed to download Google Drive file: {link}")
    
    # Step 3: Download YouTube videos for unprocessed links
    if not args.skip_youtube:
        youtube_links = get_unprocessed_links("output.csv", "youtube_playlist")
        if youtube_links:
            # Clean YouTube playlist URLs - remove newlines and extract only valid video IDs
            cleaned_youtube_links = []
            import re
            for link in youtube_links:
                if link and "youtube.com" in link:
                    if "watch_videos?video_ids=" in link:
                        # Extract the video IDs, removing any newlines or other unwanted characters
                        video_ids_part = link.split("watch_videos?video_ids=")[1]
                        # Extract only valid video ID characters
                        video_ids = re.findall(r'[a-zA-Z0-9_-]{11}', video_ids_part)
                        if video_ids:
                            # Reconstruct the URL properly
                            cleaned_link = f"https://www.youtube.com/watch_videos?video_ids={','.join(video_ids)}"
                            cleaned_youtube_links.append(cleaned_link)
                    else:
                        cleaned_youtube_links.append(link)
            
            # Replace the original links with the cleaned ones
            youtube_links = cleaned_youtube_links
            
            # Apply max-youtube limit if specified
            if args.max_youtube is not None and args.max_youtube > 0:
                youtube_links = youtube_links[:args.max_youtube]
                print(f"\n{'=' * 80}\nStep 3: Downloading YouTube videos from {len(youtube_links)} playlists (limited by --max-youtube)\n{'=' * 80}")
            else:
                print(f"\n{'=' * 80}\nStep 3: Downloading YouTube videos from {len(youtube_links)} playlists\n{'=' * 80}")
            
            # Create the youtube_downloads directory if it doesn't exist
            os.makedirs("youtube_downloads", exist_ok=True)
            
            for i, link in enumerate(youtube_links):
                if link:
                    print(f"\nDownloading YouTube playlist {i+1}/{len(youtube_links)}: {link}")
                    # Add more robust options for YouTube download
                    command = [
                        sys.executable, 
                        "download_youtube.py", 
                        link, 
                        "--transcript-format", "vtt",
                        "--no-check-certificate"  # Add this to bypass certificate issues
                    ]
                    
                    # Check if cookies file exists and add it if it does
                    cookies_path = os.path.join(os.path.dirname(os.path.realpath(__file__)), "youtube_cookies.txt")
                    if os.path.exists(cookies_path):
                        command.extend(["--cookies", cookies_path])
                    
                    exit_code = run_process(command)
                    if exit_code != 0:
                        print(f"Warning: Failed to download YouTube video: {link}")
                        print("YouTube may require authentication. Consider creating a 'youtube_cookies.txt' file in the project directory.")
    
    print(f"\n{'=' * 80}\nAll steps completed successfully!\n{'=' * 80}")
    return 0

if __name__ == "__main__":
    sys.exit(main())