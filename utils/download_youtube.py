#!/usr/bin/env python3
import os
import sys
import argparse
import subprocess
import time
from pathlib import Path
from logger import setup_component_logging

# Directory to save downloaded videos and transcripts
DOWNLOADS_DIR = "youtube_downloads"

def create_download_dir(logger=None):
    """Create download directory if it doesn't exist"""
    downloads_path = Path(DOWNLOADS_DIR)
    if not downloads_path.exists():
        downloads_path.mkdir(parents=True)
        if logger:
            logger.info(f"Created downloads directory: {DOWNLOADS_DIR}")
    return downloads_path

def download_single_video(url, video_id=None, title=None, transcript_only=False, resolution="720", output_format="mp4", yt_dlp_path="yt-dlp", logger=None):
    """Download a single YouTube video using yt-dlp"""
    if not logger:
        logger = setup_component_logging('youtube')
    
    downloads_path = create_download_dir(logger)
    
    # If video_id and title not provided, get them first
    if not video_id or not title:
        # Command to get video info
        info_cmd = [
            yt_dlp_path, 
            "--skip-download", 
            "--print", "id,title",
            url
        ]
        
        try:
            # Get video ID and title
            result = subprocess.run(info_cmd, capture_output=True, text=True, check=True)
            info = result.stdout.strip().split('\n')
            if len(info) != 2:
                logger.error(f"Couldn't get video information for {url}")
                return None, None
                
            video_id, title = info
        except subprocess.CalledProcessError as e:
            logger.error(f"Error getting video info: {e}")
            return None, None
    
    logger.info(f"Video ID: {video_id}")
    logger.info(f"Title: {title}")
    
    # Prepare transcript file path
    transcript_file = downloads_path / f"{video_id}_transcript.{output_format}"
    
    # Try to download subtitles
    has_transcript = False
    if output_format == "srt":
        sub_format = "srt"
    else:
        # Default to vtt format for better compatibility
        sub_format = "vtt"
        
    # Command to download subtitles - also try to write automatic captions
    sub_cmd = [
        yt_dlp_path,
        "--skip-download",
        "--write-subs",
        "--write-auto-subs",  # Also write automatically generated subtitles
        "--sub-langs", "en.*",
        "--sub-format", sub_format,
        "--output", f"{downloads_path}/{video_id}",
        url
    ]
    
    try:
        logger.info("Attempting to download transcript...")
        subprocess.run(sub_cmd, check=True)
        
        # Find the subtitle file, looking for both regular and auto-generated subtitles
        # (could be .en.vtt, .en-US.vtt, .en.auto.vtt, etc.)
        subtitle_files = list(downloads_path.glob(f"{video_id}.*.{sub_format}"))
        
        if subtitle_files:
            # Rename the first subtitle file to our standard name
            subtitle_files[0].rename(transcript_file)
            logger.success(f"Saved transcript to {transcript_file}")
            has_transcript = True
        else:
            # Try one more time with a broader search in case files were saved with unexpected names
            subtitle_files = list(downloads_path.glob(f"*.{sub_format}"))
            recent_subtitle_files = sorted(
                [f for f in subtitle_files if f.stat().st_mtime > time.time() - 300],  # Files created in last 5 minutes
                key=lambda f: f.stat().st_mtime,
                reverse=True
            )
            
            if recent_subtitle_files:
                # Rename the most recently created subtitle file
                recent_subtitle_files[0].rename(transcript_file)
                logger.success(f"Saved transcript to {transcript_file}")
                has_transcript = True
            else:
                logger.warning("No transcript found for this video")
    except subprocess.CalledProcessError:
        logger.error("Error downloading transcript")
    
    # If transcript only mode, stop here
    if transcript_only:
        return None, transcript_file if has_transcript else None
    
    # Command to download video
    video_file = downloads_path / f"{video_id}.{output_format}"
    video_cmd = [
        yt_dlp_path,
        "-f", f"bestvideo[height<={resolution}][ext={output_format}]+bestaudio[ext=m4a]/best[height<={resolution}][ext={output_format}]",
        "--merge-output-format", output_format,
        "--output", str(video_file),
        url
    ]
    
    try:
        logger.info(f"Downloading video in {resolution}p {output_format} format...")
        subprocess.run(video_cmd, check=True)
        logger.success(f"Video downloaded to {video_file}")
        return video_file, transcript_file if has_transcript else None
    except subprocess.CalledProcessError as e:
        logger.error(f"Error downloading video: {e}")
        return None, transcript_file if has_transcript else None


def download_video(url, transcript_only=False, resolution="720", output_format="mp4", logger=None):
    """Download a YouTube video or playlist using yt-dlp"""
    if not logger:
        logger = setup_component_logging('youtube')
    # Get the path to yt-dlp in the virtual environment
    import os
    import sys
    
    # First check if we're in a virtual environment
    if hasattr(sys, 'real_prefix') or (hasattr(sys, 'base_prefix') and sys.base_prefix != sys.prefix):
        # We're in a virtual environment
        venv_path = sys.prefix
        yt_dlp_path = os.path.join(venv_path, "bin", "yt-dlp")
    else:
        # Not in a virtual environment, use command as-is
        yt_dlp_path = "yt-dlp"
    
    # Check if this is a playlist
    if "watch_videos?video_ids=" in url:
        # Extract video IDs from the playlist URL
        import re
        video_ids_part = url.split("watch_videos?video_ids=")[1]
        video_ids = re.findall(r'[a-zA-Z0-9_-]{11}', video_ids_part)
        
        if not video_ids:
            logger.error("No valid video IDs found in playlist URL")
            return None, None
        
        # Remove duplicates while preserving order
        seen = set()
        unique_video_ids = []
        for vid in video_ids:
            if vid not in seen:
                seen.add(vid)
                unique_video_ids.append(vid)
        
        if len(video_ids) != len(unique_video_ids):
            logger.info(f"Removed {len(video_ids) - len(unique_video_ids)} duplicate video IDs")
        
        video_ids = unique_video_ids
        logger.info(f"Found {len(video_ids)} unique videos in playlist URL")
        
        # Process each video separately
        successful_video_files = []
        successful_transcript_files = []
        
        for i, vid in enumerate(video_ids):
            logger.info(f"Processing video {i+1}/{len(video_ids)}: {vid}")
            video_url = f"https://www.youtube.com/watch?v={vid}"
            video_file, transcript_file = download_single_video(
                video_url, 
                video_id=vid, 
                title=None,  # Will be fetched inside the function
                transcript_only=transcript_only,
                resolution=resolution,
                output_format=output_format,
                yt_dlp_path=yt_dlp_path,
                logger=logger
            )
            
            if video_file:
                successful_video_files.append(video_file)
            if transcript_file:
                successful_transcript_files.append(transcript_file)
        
        # Return the first successful files or None if none were successful
        return (
            successful_video_files[0] if successful_video_files else None,
            successful_transcript_files[0] if successful_transcript_files else None
        )
    else:
        # Regular single video
        return download_single_video(
            url, 
            transcript_only=transcript_only,
            resolution=resolution,
            output_format=output_format,
            yt_dlp_path=yt_dlp_path,
            logger=logger
        )

def main():
    # Setup logging
    logger = setup_component_logging('youtube')
    
    parser = argparse.ArgumentParser(description='Download YouTube videos and transcripts using yt-dlp')
    parser.add_argument('url', help='YouTube video URL')
    parser.add_argument('--transcript-only', action='store_true', help='Download transcript only, not video')
    parser.add_argument('--resolution', default='720', help='Preferred video resolution (default: 720p)')
    parser.add_argument('--format', choices=['mp4', 'webm'], default='mp4', help='Video format (default: mp4)')
    parser.add_argument('--transcript-format', choices=['vtt', 'srt'], default='vtt', help='Transcript format (default: vtt)')
    
    args = parser.parse_args()
    
    # Process the URL
    video_file, transcript_file = download_video(
        args.url, 
        args.transcript_only,
        args.resolution,
        args.format,
        logger
    )
    
    # Summary
    logger.info("\nDownload Summary:")
    if video_file:
        logger.success(f"Video: {video_file}")
    elif not args.transcript_only:
        logger.error("Video: Failed to download")
        
    if transcript_file:
        logger.success(f"Transcript: {transcript_file}")
    else:
        logger.warning("Transcript: Not available or failed to download")

if __name__ == "__main__":
    main()