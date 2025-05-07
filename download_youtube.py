#!/usr/bin/env python3
import os
import sys
import argparse
import subprocess
import time
from pathlib import Path

# Directory to save downloaded videos and transcripts
DOWNLOADS_DIR = "youtube_downloads"

def create_download_dir():
    """Create download directory if it doesn't exist"""
    downloads_path = Path(DOWNLOADS_DIR)
    if not downloads_path.exists():
        downloads_path.mkdir(parents=True)
        print(f"Created downloads directory: {DOWNLOADS_DIR}")
    return downloads_path

def download_single_video(url, video_id=None, title=None, transcript_only=False, resolution="720", output_format="mp4", transcript_format="vtt", yt_dlp_path="yt-dlp", cookies=None, no_check_certificate=False):
    """Download a single YouTube video using yt-dlp"""
    downloads_path = create_download_dir()
    
    # Base command with common options
    base_cmd = [yt_dlp_path]
    
    # Add options for handling YouTube bot detection
    base_cmd.extend(["--no-playlist", "--geo-bypass", "--ignore-errors"])
    
    # Add cookies if provided
    if cookies:
        base_cmd.extend(["--cookies", cookies])
    
    # Add no-check-certificate option if requested
    if no_check_certificate:
        base_cmd.append("--no-check-certificate")
    
    # If video_id and title not provided, get them first
    if not video_id or not title:
        # Command to get video info
        info_cmd = base_cmd.copy()
        info_cmd.extend([
            "--skip-download", 
            "--print", "id,title",
            url
        ])
        
        try:
            # Get video ID and title
            print("Attempting to get video info...")
            result = subprocess.run(info_cmd, capture_output=True, text=True, check=True)
            info = result.stdout.strip().split('\n')
            if len(info) != 2:
                print(f"Error: Couldn't get video information for {url}")
                # If we have video_id but no title, we can still proceed
                if video_id:
                    title = f"Unknown Title - {video_id}"
                    print(f"Using fallback title: {title}")
                else:
                    # Try to extract video_id from URL if not provided
                    import re
                    if 'youtube.com/watch?v=' in url:
                        match = re.search(r'watch\?v=([a-zA-Z0-9_-]{11})', url)
                        if match:
                            video_id = match.group(1)
                            title = f"Unknown Title - {video_id}"
                            print(f"Extracted video ID from URL: {video_id}")
                            print(f"Using fallback title: {title}")
                        else:
                            return None, None
                    else:
                        return None, None
            else:
                video_id, title = info
        except subprocess.CalledProcessError as e:
            print(f"Error getting video info: {e}")
            # Try to extract video_id from URL if not provided
            if not video_id and 'youtube.com/watch?v=' in url:
                import re
                match = re.search(r'watch\?v=([a-zA-Z0-9_-]{11})', url)
                if match:
                    video_id = match.group(1)
                    title = f"Unknown Title - {video_id}"
                    print(f"Extracted video ID from URL: {video_id}")
                    print(f"Using fallback title: {title}")
                else:
                    return None, None
            elif not video_id:
                return None, None
            elif not title:
                title = f"Unknown Title - {video_id}"
                print(f"Using fallback title: {title}")
    
    print(f"Video ID: {video_id}")
    print(f"Title: {title}")
    
    # Check if video already exists
    video_file = downloads_path / f"{video_id}.{output_format}"
    
    # Use proper transcript format extension
    transcript_file = downloads_path / f"{video_id}_transcript.{transcript_format}"
    
    if video_file.exists() and not transcript_only:
        print(f"Video already exists at {video_file}")
        # Still check for transcript
        if transcript_file.exists():
            print(f"Transcript already exists at {transcript_file}")
            return video_file, transcript_file
    
    # Check if transcript already exists
    has_transcript = False
    if transcript_file.exists():
        print(f"Transcript already exists at {transcript_file}")
        has_transcript = True
    else:
        # Command to download subtitles - also try to write automatic captions
        sub_cmd = base_cmd.copy()
        sub_cmd.extend([
            "--skip-download",
            "--write-subs",
            "--write-auto-subs",  # Also write automatically generated subtitles
            "--sub-langs", "en.*",
            "--sub-format", transcript_format,
            "--output", f"{downloads_path}/{video_id}",
            url
        ])
        
        try:
            print("Attempting to download transcript...")
            subprocess.run(sub_cmd, check=True)
            
            # Find the subtitle file, looking for both regular and auto-generated subtitles
            # (could be .en.vtt, .en-US.vtt, .en.auto.vtt, etc.)
            subtitle_files = list(downloads_path.glob(f"{video_id}.*.{transcript_format}"))
            
            if subtitle_files:
                # Rename the first subtitle file to our standard name
                subtitle_files[0].rename(transcript_file)
                print(f"Saved transcript to {transcript_file}")
                has_transcript = True
            else:
                # Try one more time with a broader search in case files were saved with unexpected names
                subtitle_files = list(downloads_path.glob(f"*.{transcript_format}"))
                recent_subtitle_files = sorted(
                    [f for f in subtitle_files if f.stat().st_mtime > time.time() - 300],  # Files created in last 5 minutes
                    key=lambda f: f.stat().st_mtime,
                    reverse=True
                )
                
                if recent_subtitle_files:
                    # Rename the most recently created subtitle file
                    recent_subtitle_files[0].rename(transcript_file)
                    print(f"Saved transcript to {transcript_file}")
                    has_transcript = True
                else:
                    print("No transcript found for this video")
        except subprocess.CalledProcessError:
            print("Error downloading transcript - YouTube may require authentication")
    
    # If transcript only mode, stop here
    if transcript_only:
        return None, transcript_file if has_transcript else None
    
    # If video already exists, we already checked above
    if video_file.exists():
        return video_file, transcript_file if has_transcript else None
    
    # Command to download video
    video_cmd = base_cmd.copy()
    video_cmd.extend([
        "-f", f"bestvideo[height<={resolution}][ext={output_format}]+bestaudio[ext=m4a]/best[height<={resolution}][ext={output_format}]",
        "--merge-output-format", output_format,
        "--output", str(video_file),
        url
    ])
    
    try:
        print(f"\nDownloading video in {resolution}p {output_format} format...")
        subprocess.run(video_cmd, check=True)
        print(f"Video downloaded to {video_file}")
        return video_file, transcript_file if has_transcript else None
    except subprocess.CalledProcessError as e:
        print(f"Error downloading video: {e}")
        print("YouTube may require authentication or the video might be restricted")
        return None, transcript_file if has_transcript else None


def download_video(url, transcript_only=False, resolution="720", output_format="mp4", transcript_format="vtt", cookies=None, no_check_certificate=False):
    """Download a YouTube video or playlist using yt-dlp"""
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
            print("No valid video IDs found in playlist URL")
            return None, None
        
        print(f"Found {len(video_ids)} videos in playlist URL")
        
        # Process each video separately
        successful_video_files = []
        successful_transcript_files = []
        
        for i, vid in enumerate(video_ids):
            print(f"\nProcessing video {i+1}/{len(video_ids)}: {vid}")
            video_url = f"https://www.youtube.com/watch?v={vid}"
            video_file, transcript_file = download_single_video(
                video_url, 
                video_id=vid, 
                title=None,  # Will be fetched inside the function
                transcript_only=transcript_only,
                resolution=resolution,
                output_format=output_format,
                transcript_format=transcript_format,
                yt_dlp_path=yt_dlp_path,
                cookies=cookies,
                no_check_certificate=no_check_certificate
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
            transcript_format=transcript_format,
            yt_dlp_path=yt_dlp_path,
            cookies=cookies,
            no_check_certificate=no_check_certificate
        )

def main():
    parser = argparse.ArgumentParser(description='Download YouTube videos and transcripts using yt-dlp')
    parser.add_argument('url', help='YouTube video URL')
    parser.add_argument('--transcript-only', action='store_true', help='Download transcript only, not video')
    parser.add_argument('--resolution', default='720', help='Preferred video resolution (default: 720p)')
    parser.add_argument('--format', choices=['mp4', 'webm'], default='mp4', help='Video format (default: mp4)')
    parser.add_argument('--transcript-format', choices=['vtt', 'srt'], default='vtt', help='Transcript format (default: vtt)')
    parser.add_argument('--cookies', help='Path to a cookies file for YouTube authentication')
    parser.add_argument('--no-check-certificate', action='store_true', help='Ignore SSL certificate validation')
    parser.add_argument('--extract-audio', action='store_true', help='Extract audio only from the video')
    
    args = parser.parse_args()
    
    # Process the URL
    video_file, transcript_file = download_video(
        args.url, 
        args.transcript_only,
        args.resolution,
        args.format,
        args.transcript_format,
        args.cookies,
        args.no_check_certificate
    )
    
    # Summary
    print("\nDownload Summary:")
    if video_file:
        print(f"✅ Video: {video_file}")
    elif not args.transcript_only:
        print("❌ Video: Failed to download")
        print("   This may be due to YouTube requiring authentication.")
        print("   Try adding --cookies COOKIES_FILE argument (see yt-dlp documentation)")
        
    if transcript_file:
        print(f"✅ Transcript: {transcript_file}")
    else:
        print("❌ Transcript: Not available or failed to download")

if __name__ == "__main__":
    main()