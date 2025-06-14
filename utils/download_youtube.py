#!/usr/bin/env python3
import os
import sys
import argparse
import subprocess
import time
from pathlib import Path
try:
    from logger import setup_component_logging
    from validation import validate_youtube_url, validate_file_path, ValidationError
    from retry_utils import retry_subprocess, retry_with_backoff
    from file_lock import file_lock, safe_file_operation
    from config import get_config, get_youtube_downloads_dir, get_timeout
    from rate_limiter import rate_limit, wait_for_rate_limit
    from row_context import RowContext, DownloadResult
    from sanitization import sanitize_error_message, SafeDownloadError
except ImportError:
    from .logger import setup_component_logging
    from .validation import validate_youtube_url, validate_file_path, ValidationError
    from .retry_utils import retry_subprocess, retry_with_backoff
    from .file_lock import file_lock, safe_file_operation
    from .config import get_config, get_youtube_downloads_dir, get_timeout
    from .rate_limiter import rate_limit, wait_for_rate_limit
    from .row_context import RowContext, DownloadResult
    from .sanitization import sanitize_error_message, SafeDownloadError

# Get configuration
config = get_config()

# Directory to save downloaded videos and transcripts (from config)
DOWNLOADS_DIR = get_youtube_downloads_dir()

def create_download_dir(logger=None):
    """Create download directory if it doesn't exist"""
    downloads_path = Path(DOWNLOADS_DIR)
    if not downloads_path.exists():
        downloads_path.mkdir(parents=True)
        if logger:
            logger.info(f"Created downloads directory: {DOWNLOADS_DIR}")
    return downloads_path

@rate_limit('youtube')
def download_single_video(url, video_id=None, title=None, transcript_only=False, resolution=None, output_format=None, yt_dlp_path="yt-dlp", logger=None):
    """Download a single YouTube video using yt-dlp"""
    if not logger:
        logger = setup_component_logging('youtube')
    
    # Use config defaults if not provided
    if resolution is None:
        resolution = config.get('downloads.youtube.default_resolution', '720')
    if output_format is None:
        output_format = config.get('downloads.youtube.default_format', 'mp4')
    
    # Validate URL
    try:
        url, validated_video_id = validate_youtube_url(url)
        if not video_id:
            video_id = validated_video_id
    except ValidationError as e:
        safe_error = sanitize_error_message(str(e))
        logger.error(f"Invalid YouTube URL: {safe_error}")
        return None, None
    
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
            # Get video ID and title with retry
            result = retry_subprocess(
                info_cmd,
                capture_output=True,
                text=True,
                max_attempts=3,
                base_delay=2.0,
                logger=logger
            )
            info = result.stdout.strip().split('\n')
            if len(info) != 2:
                logger.error(f"Couldn't get video information for {url}")
                return None, None
                
            video_id, title = info
        except Exception as e:
            safe_error = sanitize_error_message(str(e))
            logger.error(f"Error getting video info after retries: {safe_error}")
            return None, None
    
    logger.info(f"Video ID: {video_id}")
    logger.info(f"Title: {title}")
    
    # Try to download subtitles
    has_transcript = False
    if output_format == "srt":
        sub_format = "srt"
    else:
        # Get subtitle format from config
        sub_format = config.get('downloads.youtube.subtitle_format', 'vtt')
    
    # Prepare transcript file path with correct extension
    transcript_file = downloads_path / f"{video_id}_transcript.{sub_format}"
    
    # Use file locking to prevent race conditions when downloading transcripts
    lock_file = downloads_path / f".{video_id}_transcript.lock"
    
    with file_lock(lock_file, exclusive=True, timeout=get_timeout('video_download'), logger=logger):
        # Check if transcript already exists
        if transcript_file.exists():
            logger.info(f"Transcript already exists: {transcript_file}")
            has_transcript = True
        else:
            # Command to download subtitles with our specific naming convention
            sub_cmd = [
                yt_dlp_path,
                "--skip-download",
                "--write-subs",
                "--write-auto-subs",  # Also write automatically generated subtitles
                "--sub-langs", config.get('downloads.youtube.subtitle_languages', 'en.*'),
                "--sub-format", sub_format,
                "--convert-subs", sub_format,  # Ensure consistent format
                "--output", f"{downloads_path}/{video_id}_transcript",  # Use our naming convention directly
                url
            ]
            
            try:
                logger.info("Attempting to download transcript...")
                retry_subprocess(
                    sub_cmd,
                    max_attempts=3,
                    base_delay=2.0,
                    logger=logger
                )
                
                # Look for all subtitle files that yt-dlp might have created
                # Pattern 1: Our naming with language codes
                subtitle_files = list(downloads_path.glob(f"{video_id}_transcript.*.{sub_format}"))
                
                if not subtitle_files:
                    # Pattern 2: Standard yt-dlp naming
                    subtitle_files = list(downloads_path.glob(f"{video_id}.*.{sub_format}"))
                
                if subtitle_files:
                    # If our target file doesn't exist, create it from the best available subtitle
                    if not transcript_file.exists():
                        # Prefer files with 'orig' in the name as they're unprocessed
                        orig_files = [f for f in subtitle_files if '-orig' in f.name or '.orig' in f.name]
                        if orig_files:
                            source_file = orig_files[0]
                        else:
                            # Otherwise use the largest file
                            source_file = max(subtitle_files, key=lambda f: f.stat().st_size)
                        
                        source_file.rename(transcript_file)
                        logger.success(f"Saved transcript to {transcript_file}")
                    
                    # Clean up any remaining language-coded files
                    for f in subtitle_files:
                        if f.exists() and f != transcript_file:
                            f.unlink()
                            logger.debug(f"Cleaned up: {f.name}")
                    
                    has_transcript = True
                elif transcript_file.exists():
                    # File was created directly with correct name
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
    video_lock_file = downloads_path / f".{video_id}_video.lock"
    
    # Use file locking to prevent race conditions when downloading videos
    with file_lock(video_lock_file, exclusive=True, timeout=300.0, logger=logger):  # 5 min timeout for videos
        # Check if video already exists
        if video_file.exists():
            logger.info(f"Video already exists: {video_file}")
            return video_file, transcript_file if has_transcript else None
        
        video_cmd = [
            yt_dlp_path,
            "-f", f"bestvideo[height<={resolution}]+bestaudio/best[height<={resolution}]/best",
            "--merge-output-format", output_format,
            "--output", str(video_file),
            url
        ]
        
        try:
            logger.info(f"Downloading video in {resolution}p {output_format} format...")
            retry_subprocess(
                video_cmd,
                max_attempts=3,
                base_delay=5.0,  # Longer delay for video downloads
                logger=logger
            )
            logger.success(f"Video downloaded to {video_file}")
            return video_file, transcript_file if has_transcript else None
        except Exception as e:
            safe_error = sanitize_error_message(str(e))
            logger.error(f"Error downloading video after retries: {safe_error}")
            # Still return transcript if we got it
            return None, transcript_file if has_transcript else None


def download_youtube_with_context(url: str, row_context: RowContext, 
                                 transcript_only=False, resolution=None, output_format=None) -> DownloadResult:
    """Download YouTube video with full row context tracking"""
    logger = setup_component_logging('youtube')
    
    # Use config defaults if not provided
    if resolution is None:
        resolution = config.get('downloads.youtube.default_resolution', '720')
    if output_format is None:
        output_format = config.get('downloads.youtube.default_format', 'mp4')
    
    logger.info(f"Starting YouTube download for {row_context.name} (Row {row_context.row_id}, Type: {row_context.type})")
    
    try:
        # Download using existing functionality
        video_file, transcript_file = download_video(
            url, transcript_only, resolution, output_format, logger
        )
        
        # Extract video ID from URL for tracking
        video_id = None
        try:
            if "watch_videos?video_ids=" in url:
                import re
                video_ids = re.findall(r'[a-zA-Z0-9_-]{11}', url)
                video_id = video_ids[0] if video_ids else None
            else:
                _, video_id = validate_youtube_url(url)
        except:
            pass
        
        # Collect downloaded files
        downloaded_files = []
        if video_file and os.path.exists(video_file):
            downloaded_files.append(os.path.basename(video_file))
        if transcript_file and os.path.exists(transcript_file):
            downloaded_files.append(os.path.basename(transcript_file))
        
        # Create context-aware metadata file
        metadata_filename = None
        if video_id:
            suffix = row_context.to_filename_suffix()
            metadata_filename = f"{video_id}{suffix}_metadata.json"
        
        success = len(downloaded_files) > 0
        error_msg = None if success else "No files were downloaded"
        
        result = DownloadResult(
            success=success,
            files_downloaded=downloaded_files,
            media_id=video_id,
            error_message=error_msg,
            metadata_file=metadata_filename,
            row_context=row_context,
            download_type='youtube',
            permanent_failure=False
        )
        
        # Save metadata with row context
        if metadata_filename:
            result.save_metadata(DOWNLOADS_DIR)
            
        logger.info(f"YouTube download completed for {row_context.name}: {len(downloaded_files)} files")
        return result
        
    except Exception as e:
        # Sanitize error message to prevent CSV corruption
        safe_error = sanitize_error_message(str(e))
        logger.error(f"YouTube download failed for {row_context.name} (Row {row_context.row_id}): {safe_error}")
        
        # Check for permanent failure conditions using sanitized error
        error_msg = safe_error.lower()
        is_permanent = any(phrase in error_msg for phrase in [
            'video unavailable', 'removed by uploader', 'deleted', 
            'private video', 'video not available', 'this video has been removed'
        ])
        
        return DownloadResult(
            success=False,
            files_downloaded=[],
            media_id=None,
            error_message=safe_error,
            metadata_file=None,
            row_context=row_context,
            download_type='youtube',
            permanent_failure=is_permanent
        )


def download_video(url, transcript_only=False, resolution="720", output_format="mp4", logger=None):
    """Download a YouTube video or playlist using yt-dlp"""
    if not logger:
        logger = setup_component_logging('youtube')
    
    # Basic URL validation first
    try:
        from validation import validate_url
        url = validate_url(url, allowed_domains=['youtube.com', 'youtu.be'])
    except ValidationError as e:
        safe_error = sanitize_error_message(str(e))
        logger.error(f"Invalid URL: {safe_error}")
        return None, None
    except ImportError:
        # Fallback if validation module not available
        pass
    
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