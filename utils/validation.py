"""Input validation and sanitization utilities to prevent security vulnerabilities"""
import re
import os
from urllib.parse import urlparse, parse_qs
from pathlib import Path


class ValidationError(Exception):
    """Raised when input validation fails"""
    pass


def validate_url(url, allowed_domains=None):
    """
    Validate and sanitize a URL to prevent injection attacks
    
    Args:
        url: The URL to validate
        allowed_domains: Optional list of allowed domains (e.g., ['youtube.com', 'docs.google.com'])
    
    Returns:
        Sanitized URL string
        
    Raises:
        ValidationError: If URL is invalid or potentially malicious
    """
    if not url or not isinstance(url, str):
        raise ValidationError("URL must be a non-empty string")
    
    # Remove any null bytes or control characters
    url = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', url)
    
    # Basic URL validation
    try:
        parsed = urlparse(url)
    except Exception:
        raise ValidationError(f"Invalid URL format: {url}")
    
    # Ensure URL has a scheme and netloc
    if not parsed.scheme or not parsed.netloc:
        raise ValidationError(f"URL must include protocol and domain: {url}")
    
    # Only allow http/https
    if parsed.scheme not in ('http', 'https'):
        raise ValidationError(f"Only HTTP/HTTPS URLs are allowed: {url}")
    
    # Check for suspicious characters that might indicate injection
    suspicious_patterns = [
        r'[;|`$]',  # Command separators (removed & which is valid in URLs)
        r'\.\./',     # Path traversal
        r'%00',       # Null byte
        r'\$\(',      # Command substitution
        r'\{.*\}',    # Variable expansion
    ]
    
    for pattern in suspicious_patterns:
        if re.search(pattern, url, re.IGNORECASE):
            raise ValidationError(f"URL contains suspicious characters: {url}")
    
    # Validate against allowed domains if specified
    if allowed_domains:
        domain = parsed.netloc.lower()
        # Remove www. prefix for comparison
        domain = domain.replace('www.', '')
        
        if not any(domain.endswith(allowed_domain.lower()) for allowed_domain in allowed_domains):
            raise ValidationError(f"URL domain not in allowed list: {domain}")
    
    return url


def validate_youtube_url(url):
    """
    Validate a YouTube URL and extract video ID
    
    Returns:
        tuple: (sanitized_url, video_id)
    """
    url = validate_url(url, allowed_domains=['youtube.com', 'youtu.be'])
    
    # Extract video ID
    video_id = None
    
    if 'youtu.be' in url:
        # Match video ID followed by end of string, /, ? or #
        match = re.search(r'youtu\.be/([a-zA-Z0-9_-]{11})(?:[/?#]|$)', url)
        if match:
            video_id = match.group(1)
    else:
        # Match video ID followed by &, #, or end of string
        match = re.search(r'[?&]v=([a-zA-Z0-9_-]{11})(?:[&#]|$)', url)
        if match:
            video_id = match.group(1)
    
    if not video_id:
        raise ValidationError(f"Could not extract valid YouTube video ID from URL: {url}")
    
    # Validate video ID format
    if not re.match(r'^[a-zA-Z0-9_-]{11}$', video_id):
        raise ValidationError(f"Invalid YouTube video ID format: {video_id}")
    
    return url, video_id


def validate_google_drive_url(url):
    """
    Validate a Google Drive URL and extract file ID
    
    Returns:
        tuple: (sanitized_url, file_id)
    """
    url = validate_url(url, allowed_domains=['drive.google.com', 'docs.google.com', 'drive.usercontent.google.com'])
    
    # Extract file ID from various Google Drive URL formats
    file_id = None
    
    # Format 1: /file/d/{id}/view
    match = re.search(r'/file/d/([a-zA-Z0-9_-]+)', url)
    if match:
        file_id = match.group(1)
    
    # Format 2: ?id={id}
    if not file_id:
        parsed = urlparse(url)
        params = parse_qs(parsed.query)
        if 'id' in params:
            file_id = params['id'][0]
    
    # Format 3: /document/d/{id}/
    if not file_id:
        match = re.search(r'/document/d/([a-zA-Z0-9_-]+)', url)
        if match:
            file_id = match.group(1)
    
    if not file_id:
        raise ValidationError(f"Could not extract file ID from Google Drive URL: {url}")
    
    # Validate file ID format (alphanumeric, underscore, hyphen)
    if not re.match(r'^[a-zA-Z0-9_-]+$', file_id):
        raise ValidationError(f"Invalid Google Drive file ID format: {file_id}")
    
    return url, file_id


def validate_youtube_playlist_url(url):
    """
    Validate a YouTube playlist URL (watch_videos format)
    
    Returns:
        tuple: (sanitized_url, list of video_ids)
    """
    url = validate_url(url, allowed_domains=['youtube.com'])
    
    video_ids = []
    
    # Check for watch_videos format
    if 'watch_videos?video_ids=' in url:
        match = re.search(r'watch_videos\?video_ids=([a-zA-Z0-9_,-]+)', url)
        if match:
            ids_string = match.group(1)
            # Split by comma and validate each ID
            potential_ids = ids_string.split(',')
            for pid in potential_ids:
                # Validate each video ID using the dedicated function
                try:
                    validate_youtube_video_id(pid)
                    video_ids.append(pid)
                except ValidationError:
                    # Skip invalid IDs instead of failing
                    continue
    
    # Check for playlist format
    elif 'playlist?list=' in url:
        match = re.search(r'playlist\?list=([a-zA-Z0-9_-]+)', url)
        if match:
            # For regular playlists, we can't extract individual video IDs
            # Just validate the playlist ID format
            playlist_id = match.group(1)
            if not re.match(r'^[a-zA-Z0-9_-]+$', playlist_id):
                raise ValidationError(f"Invalid YouTube playlist ID format: {playlist_id}")
            return url, []
    
    if not video_ids and 'playlist?list=' not in url:
        raise ValidationError(f"No valid YouTube video IDs found in playlist URL: {url}")
    
    # Reconstruct clean URL with only valid video IDs
    if video_ids:
        clean_url = f"https://www.youtube.com/watch_videos?video_ids={','.join(video_ids)}"
        return clean_url, video_ids
    
    return url, video_ids


def validate_youtube_video_id(video_id):
    """
    Validate a YouTube video ID format
    
    Returns:
        bool: True if valid, raises ValidationError if not
    """
    if not video_id:
        raise ValidationError("Video ID cannot be empty")
    
    if not isinstance(video_id, str):
        raise ValidationError("Video ID must be a string")
    
    # YouTube video IDs are exactly 11 characters: alphanumeric, underscore, or hyphen
    if not re.match(r'^[a-zA-Z0-9_-]{11}$', video_id):
        raise ValidationError(f"Invalid YouTube video ID format: {video_id}. Must be exactly 11 characters.")
    
    # Additional check for common patterns that indicate corrupted IDs
    # These patterns suggest the ID contains control characters or encoding issues
    if any(pattern in video_id.lower() for pattern in ['u00', '\\u', 'origin', 'null']):
        raise ValidationError(f"Video ID appears to be corrupted: {video_id}")
    
    return True


def validate_file_path(path, base_dir=None, must_exist=False):
    """
    Validate a file path to prevent path traversal attacks
    
    Args:
        path: The file path to validate
        base_dir: Optional base directory to ensure path stays within
        must_exist: Whether the path must already exist
    
    Returns:
        Sanitized absolute Path object
    """
    if not path or not isinstance(path, (str, Path)):
        raise ValidationError("Path must be a non-empty string or Path object")
    
    # Convert to Path object
    path = Path(path)
    
    # Resolve to absolute path (this also normalizes .. and .)
    try:
        abs_path = path.resolve()
    except Exception:
        raise ValidationError(f"Invalid path: {path}")
    
    # Check for null bytes
    if '\x00' in str(abs_path):
        raise ValidationError("Path contains null bytes")
    
    # If base_dir is specified, ensure path is within it
    if base_dir:
        base_dir = Path(base_dir).resolve()
        try:
            abs_path.relative_to(base_dir)
        except ValueError:
            raise ValidationError(f"Path escapes base directory: {abs_path}")
    
    # Check if path exists if required
    if must_exist and not abs_path.exists():
        raise ValidationError(f"Path does not exist: {abs_path}")
    
    return abs_path


def validate_filename(filename):
    """
    Validate a filename to ensure it's safe
    
    Returns:
        Sanitized filename
    """
    if not filename or not isinstance(filename, str):
        raise ValidationError("Filename must be a non-empty string")
    
    # Remove any path components
    filename = os.path.basename(filename)
    
    # Remove dangerous characters
    # Allow alphanumeric, spaces, dots, underscores, hyphens
    filename = re.sub(r'[^a-zA-Z0-9._\- ]', '', filename)
    
    # Ensure filename is not empty after sanitization
    if not filename or filename in ('.', '..'):
        raise ValidationError("Invalid filename after sanitization")
    
    # Limit length
    if len(filename) > 255:
        name, ext = os.path.splitext(filename)
        if len(ext) > 20:  # Unreasonably long extension
            ext = ext[:20]
        name = name[:255 - len(ext) - 1]
        filename = name + ext
    
    return filename


# CSV sanitization moved to utils/sanitization.py for comprehensive handling


# Test the validation functions
if __name__ == "__main__":
    # Test URL validation
    test_urls = [
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        "https://youtu.be/dQw4w9WgXcQ",
        "https://drive.google.com/file/d/1abc123/view",
        "https://docs.google.com/document/d/1abc123/edit",
        "javascript:alert('xss')",  # Should fail
        "https://evil.com/malware",  # Should fail with domain check
        "https://youtube.com/watch?v=test'; rm -rf /",  # Should fail
    ]
    
    print("Testing URL validation:")
    for url in test_urls:
        try:
            clean_url = validate_url(url, allowed_domains=['youtube.com', 'youtu.be', 'drive.google.com', 'docs.google.com'])
            print(f"✓ {url} -> Valid")
        except ValidationError as e:
            print(f"✗ {url} -> {e}")
    
    print("\nTesting path validation:")
    test_paths = [
        "downloads/video.mp4",
        "../../../etc/passwd",  # Should fail
        "downloads/../../../etc/passwd",  # Should fail
        "/tmp/file\x00.txt",  # Should fail
    ]
    
    for path in test_paths:
        try:
            clean_path = validate_file_path(path, base_dir=os.getcwd())
            print(f"✓ {path} -> {clean_path}")
        except ValidationError as e:
            print(f"✗ {path} -> {e}")