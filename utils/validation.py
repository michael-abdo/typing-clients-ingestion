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


# DRY: URL Processing Functions (absorbed from utils/extract_links.py - Phase 3F)
def clean_url(url):
    """
    Clean up a URL by removing trailing junk and escape sequences.
    Consolidated from utils/extract_links.py and minimal/extraction_utils.py
    """
    if not url:
        return url
    
    original_url = url
    
    # First, decode unicode escape sequences like \u000b to actual characters
    # This handles cases like \u003d becoming =
    try:
        # Decode unicode escapes
        if '\\u' in url:
            url = url.encode('utf-8').decode('unicode_escape')
    except:
        # If decoding fails, continue with original
        pass
    
    # Special handling for YouTube playlist URLs that might have control chars in the middle
    if 'youtube.com/playlist?list' in url:
        # Try to extract the playlist ID even if there are control characters
        # Look for pattern: list[any char]=PLAYLIST_ID
        match = re.search(r'youtube\.com/playlist\?list.{0,5}=([a-zA-Z0-9_-]+)', url)
        if match:
            playlist_id = match.group(1)
            # Check if there's also an si parameter
            si_match = re.search(r'[&?]si.{0,5}=([a-zA-Z0-9_-]+)', url)
            if si_match:
                return f'https://www.youtube.com/playlist?list={playlist_id}&si={si_match.group(1)}'
            return f'https://www.youtube.com/playlist?list={playlist_id}'
    
    # Find where the URL should end by looking for control characters or certain text patterns
    # This is crucial - we want to cut off anything after control characters
    control_char_pos = float('inf')
    for i, char in enumerate(url):
        if ord(char) < 32 or ord(char) > 126:  # Control chars or non-ASCII
            control_char_pos = i
            break
    
    # Truncate at the first control character
    if control_char_pos < len(url):
        url = url[:control_char_pos]
    
    # Remove common escape sequences that might remain
    url = url.replace('\\n', '').replace('\\r', '').replace('\\t', '')
    
    # Remove trailing punctuation that's not part of a URL
    # But keep trailing slashes and common URL endings
    url = re.sub(r'[.,;:!?\)\]\}"\'>]+$', '', url)
    
    # Handle YouTube URLs specifically
    if 'youtube.com' in url or 'youtu.be' in url:
        # For youtu.be links, extract just the video ID
        match = re.search(r'youtu\.be/([a-zA-Z0-9_-]{11})', url)
        if match:
            # Check if there's a trailing slash in the original
            if original_url.rstrip().endswith(match.group(0) + '/'):
                return f'https://youtu.be/{match.group(1)}/'
            return f'https://youtu.be/{match.group(1)}'
        # For youtube.com watch links
        match = re.search(r'((?:www\.)?youtube\.com)/watch\?v=([a-zA-Z0-9_-]{11})', url)
        if match:
            # Preserve www. if it was in the original
            domain = match.group(1)
            video_id = match.group(2)
            
            # Check for additional parameters
            params = []
            list_match = re.search(r'[&?]list=([a-zA-Z0-9_-]+)', url)
            if list_match:
                params.append(f'list={list_match.group(1)}')
            
            base_url = f'https://{domain}/watch?v={video_id}'
            if params:
                return base_url + '&' + '&'.join(params)
            return base_url
        # For youtube.com playlist links
        match = re.search(r'youtube\.com/playlist\?list=([a-zA-Z0-9_-]+)', url)
        if match:
            # Extract list ID and any additional parameters
            list_id = match.group(1)
            # Check for si parameter
            si_match = re.search(r'[&?]si=([a-zA-Z0-9_-]+)', url)
            if si_match:
                return f'https://youtube.com/playlist?list={list_id}&si={si_match.group(1)}'
            return f'https://youtube.com/playlist?list={list_id}'
    
    # For non-YouTube URLs, just clean and return
    url = url.strip()
    
    # If it's a valid URL, return it as-is
    if url.startswith('http://') or url.startswith('https://'):
        return url
    
    return url


def determine_url_type(url):
    """
    Determine the type of URL (youtube, drive, docs, etc.)
    DRY: Centralized URL type detection
    """
    if not url:
        return 'unknown'
    
    url_lower = url.lower()
    
    # YouTube detection
    if 'youtube.com' in url_lower or 'youtu.be' in url_lower:
        if '/playlist' in url_lower or 'list=' in url_lower:
            return 'youtube_playlist'
        elif 'watch_videos' in url_lower:
            return 'youtube_synthetic_playlist'
        else:
            return 'youtube_video'
    
    # Google Drive detection
    elif 'drive.google.com' in url_lower:
        if '/folders/' in url_lower:
            return 'drive_folder'
        else:
            return 'drive_file'
    
    # Google Docs detection
    elif 'docs.google.com' in url_lower:
        if '/document/' in url_lower:
            return 'google_doc'
        elif '/spreadsheets/' in url_lower:
            return 'google_sheet'
        elif '/presentation/' in url_lower:
            return 'google_slide'
        else:
            return 'google_docs_other'
    
    # Email detection
    elif url_lower.startswith('mailto:'):
        return 'email'
    
    # Other web URLs
    elif url_lower.startswith('http://') or url_lower.startswith('https://'):
        return 'web'
    
    return 'unknown'


def extract_actual_url(url):
    """
    Extract the actual URL from a Google redirect URL.
    Handles google.com/url?q= redirects
    """
    if 'google.com/url?' in url:
        try:
            parsed = urlparse(url)
            params = parse_qs(parsed.query)
            if 'q' in params and params['q']:
                actual_url = params['q'][0]
                # Clean the extracted URL
                return clean_url(actual_url)
        except:
            pass
    return url


def extract_youtube_ids(links):
    """Extract YouTube video IDs from a list of links"""
    yt_ids = set()
    for link in links:
        try:
            # Skip non-URL links like mailto: or invalid URLs
            if not link.startswith('http'):
                continue
                
            parsed = urlparse(link)
            if "youtube.com" in parsed.netloc:
                qs = parse_qs(parsed.query)
                if "v" in qs:
                    yt_ids.update(qs["v"])
            elif "youtu.be" in parsed.netloc:
                yt_id = parsed.path.lstrip("/")
                if yt_id:
                    yt_ids.add(yt_id)
        except Exception as e:
            continue
    return list(yt_ids)


def extract_youtube_playlists(links):
    """
    Extract YouTube playlist URLs from a list of links.
    
    This function specifically looks for actual YouTube playlists (not synthetic ones)
    and returns clean playlist URLs with just the list parameter.
    """
    playlists = []
    for link in links:
        try:
            # Skip non-URL links
            if not link.startswith('http'):
                continue
                
            parsed = urlparse(link)
            if "youtube.com" in parsed.netloc and "/playlist" in parsed.path:
                qs = parse_qs(parsed.query)
                if "list" in qs and qs["list"]:
                    # Reconstruct clean playlist URL with only the list parameter
                    playlist_id = qs["list"][0]
                    playlist_url = f"https://www.youtube.com/playlist?list={playlist_id}"
                    if playlist_url not in playlists:
                        playlists.append(playlist_url)
        except Exception as e:
            continue
    return playlists


def build_youtube_playlist_url(yt_ids):
    """Build a YouTube synthetic playlist URL from video IDs"""
    if yt_ids:
        return "https://www.youtube.com/watch_videos?video_ids=" + ",".join(yt_ids)
    return None


def extract_drive_links_from_html(html):
    """Extract Google Drive links directly from HTML content"""
    # Comprehensive pattern to match Google Drive links in HTML
    # This matches the file ID in drive.google.com/file/d/ or drive.google.com/open?id= links
    file_pattern = re.compile(r'https?://drive\.google\.com/(?:file/d/|open\?id=)([a-zA-Z0-9_\-]+)')
    folder_pattern = re.compile(r'https?://drive\.google\.com/drive/folders/([a-zA-Z0-9_\-]+)')
    
    # Find all file and folder IDs
    file_ids = file_pattern.findall(html)
    folder_ids = folder_pattern.findall(html)
    
    # Construct clean URLs
    drive_links = []
    
    # Add file links
    for file_id in file_ids:
        drive_links.append(f"https://drive.google.com/file/d/{file_id}/view")
    
    # Add folder links
    for folder_id in folder_ids:
        drive_links.append(f"https://drive.google.com/drive/folders/{folder_id}")
    
    # Remove duplicates while preserving order
    seen = set()
    unique_links = []
    for link in drive_links:
        if link not in seen:
            seen.add(link)
            unique_links.append(link)
    
    return unique_links


def extract_drive_links(links, html=None):
    """Extract Google Drive links from URLs and optionally from HTML content"""
    drive_urls = []
    
    # First extract from regular links
    for link in links:
        try:
            # Skip non-URL links or invalid URLs
            if not link.startswith('http'):
                continue
                
            # Match Google Drive file links
            if ('drive.google.com/file/d/' in link or 
                'drive.google.com/open?id=' in link or
                'drive.google.com/drive/folders/' in link):
                drive_urls.append(link)
        except Exception as e:
            continue
    
    # If HTML content is provided, also extract links from there
    if html:
        html_drive_links = extract_drive_links_from_html(html)
        drive_urls.extend([link for link in html_drive_links if link not in drive_urls])
    
    return drive_urls


def filter_meaningful_links(links, source_url=None):
    """
    Filter out infrastructure links and keep only meaningful content links.
    Useful for extracting actual content links from Google Docs.
    """
    filtered_links = set()
    
    # Always include the source URL if provided
    if source_url:
        filtered_links.add(source_url)
    
    for link in links:
        # Keep email links
        if link.startswith('mailto:'):
            filtered_links.add(link)
            continue
            
        # Keep drive links that aren't infrastructure
        if 'drive.google.com/drive' in link and 'folder' in link:
            # Clean up Google Drive links
            if '?usp' in link:
                clean_link = link.split('?usp')[0]
                filtered_links.add(clean_link)
            else:
                filtered_links.add(link)
            continue
            
        # Check if it's a content link and not an infrastructure link
        is_infrastructure = any([
            'gstatic.com' in link,
            'apis.google.com' in link,
            'script.google.com' in link,
            'chrome.google.com' in link,
            'clients6.google.com' in link,
            '/static/' in link,
            'accounts.google.com' in link,
            'docs.google.com/picker' in link,
            'docs.google.com/relay.html' in link,
            'contacts.google.com' in link,
            'lh7-rt.googleusercontent.com' in link,
            'googleusercontent.com/docs' in link,
            'schema.org' in link,
            'w3.org' in link,
            '#' in link,
            link.endswith('.js'),
            link.endswith('.css'),
            link.endswith('.png'),
            link.endswith('.gif'),
            'docs.google.com/static' in link,
            'docs.google.com/preview' in link,
            'docs.google.com?usp=' in link,
            '",s-blob-v1-IMAGE-' in link,
            '"' in link,
            'support.google.com' in link,
            "}.config['csfu']" in link
        ])
        
        if not is_infrastructure:
            # Clean up URL if needed (remove trailing quotes or parentheses, etc.)
            link = re.sub(r'[\"\'\)]$', '', link)
            
            # Skip links that have JSON/code markers or font references
            if any([
                '{' in link,
                '}' in link,
                '[' in link,
                ']' in link,
                'si:' in link,
                'ei:' in link,
                'sm:' in link,
                'spi:' in link,
                'docs/fonts' in link,
                '.woff' in link
            ]):
                continue
                
            # Skip Google Doc internal links that aren't really content
            if link.startswith('https://docs.google.com') and source_url and link != source_url:
                if any([
                    'usp\u003d' in link,
                    '/preview' in link,
                    '/edit?' in link and source_url.split('/edit')[0] in link
                ]):
                    continue
            
            filtered_links.add(link)
    
    return list(filtered_links)


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