import os
import re
import sys
import json
import time
import argparse
import requests
from pathlib import Path
from urllib.parse import urlparse, parse_qs
try:
    from logger import setup_component_logging
    from logging_config import get_logger
    from validation import validate_google_drive_url, validate_file_path, ValidationError
    from retry_utils import retry_request, get_with_retry, retry_with_backoff
    from file_lock import file_lock, safe_file_operation
    from rate_limiter import rate_limit, wait_for_rate_limit
except ImportError:
    from .logger import setup_component_logging
    from .logging_config import get_logger
    from .validation import validate_google_drive_url, validate_file_path, ValidationError
    from .retry_utils import retry_request, get_with_retry, retry_with_backoff
    from .file_lock import file_lock, safe_file_operation
    from .rate_limiter import rate_limit, wait_for_rate_limit

# Setup module logger
logger = get_logger(__name__)

# Directory to save downloaded files
DOWNLOADS_DIR = "drive_downloads"

def create_download_dir(logger=None):
    """Create download directory if it doesn't exist"""
    if not os.path.exists(DOWNLOADS_DIR):
        os.makedirs(DOWNLOADS_DIR)
        if logger:
            logger.info(f"Created downloads directory: {DOWNLOADS_DIR}")

def extract_file_id(url):
    """Extract Google Drive file ID from URL"""
    # Pattern for different Google Drive URL formats
    patterns = [
        r'/file/d/([a-zA-Z0-9_-]+)',  # /file/d/{fileId}
        r'id=([a-zA-Z0-9_-]+)',       # id={fileId}
        r'drive.google.com/open\?id=([a-zA-Z0-9_-]+)' # open?id={fileId}
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    # Try parsing the URL query parameters
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    
    # Check various possible parameter names
    for param in ['id', 'file_id', 'fileId', 'docid']:
        if param in query_params:
            return query_params[param][0]
    
    return None

def get_filename_from_url(url):
    """Try to extract the filename from Drive URL or Content-Disposition header"""
    # First check if the filename is in the URL
    match = re.search(r'/([^/]+)$', url)
    if match:
        filename = match.group(1)
        # Clean up any URL parameters
        if '?' in filename:
            filename = filename.split('?')[0]
        if filename and filename != 'view' and not filename.startswith('d/'):
            return filename
    
    return None

def get_filename_from_response(response):
    """Extract filename from Content-Disposition header or content-type"""
    # Try Content-Disposition header first
    if 'Content-Disposition' in response.headers:
        content_disposition = response.headers['Content-Disposition']
        match = re.search(r'filename="?([^";]+)"?', content_disposition)
        if match:
            return match.group(1)
    
    # If no filename found, use the file ID with appropriate extension
    content_type = response.headers.get('Content-Type', '')
    
    # Map common MIME types to file extensions
    mime_to_ext = {
        'application/pdf': '.pdf',
        'application/msword': '.doc',
        'application/vnd.openxmlformats-officedocument.wordprocessingml.document': '.docx',
        'application/vnd.ms-excel': '.xls',
        'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet': '.xlsx',
        'application/vnd.ms-powerpoint': '.ppt',
        'application/vnd.openxmlformats-officedocument.presentationml.presentation': '.pptx',
        'text/plain': '.txt',
        'text/csv': '.csv',
        'image/jpeg': '.jpg',
        'image/png': '.png',
        'image/gif': '.gif',
        'application/zip': '.zip',
        'application/x-rar-compressed': '.rar',
        'application/x-tar': '.tar',
        'application/x-gzip': '.gz',
        'audio/mpeg': '.mp3',
        'video/mp4': '.mp4',
        'application/json': '.json'
    }
    
    extension = mime_to_ext.get(content_type, '')
    if not extension and '/' in content_type:
        # Use the subtype as extension for unrecognized types
        extension = '.' + content_type.split('/')[1].split(';')[0]
    
    return extension

def is_folder_url(url):
    """Check if URL is a Google Drive folder"""
    return '/folders/' in url or 'drive.google.com/drive/folders' in url

def extract_folder_id(url):
    """Extract Google Drive folder ID from URL"""
    patterns = [
        r'/folders/([a-zA-Z0-9_-]+)',  # /folders/{folderId}
    ]
    
    for pattern in patterns:
        match = re.search(pattern, url)
        if match:
            return match.group(1)
    
    return None

def get_folder_contents(folder_id, logger=None):
    """Not implemented - Google Drive API requires authentication"""
    if logger:
        logger.warning("Folder downloading is not supported without API keys.")
        logger.info(f"Please access the folder directly at: https://drive.google.com/drive/folders/{folder_id}")
    else:
        logger.warning("Folder downloading is not supported without API keys.")
        logger.info(f"Please access the folder directly at: https://drive.google.com/drive/folders/{folder_id}")
    return []

@retry_with_backoff(
    max_attempts=3,
    base_delay=5.0,
    exceptions=(requests.RequestException, IOError)
)
@rate_limit('google_drive')
def download_drive_file(file_id, output_filename=None, logger=None):
    """Download a file from Google Drive using file ID"""
    if not logger:
        logger = setup_component_logging('drive')
    
    # Validate file ID
    if not file_id or not re.match(r'^[a-zA-Z0-9_-]+$', file_id):
        logger.error(f"Invalid Google Drive file ID: {file_id}")
        return None
    
    # Direct download URL format
    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    
    # For large files, Google Drive shows a confirmation page
    # We need to handle this case properly
    
    session = requests.Session()
    
    logger.info(f"Downloading file with ID: {file_id}")
    
    # First request to get cookies and confirmation page for large files
    response = session.get(download_url, stream=True, timeout=30)
    
    # Check if we got the download confirmation page
    content_type = response.headers.get('Content-Type', '')
    if 'text/html' in content_type and 'virus scan warning' in response.text:
        # This is a virus scan warning page - we need to parse it
        confirm_match = re.search(r'confirm=([0-9a-zA-Z_-]+)', response.text)
        uuid_match = re.search(r'uuid=([0-9a-zA-Z_-]+)', response.text)
        
        if confirm_match:
            confirm_code = confirm_match.group(1)
            logger.info("Large file detected with virus scan warning, bypassing confirmation...")
            
            # Build the proper download URL with all parameters
            download_params = {
                'id': file_id,
                'export': 'download',
                'confirm': confirm_code
            }
            
            if uuid_match:
                download_params['uuid'] = uuid_match.group(1)
            
            # Use drive.usercontent.google.com for direct downloads
            direct_download_url = "https://drive.usercontent.google.com/download"
            
            # Make the download request with all parameters
            response = session.get(direct_download_url, params=download_params, stream=True, timeout=30)
    
    # Also check if the initial URL was already a direct download link
    elif 'drive.usercontent.google.com' in download_url and response.headers.get('Content-Type', '') == 'text/html':
        # We might have been given a direct download URL but still got HTML
        # Just retry the same URL - it should work on second attempt
        logger.info("Retrying direct download URL...")
        response = session.get(download_url, stream=True, timeout=30)
    
    # Check response
    if response.status_code != 200:
        logger.error(f"Error downloading file: HTTP status {response.status_code}")
        return None
    
    # Get filename if not provided
    if not output_filename:
        # Try to get from Content-Disposition header
        filename_extension = get_filename_from_response(response)
        if filename_extension:
            if filename_extension.startswith('.'):
                # It's just an extension
                output_filename = f"{file_id}{filename_extension}"
            else:
                # It's a full filename
                output_filename = filename_extension
        else:
            # Default filename based on file ID
            output_filename = f"{file_id}.bin"
    
    output_path = os.path.join(DOWNLOADS_DIR, output_filename)
    lock_file = Path(DOWNLOADS_DIR) / f".{file_id}.lock"
    
    # First check with shared lock if file exists
    with file_lock(lock_file, exclusive=False, timeout=30.0, logger=logger):
        if os.path.exists(output_path):
            logger.info(f"File already exists: {output_path}")
            return output_path
    
    # Now acquire exclusive lock for download
    with file_lock(lock_file, exclusive=True, timeout=300.0, logger=logger):  # 5 min timeout
        # Double-check after acquiring exclusive lock
        if os.path.exists(output_path):
            logger.info(f"File already exists: {output_path}")
            return output_path
        
        # Save file
        total_size = int(response.headers.get('content-length', 0))
        
        if total_size == 0:
            logger.warning("Could not determine file size")
        else:
            logger.info(f"File size: {total_size / (1024 * 1024):.2f} MB")
        
        # Download to a temporary file first
        temp_path = f"{output_path}.tmp"
        
        # Download with progress indicator and adaptive chunk size
        try:
            with open(temp_path, 'wb') as f:
                downloaded = 0
                
                # Adaptive chunk size based on file size
                if total_size > 100 * 1024 * 1024:  # Files > 100MB
                    chunk_size = 8 * 1024 * 1024  # 8MB chunks for large files
                elif total_size > 10 * 1024 * 1024:  # Files > 10MB
                    chunk_size = 2 * 1024 * 1024  # 2MB chunks for medium files
                else:
                    chunk_size = 1024 * 1024  # 1MB chunks for small files
                
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        if total_size > 0:
                            # Only show progress if we know the total size
                            progress = int(50 * downloaded / total_size)
                            sys.stdout.write("\r[%s%s] %s%%" % ('=' * progress, ' ' * (50-progress), int(100 * downloaded / total_size)))
                            sys.stdout.flush()
                
                if total_size > 0:
                    sys.stdout.write("\n")
            
            # Atomic rename after successful download
            os.replace(temp_path, output_path)
            logger.success(f"Downloaded file to {output_path}")
            return output_path
            
        except Exception as e:
            # Clean up temp file on error
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            logger.error(f"Error saving file: {str(e)}")
            return None

def save_metadata(file_id, url, metadata, logger=None):
    """Save file metadata to a JSON file"""
    if not logger:
        logger = setup_component_logging('drive')
    
    metadata_file = os.path.join(DOWNLOADS_DIR, f"{file_id}_metadata.json")
    
    # Add URL to metadata
    metadata['url'] = url
    metadata['file_id'] = file_id
    metadata['downloaded_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
    
    # Use file locking for metadata writes
    lock_file = Path(DOWNLOADS_DIR) / f".{file_id}_metadata.lock"
    
    with file_lock(lock_file, exclusive=True, timeout=30.0, logger=logger):
        # Write to temp file first
        temp_file = f"{metadata_file}.tmp"
        with open(temp_file, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2)
        
        # Atomic rename
        os.replace(temp_file, metadata_file)
    
    logger.info(f"Saved metadata to {metadata_file}")
    return metadata_file

def process_direct_download_url(url, output_filename=None, logger=None):
    """Process a direct drive.usercontent.google.com download URL"""
    if not logger:
        logger = setup_component_logging('drive')
    
    # Extract parameters from URL
    parsed_url = urlparse(url)
    query_params = parse_qs(parsed_url.query)
    
    file_id = query_params.get('id', [None])[0]
    if not file_id:
        logger.error("Could not extract file ID from direct download URL")
        return None
    
    # For direct download URLs, we just download directly
    create_download_dir(logger)
    
    # Create a session to handle the download
    session = requests.Session()
    
    # Make the download request
    response = session.get(url, stream=True, timeout=30)
    
    # Handle virus scan warning if present
    content_type = response.headers.get('Content-Type', '')
    if 'text/html' in content_type:
        # Extract filename from HTML if available
        html_content = response.text
        filename_match = re.search(r'>([^<]+\.[^<]+)</a>', html_content)
        suggested_filename = filename_match.group(1) if filename_match else None
        
        # Retry the same URL - it should work on second attempt
        logger.info("Retrying direct download URL after virus scan page...")
        response = session.get(url, stream=True, timeout=30)
    else:
        suggested_filename = None
    
    if response.status_code != 200:
        logger.error(f"Error downloading file: HTTP status {response.status_code}")
        return None
    
    # Determine filename
    if not output_filename:
        # Try Content-Disposition header
        cd = response.headers.get('Content-Disposition', '')
        filename_match = re.search(r'filename="?([^";]+)"?', cd)
        if filename_match:
            output_filename = filename_match.group(1)
        elif suggested_filename:
            output_filename = suggested_filename
        else:
            output_filename = f"{file_id}.bin"
    
    output_path = os.path.join(DOWNLOADS_DIR, output_filename)
    lock_file = Path(DOWNLOADS_DIR) / f".{file_id}.lock"
    
    # Check if file exists
    with file_lock(lock_file, exclusive=False, timeout=30.0, logger=logger):
        if os.path.exists(output_path):
            logger.info(f"File already exists: {output_path}")
            return output_path
    
    # Download with exclusive lock
    with file_lock(lock_file, exclusive=True, timeout=300.0, logger=logger):
        if os.path.exists(output_path):
            logger.info(f"File already exists: {output_path}")
            return output_path
        
        # Download to temp file
        temp_path = f"{output_path}.tmp"
        total_size = int(response.headers.get('Content-Length', 0))
        
        if total_size > 0:
            logger.info(f"File size: {total_size / (1024 * 1024):.2f} MB")
        
        try:
            with open(temp_path, 'wb') as f:
                downloaded = 0
                chunk_size = 8 * 1024 * 1024 if total_size > 100 * 1024 * 1024 else 1024 * 1024
                
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:
                        f.write(chunk)
                        downloaded += len(chunk)
                        
                        if total_size > 0:
                            progress = int(50 * downloaded / total_size)
                            sys.stdout.write("\r[%s%s] %s%%" % ('=' * progress, ' ' * (50-progress), int(100 * downloaded / total_size)))
                            sys.stdout.flush()
                
                if total_size > 0:
                    sys.stdout.write("\n")
            
            # Atomic rename
            os.replace(temp_path, output_path)
            logger.success(f"Downloaded file to {output_path}")
            return output_path
            
        except Exception as e:
            if os.path.exists(temp_path):
                os.unlink(temp_path)
            logger.error(f"Error saving file: {str(e)}")
            return None


def process_drive_url(url, output_filename=None, save_metadata_flag=False, logger=None):
    """Process a Google Drive URL (file or folder)"""
    if not logger:
        logger = setup_component_logging('drive')
    
    # Check if this is a direct download URL
    if 'drive.usercontent.google.com/download' in url:
        logger.info("Processing direct download URL...")
        downloaded_path = process_direct_download_url(url, output_filename, logger)
        
        # Handle metadata if requested
        metadata_path = None
        if downloaded_path and save_metadata_flag:
            parsed_url = urlparse(url)
            query_params = parse_qs(parsed_url.query)
            file_id = query_params.get('id', [None])[0]
            
            if file_id:
                metadata = {
                    'file_id': file_id,
                    'url': url,
                    'filename': os.path.basename(downloaded_path),
                    'file_size_bytes': os.path.getsize(downloaded_path)
                }
                metadata_path = save_metadata(file_id, url, metadata, logger)
        
        return downloaded_path, metadata_path
    
    # Validate URL for regular Drive URLs
    try:
        url, file_id = validate_google_drive_url(url)
    except ValidationError as e:
        logger.error(f"Invalid Google Drive URL: {e}")
        return None, None
    
    create_download_dir(logger)
    
    # Check if it's a folder
    if is_folder_url(url):
        folder_id = extract_folder_id(url)
        if not folder_id:
            logger.error(f"Could not extract folder ID from URL: {url}")
            return None, None
        
        logger.info(f"Folder ID: {folder_id}")
        logger.warning("Note: Folder downloading requires Google Drive API authentication")
        logger.info("Try accessing the folder directly in your browser")
        get_folder_contents(folder_id, logger)
        return None, None
    
    # Process single file
    file_id = extract_file_id(url)
    if not file_id:
        logger.error(f"Could not extract file ID from URL: {url}")
        return None, None
    
    logger.info(f"File ID: {file_id}")
    
    # Check if file already exists
    if output_filename:
        file_path = os.path.join(DOWNLOADS_DIR, output_filename)
    else:
        # Try to derive filename, otherwise we'll use the file ID
        filename_from_url = get_filename_from_url(url)
        if filename_from_url:
            file_path = os.path.join(DOWNLOADS_DIR, filename_from_url)
        else:
            # We'll determine this after the request
            file_path = None
    
    if file_path and os.path.exists(file_path):
        logger.info(f"File already exists at {file_path}")
        
        # Check for existing metadata
        metadata_path = os.path.join(DOWNLOADS_DIR, f"{file_id}_metadata.json")
        if os.path.exists(metadata_path):
            logger.info(f"Metadata already exists at {metadata_path}")
            return file_path, metadata_path
        
        # If metadata requested but doesn't exist, we'll create it
        if save_metadata_flag:
            metadata = {
                'file_id': file_id,
                'url': url,
                'filename': os.path.basename(file_path),
                'downloaded_at': 'previously',
                'file_size_bytes': os.path.getsize(file_path)
            }
            metadata_path = save_metadata(file_id, url, metadata, logger)
            return file_path, metadata_path
        
        return file_path, None
    
    # Download file
    downloaded_path = download_drive_file(file_id, output_filename, logger)
    
    # Save metadata if requested
    metadata_path = None
    if downloaded_path and save_metadata_flag:
        metadata = {
            'file_id': file_id,
            'url': url,
            'filename': os.path.basename(downloaded_path),
            'file_size_bytes': os.path.getsize(downloaded_path)
        }
        metadata_path = save_metadata(file_id, url, metadata, logger)
    
    return downloaded_path, metadata_path

def main():
    # Setup logging
    logger = setup_component_logging('drive')
    
    parser = argparse.ArgumentParser(description='Download Google Drive files')
    parser.add_argument('url', help='Google Drive file or folder URL')
    parser.add_argument('--filename', help='Output filename (optional)')
    parser.add_argument('--metadata', action='store_true',
                      help='Save file metadata to a JSON file')
    
    args = parser.parse_args()
    
    create_download_dir(logger)
    
    # Process the URL
    file_path, metadata_path = process_drive_url(
        args.url, 
        args.filename,
        args.metadata,
        logger
    )
    
    if file_path:
        logger.success(f"Download complete: {file_path}")
        if metadata_path:
            logger.info(f"Metadata saved: {metadata_path}")
    else:
        logger.error("Download failed.")

if __name__ == "__main__":
    main()