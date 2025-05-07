import os
import re
import sys
import json
import time
import argparse
import requests
from urllib.parse import urlparse, parse_qs

# Directory to save downloaded files
DOWNLOADS_DIR = "drive_downloads"

def create_download_dir():
    """Create download directory if it doesn't exist"""
    if not os.path.exists(DOWNLOADS_DIR):
        os.makedirs(DOWNLOADS_DIR)
        print(f"Created downloads directory: {DOWNLOADS_DIR}")

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

def get_folder_contents(folder_id):
    """Not implemented - Google Drive API requires authentication"""
    print("Folder downloading is not supported without API keys.")
    print(f"Please access the folder directly at: https://drive.google.com/drive/folders/{folder_id}")
    return []

def download_drive_file(file_id, output_filename=None):
    """Download a file from Google Drive using file ID"""
    # Direct download URL format
    download_url = f"https://drive.google.com/uc?export=download&id={file_id}"
    
    # For large files, Google Drive shows a confirmation page
    # We need to handle this case properly
    
    session = requests.Session()
    
    print(f"Downloading file with ID: {file_id}")
    
    # First request to get cookies and confirmation page for large files
    response = session.get(download_url, stream=True)
    
    # Check if we got the download confirmation page
    if 'confirm=' in response.text:
        confirm_match = re.search(r'confirm=([0-9a-zA-Z_-]+)', response.text)
        if confirm_match:
            confirm_code = confirm_match.group(1)
            print("Large file detected, bypassing confirmation...")
            download_url = f"{download_url}&confirm={confirm_code}"
            response = session.get(download_url, stream=True)
    
    # Check response
    if response.status_code != 200:
        print(f"Error downloading file: HTTP status {response.status_code}")
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
    
    # Save file
    total_size = int(response.headers.get('content-length', 0))
    
    if total_size == 0:
        print("Warning: Could not determine file size")
    else:
        print(f"File size: {total_size / (1024 * 1024):.2f} MB")
    
    # Download with progress indicator
    try:
        with open(output_path, 'wb') as f:
            downloaded = 0
            chunk_size = 1024 * 1024  # 1MB chunks
            
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
        
        print(f"Downloaded file to {output_path}")
        return output_path
        
    except Exception as e:
        print(f"Error saving file: {str(e)}")
        return None

def save_metadata(file_id, url, metadata):
    """Save file metadata to a JSON file"""
    metadata_file = os.path.join(DOWNLOADS_DIR, f"{file_id}_metadata.json")
    
    # Add URL to metadata
    metadata['url'] = url
    metadata['file_id'] = file_id
    metadata['downloaded_at'] = time.strftime('%Y-%m-%d %H:%M:%S')
    
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2)
    
    print(f"Saved metadata to {metadata_file}")
    return metadata_file

def process_drive_url(url, output_filename=None, save_metadata_flag=False):
    """Process a Google Drive URL (file or folder)"""
    create_download_dir()
    
    # Check if it's a folder
    if is_folder_url(url):
        folder_id = extract_folder_id(url)
        if not folder_id:
            print(f"Could not extract folder ID from URL: {url}")
            return None, None
        
        print(f"Folder ID: {folder_id}")
        print("Note: Folder downloading requires Google Drive API authentication")
        print("Try accessing the folder directly in your browser")
        return None, None
    
    # Process single file
    file_id = extract_file_id(url)
    if not file_id:
        print(f"Could not extract file ID from URL: {url}")
        return None, None
    
    print(f"File ID: {file_id}")
    
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
        print(f"File already exists at {file_path}")
        
        # Check for existing metadata
        metadata_path = os.path.join(DOWNLOADS_DIR, f"{file_id}_metadata.json")
        if os.path.exists(metadata_path):
            print(f"Metadata already exists at {metadata_path}")
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
            metadata_path = save_metadata(file_id, url, metadata)
            return file_path, metadata_path
        
        return file_path, None
    
    # Download file
    downloaded_path = download_drive_file(file_id, output_filename)
    
    # Save metadata if requested
    metadata_path = None
    if downloaded_path and save_metadata_flag:
        metadata = {
            'file_id': file_id,
            'url': url,
            'filename': os.path.basename(downloaded_path),
            'file_size_bytes': os.path.getsize(downloaded_path)
        }
        metadata_path = save_metadata(file_id, url, metadata)
    
    return downloaded_path, metadata_path

def main():
    parser = argparse.ArgumentParser(description='Download Google Drive files')
    parser.add_argument('url', help='Google Drive file or folder URL')
    parser.add_argument('--filename', help='Output filename (optional)')
    parser.add_argument('--metadata', action='store_true',
                      help='Save file metadata to a JSON file')
    
    args = parser.parse_args()
    
    create_download_dir()
    
    # Process the URL
    file_path, metadata_path = process_drive_url(
        args.url, 
        args.filename,
        args.metadata
    )
    
    if file_path:
        print(f"\nDownload complete: {file_path}")
        if metadata_path:
            print(f"Metadata saved: {metadata_path}")
    else:
        print("\nDownload failed.")

if __name__ == "__main__":
    main()