from bs4 import BeautifulSoup
import re
import os
import json
import time
import atexit
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

try:
    from http_pool import get as http_get
    from config import get_config
    from logging_config import get_logger
except ImportError:
    from .http_pool import get as http_get
    from .config import get_config
    from .logging_config import get_logger

# Setup module logger
logger = get_logger(__name__)

# Get configuration
config = get_config()

# Base directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(BASE_DIR, "cache")

# Only cache Google Sheets HTML
GOOGLE_SHEET_CACHE_FILE = os.path.join(CACHE_DIR, "google_sheet_cache.html")

# Selenium driver instance (initialize lazily)
_driver = None

# Register cleanup function to run on exit
atexit.register(lambda: cleanup_driver())

def cleanup_driver():
    """Clean up the Selenium driver"""
    global _driver
    if _driver is not None:
        try:
            _driver.quit()
            _driver = None
            logger.info("Selenium driver cleaned up successfully")
        except Exception as e:
            logger.error(f"Error cleaning up Selenium driver: {e}")

def clean_url(url):
    """Clean up a URL by removing trailing junk and escape sequences"""
    if not url:
        return url
    
    # Remove common escape sequences
    url = url.replace('\\n', '').replace('\\r', '').replace('\\t', '')
    url = url.replace('\n', '').replace('\r', '').replace('\t', '')
    
    # Remove unicode escape sequences
    url = re.sub(r'\\u[0-9a-fA-F]{4}', '', url)
    
    # Remove trailing punctuation that's not part of a URL
    # But keep trailing slashes and common URL endings
    url = re.sub(r'[.,;:!?\)\]\}"\'>]+$', '', url)
    
    # Remove any text after common URL patterns
    # Handle YouTube URLs specifically
    if 'youtube.com' in url or 'youtu.be' in url:
        # For youtu.be links, extract just the video ID
        match = re.search(r'youtu\.be/([a-zA-Z0-9_-]{11})', url)
        if match:
            return f'https://youtu.be/{match.group(1)}'
        # For youtube.com watch links
        match = re.search(r'youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})', url)
        if match:
            return f'https://www.youtube.com/watch?v={match.group(1)}'
    
    # Remove any remaining non-URL characters at the end
    url = re.sub(r'[^a-zA-Z0-9\-._~:/?#\[\]@!$&\'()*+,;=%]+$', '', url)
    
    return url.strip()

def get_selenium_driver():
    """Initialize and return a Selenium WebDriver instance"""
    global _driver
    if _driver is None:
        logger.info("Initializing Selenium Chrome driver...")
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Run in headless mode
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--no-sandbox")
        
        try:
            _driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        except Exception as e:
            logger.error(f"Error initializing Selenium driver: {str(e)}")
            # Try fallback method - requires Chrome driver to be installed manually
            try:
                _driver = webdriver.Chrome(options=chrome_options)
            except Exception as e:
                logger.error(f"Could not initialize Selenium driver: {str(e)}")
                return None
    
    # Ensure driver is still alive
    try:
        _driver.title  # Simple check to see if driver is responsive
    except Exception:
        logger.warning("Driver was closed, reinitializing...")
        _driver = None
        return get_selenium_driver()
    
    return _driver

def get_html_with_selenium(url, debug=False):
    """Get HTML using Selenium for JavaScript rendering"""
    driver = get_selenium_driver()
    if not driver:
        logger.error("Failed to initialize Selenium driver")
        return ""
    
    logger.info(f"Loading {url} with Selenium...")
    try:
        driver.get(url)
        # Wait for page to load
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        # Extra wait for Google Docs to render
        time.sleep(5)
        
        # For Google Docs, try scrolling to ensure all content is loaded
        if "docs.google.com/document" in url:
            # Scroll down in small increments to ensure content loads
            height = driver.execute_script("return document.body.scrollHeight")
            for i in range(0, height, 300):
                driver.execute_script(f"window.scrollTo(0, {i});")
                time.sleep(0.1)
            # Scroll back to top
            driver.execute_script("window.scrollTo(0, 0);")
        
        html = driver.page_source
        
        # For debugging, save the HTML content
        if debug:
            # Create debug directory if it doesn't exist
            if not os.path.exists(CACHE_DIR):
                os.makedirs(CACHE_DIR)
                
            debug_file = os.path.join(CACHE_DIR, f"selenium_debug_{url.replace('://', '_').replace('/', '_').replace('?', '_').replace('=', '_')}.html")
            with open(debug_file, 'w', encoding='utf-8') as f:
                f.write(html)
            logger.debug(f"Saved Selenium debug HTML to {debug_file}")
        
        return html
    except Exception as e:
        logger.error(f"Error loading {url} with Selenium: {str(e)}")
        return ""

def get_html(url, debug=False):
    """Get HTML from the web without caching (except for Google Sheets)"""
    # Use Selenium for Google Docs to handle JavaScript rendering
    if "docs.google.com/document" in url:
        return get_html_with_selenium(url, debug)
    
    logger.info(f"Downloading HTML for {url}")
    try:
        # Headers are already configured in http_pool
        # Just use streaming
        response = http_get(url, stream=True)
        response.raise_for_status()
        
        # Stream HTML content to avoid loading large pages into memory
        html = ""
        chunk_size = 8192
        for chunk in response.iter_content(chunk_size=chunk_size, decode_unicode=True):
            if chunk:
                html += chunk
        
        # For debugging, save the HTML content
        if debug:
            # Create debug directory if it doesn't exist
            if not os.path.exists(CACHE_DIR):
                os.makedirs(CACHE_DIR)
                
            debug_file = os.path.join(CACHE_DIR, f"debug_{url.replace('://', '_').replace('/', '_').replace('?', '_').replace('=', '_')}.html")
            with open(debug_file, 'w', encoding='utf-8') as f:
                # Write HTML in chunks if it's very large
                if len(html) > 1024 * 1024:  # 1MB
                    for i in range(0, len(html), chunk_size):
                        f.write(html[i:i+chunk_size])
                else:
                    f.write(html)
            logger.debug(f"Saved debug HTML to {debug_file}")
        
        # Only cache Google Sheets
        if "docs.google.com/spreadsheets" in url and html:
            with open(GOOGLE_SHEET_CACHE_FILE, 'w', encoding='utf-8') as f:
                # Write in chunks for large Google Sheets
                if len(html) > 1024 * 1024:  # 1MB
                    for i in range(0, len(html), chunk_size):
                        f.write(html[i:i+chunk_size])
                else:
                    f.write(html)
            logger.info(f"Cached Google Sheet HTML to {GOOGLE_SHEET_CACHE_FILE}")
        
        # Add a small delay to ensure the page has time to render
        time.sleep(1)
        
        return html
    except Exception as e:
        # Log error but don't fail completely - some URLs might be temporarily unavailable
        logger.warning(f"Error downloading {url}: {str(e)}")
        # Return empty string to allow processing to continue
        # Caller should check for empty response
        return ""

def extract_links(url, limit=1, debug=False):
    """
    Extract links from a given URL. Limit parameter restricts the number of links returned.
    For Google Docs, we use Selenium to handle JavaScript rendering.
    
    Returns a tuple of (all_links, drive_links) where drive_links is a list of Google Drive URLs.
    """
    # Get the HTML content with the appropriate method
    html = get_html(url, debug=debug)
    if not html:
        return []
    
    if debug:
        logger.debug(f"Downloaded HTML for debugging purposes ({len(html)} bytes)")
    
    # Google Docs special handling
    if "docs.google.com/document" in url:
        logger.info(f"Google document detected: {url}")
        # Add original URL to results
        result = [url]
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extract links from anchor tags
        doc_links = {clean_url(a.get('href')) for a in soup.find_all('a', href=True) if a.get('href')}
        
        # Get links appearing in plain text (emails, URLs)
        # Updated regex to properly terminate URLs at common boundaries
        raw_text_links = re.findall(r'https?://[^\s<>"{}\\|\^\[\]`\n\r]+(?:[.,;:!?\)\]]*(?=[\s<>"{}\\|\^\[\]`\n\r]|$))', html)
        text_links = {clean_url(link) for link in raw_text_links if link}
        email_links = set(re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', html))
        
        # Parse meta tags for content links
        meta_links = set()
        for meta in soup.find_all('meta', property=lambda x: x and ('og:description' in x or 'description' in x)):
            content = meta.get('content', '')
            if content:
                # Find URLs in content
                raw_meta_links = re.findall(r'https?://[^\s<>"{}\\|\^\[\]`\n\r]+(?:[.,;:!?\)\]]*(?=[\s<>"{}\\|\^\[\]`\n\r]|$))', content)
                meta_links.update(clean_url(link) for link in raw_meta_links if link)
                # Find emails in content
                meta_links.update([f"mailto:{email}" for email in 
                                 re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', content)])
        
        # Add email links with mailto: prefix
        email_links = {f"mailto:{email}" for email in email_links}
        
        # Combine all links
        all_links = doc_links.union(text_links).union(email_links).union(meta_links)
        
        # Filter out infrastructure links and keep only content links
        filtered_links = set()
        
        # Always include the original document URL
        filtered_links.add(url)
        
        # Include emails and specific content links
        for link in all_links:
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
                if link.startswith('https://docs.google.com') and link != url:
                    if any([
                        'usp\u003d' in link,
                        '/preview' in link,
                        '/edit?' in link and url.split('/edit')[0] in link
                    ]):
                        continue
                
                filtered_links.add(link)
        
        # Remove duplicates and return
        result = list(filtered_links)
        return result[:limit] if limit > 0 else result
    
    # Regular drive.google.com links
    elif "drive.google.com" in url:
        logger.info(f"Google Drive detected: {url}")
        # Add original URL to results
        result = [url]
        
        # Parse with BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        
        # Extract links from anchor tags
        drive_links = {a.get('href') for a in soup.find_all('a', href=True)}
        
        # Get links appearing in plain text
        text_links = set(re.findall(r'https?://\S+', html))
        
        # Combine all links
        all_links = drive_links.union(text_links)
        
        # Filter out empty or None links
        all_links = {link for link in all_links if link and not link.startswith('javascript:')}
        
        # Add them to result
        result.extend(all_links)
        
        # Remove duplicates and return
        result = list(set(result))
        return result[:limit] if limit > 0 else result
    
    # For other sites, regular link extraction
    soup = BeautifulSoup(html, 'html.parser')
    # Get links from anchor tags
    links = {a.get('href') for a in soup.find_all('a', href=True)}
    # Get links appearing in plain text
    text_links = set(re.findall(r'https?://\S+', html))
    result = list(links.union(text_links))
    
    # Filter out empty or None links
    result = [link for link in result if link]
    
    # Return only the requested number of links
    return result[:limit] if limit > 0 else result


from urllib.parse import urlparse, parse_qs

def extract_drive_links_from_html(html):
    """Extract Google Drive links directly from HTML content"""
    import re
    
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
            print(f"Error parsing drive link {link}: {str(e)}")
            continue
    
    # If HTML content is provided, also extract links from there
    if html:
        html_drive_links = extract_drive_links_from_html(html)
        drive_urls.extend([link for link in html_drive_links if link not in drive_urls])
    
    return drive_urls

def extract_youtube_ids(links):
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
            print(f"Error parsing link {link}: {str(e)}")
            continue
    return list(yt_ids)

def build_youtube_playlist_url(yt_ids):
    if yt_ids:
        return "https://www.youtube.com/watch_videos?video_ids=" + ",".join(yt_ids)
    return None

def process_url(url, limit=1, debug=False, use_dash_for_empty=True):
    """
    Process a URL to extract links, YouTube playlists, and Google Drive links.
    
    Args:
        url (str): The URL to process
        limit (int): Maximum number of links to return. Default is 1 to only return the primary link.
        debug (bool): Whether to save HTML content for debugging purposes
        use_dash_for_empty (bool): If True, return "-" for empty YouTube playlist and Google Drive links
    
    Returns:
        tuple: (list of links, YouTube playlist URL or "-", list of Google Drive links or "-")
    """
    # First get the HTML content
    html = ""
    if "docs.google.com/document" in url:
        html = get_html_with_selenium(url, debug=debug)
    elif "drive.google.com" in url:
        html = get_html(url, debug=debug)
    
    # Extract regular links
    links = extract_links(url, limit, debug=debug)
    
    # Process links to extract YouTube IDs and Drive links
    if links:
        yt_ids = extract_youtube_ids(links)
        yt_playlist_url = build_youtube_playlist_url(yt_ids)
        
        # Extract Drive links from both regular links and HTML content
        drive_links = extract_drive_links(links, html=html)
        
        # If using dash for empty values, replace None or empty lists with "-"
        if use_dash_for_empty:
            if not yt_playlist_url:
                yt_playlist_url = "-"
            if not drive_links:
                drive_links = ["-"]
        
        return links, yt_playlist_url, drive_links
    
    # Even if no regular links were found, try to extract Drive links from HTML
    elif html:
        drive_links = extract_drive_links([], html=html)
        
        # If using dash for empty values, replace None or empty lists with "-"
        if use_dash_for_empty:
            if not drive_links:
                drive_links = ["-"]
                
        return [], "-" if use_dash_for_empty else None, drive_links
    
    # Return empty values with "-" if specified
    if use_dash_for_empty:
        return [], "-", ["-"]
    else:
        return [], None, []
    
# If the script is run directly, test with a sample URL
if __name__ == "__main__":
    # Process the specified Google Doc
    import sys
    if len(sys.argv) > 1:
        test_url = sys.argv[1]
    else:
        test_url = "https://docs.google.com/document/d/1kMxRcQEdKLI89GV5E7Fr0N4h607vB7k4S1CU4s3Lihw/edit?tab=t.0"
    
    links, playlist, drive_links = process_url(test_url, limit=10, debug=True)
    print(f"Extracted links from {test_url}:")
    for link in links:
        print(f"  - {link}")
    
    print(f"\nYouTube playlist: {playlist if playlist else 'None'}")
    
    print(f"\nGoogle Drive links ({len(drive_links)}):")
    for drive_link in drive_links:
        print(f"  - {drive_link}")
    
    # Clean up the driver
    cleanup_driver()