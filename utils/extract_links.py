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

# DRY: Use consolidated import management (Phase 3F)
from config import bulk_safe_import

imports = bulk_safe_import([
    ('http_pool', 'get'),
    ('config', 'get_config'),
    ('logging_config', 'get_logger'),
    ('rate_limiter', 'rate_limit'),
    ('rate_limiter', 'wait_for_rate_limit'),
    # Import URL processing functions from validation.py  
    ('validation', 'clean_url'),
    ('validation', 'determine_url_type'),
    ('validation', 'extract_actual_url'),
    ('validation', 'extract_youtube_ids'),
    ('validation', 'extract_youtube_playlists'),
    ('validation', 'build_youtube_playlist_url'),
    ('validation', 'extract_drive_links_from_html'),
    ('validation', 'extract_drive_links'),
    ('validation', 'filter_meaningful_links')
])

http_get = imports['get']
get_config = imports['get_config']
get_logger = imports['get_logger']
rate_limit = imports['rate_limit']
wait_for_rate_limit = imports['wait_for_rate_limit']
# URL processing functions from validation.py
clean_url = imports['clean_url']
determine_url_type = imports['determine_url_type']
extract_actual_url = imports['extract_actual_url']
extract_youtube_ids = imports['extract_youtube_ids']
extract_youtube_playlists = imports['extract_youtube_playlists']
build_youtube_playlist_url = imports['build_youtube_playlist_url']
extract_drive_links_from_html = imports['extract_drive_links_from_html']
extract_drive_links = imports['extract_drive_links']
filter_meaningful_links = imports['filter_meaningful_links']

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

# URL cleaning function is now imported from validation.py

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

@rate_limit('selenium')
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
        doc_links = set()
        for a in soup.find_all('a', href=True):
            href = a.get('href')
            if href:
                # Decode unicode escapes in href
                try:
                    if '\\u' in href:
                        href = href.encode('utf-8').decode('unicode_escape')
                except:
                    pass
                doc_links.add(clean_url(href))
        
        # Get links appearing in plain text (emails, URLs)
        # First decode unicode escapes in the HTML before extracting
        # Google Docs often encodes URL characters as unicode escapes
        decoded_html = html
        try:
            # Replace common unicode escapes for URL characters
            # \u003d is =, \u0026 is &, \u003f is ?
            decoded_html = decoded_html.replace('\\u003d', '=')
            decoded_html = decoded_html.replace('\\u0026', '&')
            decoded_html = decoded_html.replace('\\u003f', '?')
        except:
            pass
        
        # Updated regex to properly terminate URLs at common boundaries
        # Explicitly exclude all control characters (ASCII 0-31 and 127-159)
        raw_text_links = re.findall(r'https?://[^\s<>"{}\\|\^\[\]`\x00-\x1f\x7f-\x9f]+(?:[.,;:!?\)\]]*(?=[\s<>"{}\\|\^\[\]`\x00-\x1f\x7f-\x9f]|$))', decoded_html)
        text_links = {clean_url(link) for link in raw_text_links if link}
        email_links = set(re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', html))
        
        # Parse meta tags for content links
        meta_links = set()
        for meta in soup.find_all('meta', property=lambda x: x and ('og:description' in x or 'description' in x)):
            content = meta.get('content', '')
            if content:
                # Decode unicode escapes in meta content
                try:
                    content = content.replace('\\u003d', '=').replace('\\u0026', '&').replace('\\u003f', '?')
                except:
                    pass
                # Find URLs in content
                raw_meta_links = re.findall(r'https?://[^\s<>"{}\\|\^\[\]`\x00-\x1f\x7f-\x9f]+(?:[.,;:!?\)\]]*(?=[\s<>"{}\\|\^\[\]`\x00-\x1f\x7f-\x9f]|$))', content)
                meta_links.update(clean_url(link) for link in raw_meta_links if link)
                # Find emails in content
                meta_links.update([f"mailto:{email}" for email in 
                                 re.findall(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', content)])
        
        # Add email links with mailto: prefix
        email_links = {f"mailto:{email}" for email in email_links}
        
        # Combine all links
        all_links = doc_links.union(text_links).union(email_links).union(meta_links)
        
        # Use consolidated filter function from validation.py
        result = filter_meaningful_links(all_links, source_url=url)
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


# URL extraction functions are now imported from validation.py
from urllib.parse import urlparse, parse_qs

def process_url(url, limit=1, debug=False):
    """
    Process a URL to extract links, YouTube playlists, and Google Drive links.
    
    Args:
        url (str): The URL to process
        limit (int): Maximum number of links to return. Default is 1 to only return the primary link.
        debug (bool): Whether to save HTML content for debugging purposes
    
    Returns:
        tuple: (list of links, YouTube playlist URL or None, list of Google Drive links or None)
    """
    # Check if this is a YouTube video URL (not a playlist)
    if url and ('youtube.com/watch' in url or 'youtu.be/' in url):
        # Skip processing individual YouTube videos to avoid large data extraction
        logger.info(f"Skipping link extraction for YouTube video: {url}")
        return [], None, None
    
    # First get the HTML content
    html = ""
    if "docs.google.com/document" in url:
        html = get_html_with_selenium(url, debug=debug)
    elif "drive.google.com" in url:
        html = get_html(url, debug=debug)
    
    # Extract regular links
    links = extract_links(url, limit, debug=debug)
    
    # Process links to extract YouTube content and Drive links
    if links:
        # First check for actual YouTube playlists
        youtube_playlists = extract_youtube_playlists(links)
        
        if youtube_playlists:
            # If we found actual playlists, use them (join with | for multiple)
            yt_playlist_url = "|".join(youtube_playlists)
        else:
            # Fall back to synthetic playlist from individual videos
            yt_ids = extract_youtube_ids(links)
            yt_playlist_url = build_youtube_playlist_url(yt_ids)
        
        # Extract Drive links from both regular links and HTML content
        drive_links = extract_drive_links(links, html=html)
        
        # Return None for empty values
        if not yt_playlist_url:
            yt_playlist_url = None
        if not drive_links:
            drive_links = None
        
        return links, yt_playlist_url, drive_links
    
    # Even if no regular links were found, try to extract Drive links from HTML
    elif html:
        drive_links = extract_drive_links([], html=html)
        
        # Return None for empty values
        if not drive_links:
            drive_links = None
                
        return [], None, drive_links
    
    # Return None for empty values
    return [], None, None
    
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