from bs4 import BeautifulSoup
import re
import os
import json
import time
import atexit
import urllib.parse
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

try:
    from http_pool import get as http_get
    from config import get_config
    from logging_config import get_logger
    from rate_limiter import rate_limit, wait_for_rate_limit
    from patterns import clean_url, get_selenium_driver, cleanup_selenium_driver
    from error_handling import with_standard_error_handling
except ImportError:
    from .http_pool import get as http_get
    from .config import get_config
    from .logging_config import get_logger
    from .rate_limiter import rate_limit, wait_for_rate_limit
    from .patterns import clean_url, get_selenium_driver, cleanup_selenium_driver
    from .error_handling import with_standard_error_handling

# Setup module logger
logger = get_logger(__name__)

# Get configuration
config = get_config()

# Base directories
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CACHE_DIR = os.path.join(BASE_DIR, "cache")

# Only cache Google Sheets HTML
GOOGLE_SHEET_CACHE_FILE = os.path.join(CACHE_DIR, "google_sheet_cache.html")

# Selenium driver functions are now imported from patterns.py (DRY consolidation)

@rate_limit('selenium')
@with_standard_error_handling("Selenium HTML extraction", "")
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

@with_standard_error_handling("Google Doc text extraction", "")
def extract_google_doc_text(url, driver=None):
    """Enhanced Google Doc text extraction with JavaScript and dynamic waiting
    
    Consolidates extraction logic from multiple duplicate implementations.
    Uses advanced techniques for modern Google Docs content extraction.
    
    Args:
        url (str): Google Doc URL to extract text from
        driver: Optional existing Selenium driver instance
        
    Returns:
        str: Extracted text content from the document
    """
    # Use provided driver or get the shared one
    if driver is None:
        driver = get_selenium_driver()
    if not driver:
        logger.error("Failed to initialize Selenium driver")
        return ""
    
    logger.info(f"Loading Google Doc with enhanced extraction: {url}")
    start_time = time.time()
    driver.get(url)
    
    # Wait for page to load
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )
    load_time = time.time() - start_time
    logger.info(f"Page loaded in {load_time:.2f} seconds")
    
    # Dynamic wait for content to stabilize
    logger.info("Waiting for content to stabilize...")
    previous_content_length = 0
    stable_checks = 0
    max_wait = 30
    start_wait = time.time()
    
    while time.time() - start_wait < max_wait:
        try:
            current_content_length = driver.execute_script("""
                var content = document.body.innerText || '';
                var editables = document.querySelectorAll('[contenteditable="true"]');
                for (var i = 0; i < editables.length; i++) {
                    content += editables[i].innerText || '';
                }
                return content.length;
            """)
            
            if current_content_length > previous_content_length:
                previous_content_length = current_content_length
                stable_checks = 0
                time.sleep(2)
            elif current_content_length == previous_content_length and current_content_length > 100:
                stable_checks += 1
                if stable_checks >= 2:
                    logger.info(f"Content stabilized at {current_content_length} chars")
                    break
                time.sleep(1)
            else:
                time.sleep(2)
        except:
            time.sleep(2)
    
    # Enhanced JavaScript-based extraction
    logger.info("Extracting content with JavaScript...")
    extraction_result = driver.execute_script("""
            var content = '';
            
            // Method 1: Extract from contenteditable areas (main document content)
            var editables = document.querySelectorAll('[contenteditable="true"]');
            for (var i = 0; i < editables.length; i++) {
                var text = editables[i].innerText || editables[i].textContent || '';
                if (text.length > 20) {
                    content += text + '\\n';
                }
            }
            
            // Method 2: Extract from document/textbox roles
            var docElements = document.querySelectorAll('[role="document"], [role="textbox"]');
            for (var i = 0; i < docElements.length; i++) {
                var text = docElements[i].innerText || docElements[i].textContent || '';
                if (text.length > 20 && content.indexOf(text.substring(0, 50)) === -1) {
                    content += text + '\\n';
                }
            }
            
            // Method 3: Look for Google Docs specific content areas
            var kixElements = document.querySelectorAll('[class*="kix"]');
            for (var i = 0; i < kixElements.length; i++) {
                var text = kixElements[i].innerText || kixElements[i].textContent || '';
                if (text.length > 50 && content.indexOf(text.substring(0, 50)) === -1) {
                    content += text + '\\n';
                }
            }
            
            // Method 4: Try iframe content if accessible
            var iframes = document.querySelectorAll('iframe');
            for (var i = 0; i < iframes.length; i++) {
                try {
                    var iframeDoc = iframes[i].contentDocument || iframes[i].contentWindow.document;
                    if (iframeDoc && iframeDoc.body) {
                        var text = iframeDoc.body.innerText || iframeDoc.body.textContent || '';
                        if (text.length > 50 && content.indexOf(text.substring(0, 50)) === -1) {
                            content += text + '\\n';
                        }
                    }
                } catch (e) {
                    // Cross-origin iframe, skip
                }
            }
            
            // Fallback: get all meaningful text
            if (content.length < 100) {
                content = document.body.innerText || document.body.textContent || '';
            }
            
            return content.trim();
    """)
    
    # Clean up the extracted content
    text_content = re.sub(r'\s+', ' ', extraction_result).strip()
    
    # Additional verification - ensure we got meaningful content
    if len(text_content) < 50:
        logger.warning("Low content extraction, trying fallback...")
        # Fallback to BeautifulSoup extraction
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        body = soup.find('body')
        if body:
            fallback_text = body.get_text(separator=' ', strip=True)
            if len(fallback_text) > len(text_content):
                text_content = fallback_text
    
    logger.info(f"Enhanced extraction: {len(text_content)} characters")
    
    # Quality assessment
    if len(text_content) > 200:
        logger.debug("Quality: High content volume")
    elif len(text_content) > 50:
        logger.debug("Quality: Moderate content volume")
    else:
        logger.warning("Quality: Low content volume - may need manual review")
    
    return text_content
        # Note: Error handling now provided by @with_standard_error_handling decorator (DRY)

@with_standard_error_handling("Document text extraction with retry", ("", "Failed to extract text"))
def extract_text_with_retry(doc_url, max_attempts=None):
    """Extract text from document with retry logic (DRY consolidation)"""
    if max_attempts is None:
        max_attempts = config.get("retry.max_attempts", 3)
    
    for attempt in range(max_attempts):
        try:
            logger.info(f"Attempt {attempt + 1}/{max_attempts}: Extracting text from {doc_url}")
            text = extract_google_doc_text(doc_url)
            if text and len(text.strip()) > 0:
                logger.info(f"Successfully extracted {len(text)} characters")
                return text, None
            else:
                logger.warning(f"No text extracted on attempt {attempt + 1}")
        except Exception as e:
            error_msg = str(e)
            logger.error(f"Attempt {attempt + 1} failed: {error_msg}")
            if attempt < max_attempts - 1:
                retry_delay = config.get("retry.base_delay", 2.0)
                logger.info(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)
    
    return "", f"Failed after {max_attempts} attempts"

def extract_actual_url(google_url):
    """Extract the actual URL from a Google redirect URL
    
    Consolidates URL extraction logic from multiple duplicate implementations.
    
    Args:
        google_url (str): Google redirect URL (starts with https://www.google.com/url?q=)
        
    Returns:
        str: The actual URL extracted from the Google redirect, or original URL if not a redirect
    """
    if not google_url.startswith("https://www.google.com/url?q="):
        return google_url
    
    # Extract the 'q' parameter which contains the actual URL
    start_idx = google_url.find("q=") + 2
    end_idx = google_url.find("&", start_idx)
    if end_idx == -1:
        actual_url = google_url[start_idx:]
    else:
        actual_url = google_url[start_idx:end_idx]
    
    # URL decode
    return urllib.parse.unquote(actual_url)

@with_standard_error_handling("HTML download", "")
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

def extract_youtube_playlists(links):
    """Extract YouTube playlist URLs from a list of links.
    
    This function specifically looks for actual YouTube playlists (not synthetic ones)
    and returns clean playlist URLs with just the list parameter.
    
    Args:
        links: List of URLs to search for playlists
        
    Returns:
        List of clean YouTube playlist URLs
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
            logger.error(f"Error parsing playlist link {link}: {str(e)}")
            continue
    return playlists

def build_youtube_playlist_url(yt_ids):
    if yt_ids:
        return "https://www.youtube.com/watch_videos?video_ids=" + ",".join(yt_ids)
    return None

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
    cleanup_selenium_driver()