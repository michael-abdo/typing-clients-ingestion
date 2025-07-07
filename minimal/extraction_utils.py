#!/usr/bin/env python3
"""
Extraction utilities - Reusable functions for document and link extraction
Following DRY principles: All extraction logic in one place
"""

import re
import time
import urllib.parse
from typing import Dict, List, Tuple, Optional, Callable, Any
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Global selenium driver
_driver = None

def get_selenium_driver():
    """Initialize and return a Selenium WebDriver instance with Google Docs optimization"""
    global _driver
    if _driver is None:
        print("Initializing Selenium Chrome driver with Google Docs support...")
        # DRY: Use consolidated Chrome driver creation from utils/config.py
        import sys
        import os
        sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        from utils.config import create_chrome_driver
        
        try:
            _driver = create_chrome_driver(config_type="extraction", headless=True)
            print("✓ Driver initialized with consolidated configuration")
        except Exception as e:
            print(f"Could not initialize Selenium driver: {str(e)}")
            return None
    
    return _driver

def cleanup_driver():
    """Clean up the Selenium driver"""
    global _driver
    if _driver is not None:
        try:
            _driver.quit()
            _driver = None
            print("Selenium driver cleaned up")
        except Exception as e:
            print(f"Error cleaning up Selenium driver: {e}")

def extract_actual_url(google_url: str) -> str:
    """Extract the actual URL from a Google redirect URL"""
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

def clean_url(url: str) -> str:
    """Clean up a URL by removing trailing junk and escape sequences"""
    if not url:
        return url
    
    # Decode unicode escapes
    try:
        if '\\u' in url:
            url = url.encode('utf-8').decode('unicode_escape')
    except:
        pass
    
    # Remove common escape sequences
    url = url.replace('\\n', '').replace('\\r', '').replace('\\t', '')
    
    # Remove trailing punctuation that's not part of a URL
    url = re.sub(r'[.,;:!?\)\]\}"\'>]+$', '', url)
    
    # Clean YouTube URLs
    if 'youtube.com' in url or 'youtu.be' in url:
        # For youtu.be links
        match = re.search(r'youtu\.be/([a-zA-Z0-9_-]{11})', url)
        if match:
            return f'https://youtu.be/{match.group(1)}'
        
        # For youtube.com watch links
        match = re.search(r'youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})', url)
        if match:
            video_id = match.group(1)
            # Check for additional parameters
            list_match = re.search(r'[&?]list=([a-zA-Z0-9_-]+)', url)
            if list_match:
                return f'https://www.youtube.com/watch?v={video_id}&list={list_match.group(1)}'
            return f'https://www.youtube.com/watch?v={video_id}'
        
        # For playlist links
        match = re.search(r'youtube\.com/playlist\?list=([a-zA-Z0-9_-]+)', url)
        if match:
            return f'https://www.youtube.com/playlist?list={match.group(1)}'
    
    # Clean Google Drive URLs
    if 'drive.google.com' in url:
        # File links
        match = re.search(r'drive\.google\.com/file/d/([a-zA-Z0-9_-]+)', url)
        if match:
            return f'https://drive.google.com/file/d/{match.group(1)}/view'
        
        # Folder links
        match = re.search(r'drive\.google\.com/drive/folders/([a-zA-Z0-9_-]+)', url)
        if match:
            return f'https://drive.google.com/drive/folders/{match.group(1)}'
    
    return url.strip()

def determine_document_type(url: str) -> str:
    """Determine the type of document from URL"""
    if 'docs.google.com' in url:
        return 'google_doc'
    elif 'youtube.com' in url or 'youtu.be' in url:
        return 'youtube'
    elif 'drive.google.com' in url:
        return 'google_drive'
    else:
        return 'other'

def extract_google_doc_text(url: str, auth_cookies: Optional[Dict] = None) -> str:
    """Extract text content from a Google Doc using requests first, then Selenium fallback"""
    
    # First try requests-based approach for Google Docs
    try:
        print(f"  Attempting requests-based extraction...")
        import requests
        
        # Convert edit URL to export URL for plain text
        if 'docs.google.com/document' in url:
            # Extract document ID
            if '/d/' in url:
                doc_id = url.split('/d/')[1].split('/')[0]
                export_url = f"https://docs.google.com/document/d/{doc_id}/export?format=txt"
                
                headers = {
                    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                }
                
                response = requests.get(export_url, headers=headers, timeout=30)
                if response.status_code == 200:
                    doc_text = response.text.strip()
                    if doc_text and len(doc_text) > 20:  # Minimum content check
                        print(f"  ✓ Extracted {len(doc_text)} characters via requests")
                        return doc_text
                    else:
                        print(f"  ⚠️ Export returned minimal content ({len(doc_text)} chars), trying Selenium...")
                else:
                    print(f"  ⚠️ Export failed (status {response.status_code}), trying Selenium...")
        
    except Exception as e:
        print(f"  ⚠️ Requests extraction failed: {e}, trying Selenium...")
    
    # Fallback to Selenium 
    print(f"  Falling back to Selenium extraction...")
    driver = get_selenium_driver()
    if not driver:
        print("Failed to initialize Selenium driver")
        return ""
    
    try:
        # Add authentication cookies if provided
        if auth_cookies:
            driver.get("https://accounts.google.com")
            for name, value in auth_cookies.items():
                driver.add_cookie({'name': name, 'value': value})
        
        print(f"Loading Google Doc with Selenium: {url}")
        driver.get(url)
        
        # Check for access denied or login required
        time.sleep(2)
        page_text = driver.find_element(By.TAG_NAME, "body").text.lower()
        if any(phrase in page_text for phrase in ["access denied", "sign in", "request access", "you need permission"]):
            print("  ⚠ Document requires authentication or permission")
            return "[AUTHENTICATION_REQUIRED]"
        
        # Wait for page to load
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Wait specifically for Google Docs content to load
        try:
            # Wait for any of the Google Docs content containers
            WebDriverWait(driver, 15).until(
                lambda d: any([
                    d.find_elements(By.CSS_SELECTOR, '.kix-page'),
                    d.find_elements(By.CSS_SELECTOR, '.kix-page-content'),
                    d.find_elements(By.CSS_SELECTOR, '.kix-lineview'),
                    d.find_elements(By.CSS_SELECTOR, '[data-kix-canvas]')
                ])
            )
            print("  ✓ Google Docs content containers loaded")
        except:
            print("  ⚠ Google Docs content containers not found - continuing anyway")
        
        # Extra wait for Google Docs to render
        time.sleep(3)
        
        # Wait for content to be non-empty
        for attempt in range(5):
            current_text = driver.find_element(By.TAG_NAME, "body").text
            if current_text and len(current_text.strip()) > 100:  # Meaningful content
                print("  ✓ Meaningful content detected")
                break
            print(f"  ⏳ Waiting for content... attempt {attempt + 1}")
            time.sleep(2)
        
        # Scroll to ensure all content is loaded
        height = driver.execute_script("return document.body.scrollHeight")
        for i in range(0, height, 300):
            driver.execute_script(f"window.scrollTo(0, {i});")
            time.sleep(0.1)
        
        # Scroll back to top
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(1)
        
        # Get the page source and extract text
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        
        # Check if we're getting the blocked message
        if "JavaScript isn't enabled" in html or "blocked" in html.lower():
            print("  ⚠ Detected JavaScript blocking - may need different approach")
        
        # Extract text from the document content
        text_content = ""
        
        # Try different selectors for Google Docs content (ordered by specificity)
        content_selectors = [
            '.kix-page-content-wrap',
            '.kix-page-content', 
            '.kix-lineview-content',
            '.kix-lineview',
            '.kix-page',
            '.kix-pagesettings-protected-text',
            '.doc-content',
            '[data-kix-canvas]',
            '.google-docs-content'
        ]
        
        for selector in content_selectors:
            elements = soup.select(selector)
            if elements:
                print(f"  Found {len(elements)} elements with selector: {selector}")
                for element in elements:
                    element_text = element.get_text(separator=' ', strip=True)
                    if element_text:  # Only add non-empty text
                        text_content += element_text + " "
                if text_content.strip():  # If we got content, break
                    break
        
        # Fallback: extract all text from body
        if not text_content.strip():
            body = soup.find('body')
            if body:
                text_content = body.get_text(separator=' ', strip=True)
        
        # Clean up the text
        text_content = re.sub(r'\s+', ' ', text_content).strip()
        
        print(f"✓ Extracted {len(text_content)} characters of text")
        return text_content
        
    except Exception as e:
        print(f"✗ Error extracting text from {url}: {e}")
        return ""

def extract_google_doc_content_and_links(url: str, auth_cookies: Optional[Dict] = None) -> tuple[str, str, Dict[str, List[str]]]:
    """Extract both text content and links from a Google Doc using requests first, Selenium fallback"""
    
    # First try requests-based extraction for the text content
    doc_text = ""
    html_content = ""
    
    try:
        print(f"  Attempting requests-based extraction...")
        import requests
        
        # Convert edit URL to export URL for plain text
        if 'docs.google.com/document' in url:
            # Extract document ID
            if '/d/' in url:
                doc_id = url.split('/d/')[1].split('/')[0]
                export_url = f"https://docs.google.com/document/d/{doc_id}/export?format=txt"
                
                headers = {
                    'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36'
                }
                
                response = requests.get(export_url, headers=headers, timeout=30)
                if response.status_code == 200:
                    doc_text = response.text.strip()
                    if doc_text and len(doc_text) > 20:  # Minimum content check
                        print(f"  ✓ Extracted {len(doc_text)} characters via requests")
                        
                        # Also try to get HTML content for link extraction
                        try:
                            html_response = requests.get(url, headers=headers, timeout=30)
                            if html_response.status_code == 200:
                                html_content = html_response.text
                                print(f"  ✓ Also extracted HTML content ({len(html_content)} chars)")
                        except:
                            print(f"  ⚠️ Could not get HTML, will extract links from text only")
                        
                        # Extract links from the content we have
                        links = extract_links_from_content(html_content, doc_text)
                        return doc_text, html_content, links
                    else:
                        print(f"  ⚠️ Export returned minimal content ({len(doc_text)} chars), trying Selenium...")
                else:
                    print(f"  ⚠️ Export failed (status {response.status_code}), trying Selenium...")
        
    except Exception as e:
        print(f"  ⚠️ Requests extraction failed: {e}, trying Selenium...")
    
    # Fallback to Selenium if requests fails
    print(f"  Falling back to Selenium extraction...")
    driver = get_selenium_driver()
    if not driver:
        print("Failed to initialize Selenium driver")
        return "", "", {'youtube': [], 'drive_files': [], 'drive_folders': [], 'all_links': []}
    
    try:
        # Add authentication cookies if provided
        if auth_cookies:
            driver.get("https://accounts.google.com")
            for name, value in auth_cookies.items():
                driver.add_cookie({'name': name, 'value': value})
        
        print(f"Loading Google Doc with Selenium: {url}")
        driver.get(url)
        
        # Check for access denied or login required
        time.sleep(2)
        page_text = driver.find_element(By.TAG_NAME, "body").text.lower()
        if any(phrase in page_text for phrase in ["access denied", "sign in", "request access", "you need permission"]):
            print("  ⚠ Document requires authentication or permission")
            return "[AUTHENTICATION_REQUIRED]", "", {'youtube': [], 'drive_files': [], 'drive_folders': [], 'all_links': []}
        
        # Wait for page to load
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Wait specifically for Google Docs content to load
        try:
            # Wait for any of the Google Docs content containers
            WebDriverWait(driver, 15).until(
                lambda d: any([
                    d.find_elements(By.CSS_SELECTOR, '.kix-page'),
                    d.find_elements(By.CSS_SELECTOR, '.kix-page-content'),
                    d.find_elements(By.CSS_SELECTOR, '.kix-lineview'),
                    d.find_elements(By.CSS_SELECTOR, '[data-kix-canvas]')
                ])
            )
            print("  ✓ Google Docs content containers loaded")
        except:
            print("  ⚠ Google Docs content containers not found - continuing anyway")
        
        # Extra wait for Google Docs to render
        time.sleep(3)
        
        # Wait for content to be non-empty
        for attempt in range(5):
            current_text = driver.find_element(By.TAG_NAME, "body").text
            if current_text and len(current_text.strip()) > 100:  # Meaningful content
                print("  ✓ Meaningful content detected")
                break
            print(f"  ⏳ Waiting for content... attempt {attempt + 1}")
            time.sleep(2)
        
        # Scroll to ensure all content is loaded
        height = driver.execute_script("return document.body.scrollHeight")
        for i in range(0, height, 300):
            driver.execute_script(f"window.scrollTo(0, {i});")
            time.sleep(0.1)
        
        # Scroll back to top
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(1)
        
        # Get the page source
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        
        # Check if we're getting the blocked message
        if "JavaScript isn't enabled" in html or "blocked" in html.lower():
            print("  ⚠ Detected JavaScript blocking - may need different approach")
        
        # Extract text from the document content
        text_content = ""
        
        # Try different selectors for Google Docs content (ordered by specificity)
        content_selectors = [
            '.kix-page-content-wrap',
            '.kix-page-content', 
            '.kix-lineview-content',
            '.kix-lineview',
            '.kix-page',
            '.kix-pagesettings-protected-text',
            '.doc-content',
            '[data-kix-canvas]',
            '.google-docs-content'
        ]
        
        for selector in content_selectors:
            elements = soup.select(selector)
            if elements:
                print(f"  Found {len(elements)} elements with selector: {selector}")
                for element in elements:
                    element_text = element.get_text(separator=' ', strip=True)
                    if element_text:  # Only add non-empty text
                        text_content += element_text + " "
                if text_content.strip():  # If we got content, break
                    break
        
        # Fallback: extract all text from body
        if not text_content.strip():
            body = soup.find('body')
            if body:
                text_content = body.get_text(separator=' ', strip=True)
        
        # Clean up the text
        text_content = re.sub(r'\s+', ' ', text_content).strip()
        
        # Extract links from the rendered HTML
        links = extract_links_from_content(html, text_content)
        
        print(f"✓ Extracted {len(text_content)} characters of text and {sum(len(v) for v in links.values() if isinstance(v, list))} links")
        return text_content, html, links
        
    except Exception as e:
        print(f"✗ Error extracting content from {url}: {e}")
        return "", "", {'youtube': [], 'drive_files': [], 'drive_folders': [], 'all_links': []}

def extract_links_from_content(doc_content: str, doc_text: str = "") -> Dict[str, List[str]]:
    """Extract links from document content and text"""
    print("Extracting links from content...")
    
    # Combine HTML content and plain text for comprehensive link extraction
    combined_content = doc_content + " " + doc_text
    
    links = {
        'youtube': [],
        'drive_files': [],
        'drive_folders': [],
        'all_links': []
    }
    
    # Enhanced YouTube patterns
    youtube_patterns = [
        r'https?://(?:www\.)?youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})[^\s<>"]*',
        r'https?://youtu\.be/([a-zA-Z0-9_-]{11})[^\s<>"]*',
        r'https?://(?:www\.)?youtube\.com/playlist\?list=([a-zA-Z0-9_-]+)[^\s<>"]*'
    ]
    
    for pattern in youtube_patterns:
        matches = re.findall(pattern, combined_content, re.IGNORECASE)
        for match in matches:
            if 'playlist' in pattern:
                clean_link = f'https://www.youtube.com/playlist?list={match}'
            else:
                clean_link = f'https://www.youtube.com/watch?v={match}'
            
            if clean_link not in links['youtube']:
                links['youtube'].append(clean_link)
    
    # Enhanced Google Drive patterns
    drive_patterns = [
        r'https://drive\.google\.com/file/d/([a-zA-Z0-9_-]+)[^\s<>"]*',
        r'https://drive\.google\.com/open\?id=([a-zA-Z0-9_-]+)[^\s<>"]*',
        r'https://drive\.google\.com/drive/folders/([a-zA-Z0-9_-]+)[^\s<>"]*'
    ]
    
    for pattern in drive_patterns:
        matches = re.findall(pattern, combined_content, re.IGNORECASE)
        for match in matches:
            if 'folders' in pattern:
                clean_link = f'https://drive.google.com/drive/folders/{match}'
                if clean_link not in links['drive_folders']:
                    links['drive_folders'].append(clean_link)
            else:
                clean_link = f'https://drive.google.com/file/d/{match}/view'
                if clean_link not in links['drive_files']:
                    links['drive_files'].append(clean_link)
    
    # Extract all HTTP(S) links for comprehensive coverage
    all_link_pattern = r'https?://[^\s<>"{}\\|\^\[\]`]+[^\s<>"{}\\|\^\[\]`.,;:!?\)\]]'
    all_found_links = re.findall(all_link_pattern, combined_content, re.IGNORECASE)
    
    # Clean and categorize all links
    for link in all_found_links:
        clean_link = clean_url(link)
        if clean_link and clean_link not in links['all_links']:
            links['all_links'].append(clean_link)
            
            # Additional categorization for missed links
            if ('youtube.com' in clean_link or 'youtu.be' in clean_link) and clean_link not in links['youtube']:
                links['youtube'].append(clean_link)
            elif 'drive.google.com/file' in clean_link and clean_link not in links['drive_files']:
                links['drive_files'].append(clean_link)
            elif 'drive.google.com/drive/folders' in clean_link and clean_link not in links['drive_folders']:
                links['drive_folders'].append(clean_link)
    
    # Remove duplicates
    links['youtube'] = list(set(links['youtube']))
    links['drive_files'] = list(set(links['drive_files']))
    links['drive_folders'] = list(set(links['drive_folders']))
    links['all_links'] = list(set(links['all_links']))
    
    total_links = len(links['youtube']) + len(links['drive_files']) + len(links['drive_folders'])
    print(f"✓ Found {total_links} targeted links (YT: {len(links['youtube'])}, Files: {len(links['drive_files'])}, Folders: {len(links['drive_folders'])})")
    print(f"✓ Found {len(links['all_links'])} total links")
    
    return links

def filter_meaningful_links(links: Dict[str, List[str]]) -> Dict[str, List[str]]:
    """Filter links to keep only meaningful content links"""
    meaningful_youtube = []
    
    # Filter YouTube links - only keep actual video/playlist content
    for link in links.get('youtube', []):
        if any([
            '/watch?v=' in link and len(re.findall(r'v=([a-zA-Z0-9_-]{11})', link)) > 0,
            '/playlist?list=' in link,
            'youtu.be/' in link and len(link.split('/')[-1]) == 11
        ]):
            # Clean the link to get just the essential content
            if '/watch?v=' in link:
                match = re.search(r'v=([a-zA-Z0-9_-]{11})', link)
                if match:
                    meaningful_youtube.append(f"https://www.youtube.com/watch?v={match.group(1)}")
            elif '/playlist?list=' in link:
                match = re.search(r'list=([a-zA-Z0-9_-]+)', link)
                if match:
                    meaningful_youtube.append(f"https://www.youtube.com/playlist?list={match.group(1)}")
            elif 'youtu.be/' in link:
                video_id = link.split('/')[-1]
                if len(video_id) == 11:
                    meaningful_youtube.append(f"https://www.youtube.com/watch?v={video_id}")
    
    # Remove duplicates
    meaningful_youtube = list(set(meaningful_youtube))
    
    return {
        'youtube': meaningful_youtube,
        'drive_files': links.get('drive_files', []),
        'drive_folders': links.get('drive_folders', []),
        'all_links': links.get('all_links', [])
    }

def extract_people_from_sheet_html(html_content: str, target_div_id: str = "1159146182") -> List[Dict]:
    """Extract people data from Google Sheet HTML"""
    print("Extracting people data from sheet...")
    
    soup = BeautifulSoup(html_content, "html.parser")
    
    # Look for the specific div with target ID
    target_div = soup.find("div", {"id": target_div_id})
    if target_div:
        table = target_div.find("table")
    else:
        # Fallback to any table
        table = soup.find("table", {"class": "waffle"})
        if not table:
            tables = soup.find_all("table")
            table = tables[0] if tables else None
    
    people_data = []
    if table:
        rows = table.find_all("tr")
        print(f"Found {len(rows)} rows in the table")
        
        # Process rows starting from row 1 (skip header)
        for row_index in range(1, len(rows)):
            row = rows[row_index]
            cells = row.find_all("td")
            
            # Need at least 5 cells (row_id, name, email, type)
            if len(cells) < 5:
                continue
            
            # Extract data using the correct column indices
            row_id = cells[0].get_text(strip=True)
            name = cells[2].get_text(strip=True)  # Name in column 2
            email = cells[3].get_text(strip=True)  # Email in column 3  
            type_val = cells[4].get_text(strip=True)  # Type in column 4
            
            # Skip header rows and invalid data
            if not name or name.lower() == "name" or row_id == "#" or "name" in name.lower() and "email" in email.lower():
                continue
            
            # Skip any row that looks like a header
            if any(["Name" in str(cell.get_text(strip=True)) and "Email" in str(cells[3].get_text(strip=True)) and "Type" in str(cells[4].get_text(strip=True)) for cell in cells]):
                continue
            
            # Look for Google Doc link in the name cell
            doc_link = None
            name_cell = cells[2]
            a_tags = name_cell.find_all("a")
            if a_tags:
                a_tag = a_tags[0]
                if a_tag.has_attr("href"):
                    href = a_tag["href"]
                    if href.startswith("https://www.google.com/url?q="):
                        doc_link = extract_actual_url(href)
            
            people_data.append({
                "row_id": row_id,
                "name": name,
                "email": email,
                "type": type_val,
                "doc_link": doc_link if doc_link else ""
            })
    
    print(f"✓ Found {len(people_data)} people records")
    return people_data

# DRY: Moved from workflow files to eliminate duplication
def download_google_sheet(sheet_url: str) -> str:
    """Step 1: Download a local copy of the Google Sheet"""
    print("Step 1: Downloading Google Sheet...")
    
    import requests
    response = requests.get(sheet_url)
    response.raise_for_status()
    
    # Save the HTML
    with open("sheet.html", "w", encoding="utf-8") as f:
        f.write(response.text)
    
    print("✓ Sheet downloaded")
    return response.text

def scrape_document_content(doc_url: str) -> tuple[str, str]:
    """Step 3: Scrape contents and text of a Google Doc"""
    print(f"Step 3: Scraping doc: {doc_url}")
    
    # For Google Docs, use Selenium to get both HTML and text
    if "docs.google.com/document" in doc_url:
        # Extract the document text using Selenium
        doc_text = extract_google_doc_text(doc_url)
        
        # Also get the HTML for link extraction
        try:
            import requests
            response = requests.get(doc_url)
            response.raise_for_status()
            html_content = response.text
            print("✓ Doc scraped successfully (HTML + text)")
            return html_content, doc_text
        except Exception as e:
            print(f"✗ Failed to scrape HTML: {e}")
            return "", doc_text
    else:
        # For other URLs, just get HTML
        try:
            import requests
            response = requests.get(doc_url)
            response.raise_for_status()
            print("✓ Doc scraped successfully (HTML only)")
            return response.text, ""
        except Exception as e:
            print(f"✗ Failed to scrape doc: {e}")
            return "", ""

def extract_text_with_retry(doc_url: str, max_attempts: int = 3, delay: int = 2) -> tuple[str, Optional[str]]:
    """Extract text from document with retry logic"""
    for attempt in range(max_attempts):
        try:
            print(f"  Attempt {attempt + 1}/{max_attempts}: Extracting text...")
            text = extract_google_doc_text(doc_url)
            if text and len(text.strip()) > 0:
                print(f"  ✓ Extracted {len(text)} characters")
                return text, None
            else:
                print(f"  ⚠ No text extracted on attempt {attempt + 1}")
        except Exception as e:
            error_msg = str(e)
            print(f"  ✗ Attempt {attempt + 1} failed: {error_msg}")
            if attempt < max_attempts - 1:
                print(f"  Retrying in {delay} seconds...")
                time.sleep(delay)
    
    return "", f"Failed after {max_attempts} attempts"


# ============================================================================
# Test Utilities Section (DRY: Consolidate test file duplications)
# ============================================================================

def categorize_link(url: str) -> str:
    """Categorize a link by its type (DRY: consolidate from test files)"""
    if not url:
        return 'other'
    
    url_lower = url.lower()
    
    if 'youtube.com/watch' in url_lower or 'youtu.be' in url_lower:
        return 'youtube_video'
    elif 'youtube.com/playlist' in url_lower:
        return 'youtube_playlist'
    elif 'docs.google.com/document' in url_lower or 'docs.google.com/open?id=' in url_lower:
        return 'google_doc'
    elif 'drive.google.com/file' in url_lower:
        return 'google_drive_file'
    elif 'drive.google.com/drive/folders' in url_lower:
        return 'google_drive_folder'
    else:
        return 'other'


def extract_people_with_all_links(sheet_html: str, target_div_id: str = "1159146182", 
                                   row_filter: Optional[Callable] = None) -> List[Dict]:
    """Extract people and ALL their links from any column with optional row filtering"""
    soup = BeautifulSoup(sheet_html, "html.parser")
    
    target_div = soup.find("div", {"id": target_div_id})
    if not target_div:
        return []
    
    table = target_div.find("table")
    if not table:
        return []
    
    rows = table.find_all("tr")
    people_data = []
    
    for row_index in range(1, len(rows)):  # Skip header
        row = rows[row_index]
        cells = row.find_all("td")
        
        if len(cells) < 5:
            continue
        
        # Extract basic data
        row_id = cells[0].get_text(strip=True)
        
        # Apply row filter if provided
        if row_filter and not row_filter(row_id):
            continue
            
        name = cells[2].get_text(strip=True)
        email = cells[3].get_text(strip=True)
        type_info = cells[4].get_text(strip=True)
        
        # Extract ALL links from ALL cells (excluding column 4)
        all_links = []
        for cell_idx, cell in enumerate(cells):
            # Skip column 4 (index 4) per user requirement
            if cell_idx == 4:
                continue
                
            cell_links = cell.find_all('a', href=True)
            for link in cell_links:
                actual_url = extract_actual_url(link['href'])
                if actual_url and actual_url != '#':
                    all_links.append({
                        'url': actual_url,
                        'column': cell_idx,
                        'type': categorize_link(actual_url)
                    })
        
        people_data.append({
            'row_id': row_id,
            'name': name,
            'email': email,
            'type': type_info,
            'links': all_links
        })
    
    return people_data


def create_row_range_filter(start_row: int, end_row: int) -> Callable:
    """Create a filter function for row ranges"""
    def filter_func(row_id: str) -> bool:
        try:
            row_num = int(row_id)
            return start_row <= row_num <= end_row
        except:
            return False
    return filter_func


def create_row_list_filter(row_ids: List[str]) -> Callable:
    """Create a filter function for specific row IDs"""
    row_id_set = set(str(id) for id in row_ids)
    def filter_func(row_id: str) -> bool:
        return str(row_id) in row_id_set
    return filter_func


def setup_test_database(db_path: str = 'test.db') -> Any:
    """Setup a test database with schema (returns DatabaseManager if available)"""
    # Try to use DatabaseManager if available
    try:
        from database_manager import DatabaseManager
        db = DatabaseManager(db_path)
        db.connect()
        db.create_tables()  # Use full schema
        return db
    except ImportError:
        # Fallback to basic sqlite3 for simple tests
        import sqlite3
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row
        
        # Create minimal test schema
        conn.executescript('''
            CREATE TABLE IF NOT EXISTS people (
                id INTEGER PRIMARY KEY,
                row_id INTEGER,
                name TEXT,
                email TEXT,
                type TEXT
            );
            
            CREATE TABLE IF NOT EXISTS documents (
                id INTEGER PRIMARY KEY,
                person_id INTEGER,
                url TEXT,
                document_text TEXT,
                document_type TEXT,
                processed BOOLEAN DEFAULT 0,
                extraction_date TEXT,
                FOREIGN KEY (person_id) REFERENCES people(id)
            );
            
            CREATE TABLE IF NOT EXISTS extracted_links (
                id INTEGER PRIMARY KEY,
                document_id INTEGER,
                person_id INTEGER,
                url TEXT,
                link_type TEXT,
                source TEXT,
                FOREIGN KEY (document_id) REFERENCES documents(id),
                FOREIGN KEY (person_id) REFERENCES people(id)
            );
        ''')
        conn.commit()
        return conn
