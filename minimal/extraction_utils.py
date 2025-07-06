#!/usr/bin/env python3
"""
Extraction utilities - Reusable functions for document and link extraction
Following DRY principles: All extraction logic in one place
"""

import re
import time
import urllib.parse
from typing import Dict, List, Tuple, Optional
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
    """Initialize and return a Selenium WebDriver instance"""
    global _driver
    if _driver is None:
        print("Initializing Selenium Chrome driver...")
        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--no-sandbox")
        
        try:
            _driver = webdriver.Chrome(options=chrome_options)
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

def extract_google_doc_text(url: str) -> str:
    """Extract text content from a Google Doc using Selenium"""
    driver = get_selenium_driver()
    if not driver:
        print("Failed to initialize Selenium driver")
        return ""
    
    try:
        print(f"Loading Google Doc with Selenium: {url}")
        driver.get(url)
        
        # Wait for page to load
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Extra wait for Google Docs to render
        time.sleep(3)
        
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
        
        # Extract text from the document content
        text_content = ""
        
        # Try different selectors for Google Docs content
        content_selectors = [
            '.kix-pagesettings-protected-text',
            '.kix-page',
            '.doc-content',
            '.google-docs-content'
        ]
        
        for selector in content_selectors:
            elements = soup.select(selector)
            if elements:
                for element in elements:
                    text_content += element.get_text(separator=' ', strip=True) + " "
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
