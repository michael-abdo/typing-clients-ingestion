#!/usr/bin/env python3
"""
SIMPLE 6-STEP WORKFLOW - MINIMAL IMPLEMENTATION
Keep it simple. No over-engineering.
"""

import requests
import pandas as pd
import re
from pathlib import Path
from urllib.parse import urlparse
import os
import urllib.parse
import time
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Constants
GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/u/1/d/e/2PACX-1vRqqjqoaj8sEZBfZRw0Og7g8ms_0yTL2MsegTubcjhhBnXr1s1jFBwIVAsbkyj1xD0TMj06LvGTQIHU/pubhtml?pli=1#"
TARGET_DIV_ID = "1159146182"
OUTPUT_DIR = Path("simple_downloads")
OUTPUT_CSV = "simple_output.csv"

# Global selenium driver
_driver = None

def get_selenium_driver():
    """Initialize and return a Selenium WebDriver instance"""
    global _driver
    if _driver is None:
        print("Initializing Selenium Chrome driver...")
        chrome_options = Options()
        chrome_options.add_argument("--headless")  # Run in headless mode
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

def extract_google_doc_text(url):
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
        # Google Docs puts content in divs with specific classes
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

def extract_actual_url(google_url):
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

def step1_download_sheet():
    """Step 1: Download a local copy of the Google Sheet"""
    print("Step 1: Downloading Google Sheet...")
    
    response = requests.get(GOOGLE_SHEET_URL)
    response.raise_for_status()
    
    # Save the HTML
    with open("sheet.html", "w", encoding="utf-8") as f:
        f.write(response.text)
    
    print("✓ Sheet downloaded")
    return response.text

def step2_extract_people_and_docs(html_content):
    """Step 2: Extract people data and Google Doc links from the sheet"""
    print("Step 2: Extracting people data and Google Doc links...")
    
    soup = BeautifulSoup(html_content, "html.parser")
    
    # Look for the specific div with target ID
    target_div = soup.find("div", {"id": TARGET_DIV_ID})
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
            
            # Extract data using the correct column indices from the working code
            row_id = cells[0].get_text(strip=True)
            name = cells[2].get_text(strip=True)  # Name in column 2
            email = cells[3].get_text(strip=True)  # Email in column 3  
            type_val = cells[4].get_text(strip=True)  # Type in column 4
            
            # Skip header rows and invalid data
            if not name or name.lower() == "name" or row_id == "#" or "name" in name.lower() and "email" in email.lower():
                continue
            
            # Skip any row that looks like a header (contains "Name", "Email", "Type" pattern)
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
    
    # Filter to only those with Google Doc links
    people_with_docs = [person for person in people_data if person["doc_link"]]
    print(f"✓ Found {len(people_with_docs)} people with Google Doc links")
    
    return people_data, people_with_docs

def step3_scrape_doc_contents(doc_url):
    """Step 3: Scrape contents and text of a Google Doc"""
    print(f"Step 3: Scraping doc: {doc_url}")
    
    # For Google Docs, use Selenium to get both HTML and text
    if "docs.google.com/document" in doc_url:
        # Extract the document text using Selenium
        doc_text = extract_google_doc_text(doc_url)
        
        # Also get the HTML for link extraction
        try:
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
            response = requests.get(doc_url)
            response.raise_for_status()
            print("✓ Doc scraped successfully (HTML only)")
            return response.text, ""
        except Exception as e:
            print(f"✗ Failed to scrape doc: {e}")
            return "", ""

def clean_url(url):
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

def step4_extract_links(doc_content, doc_text=""):
    """Step 4: Extract links from scraped content and document text"""
    print("Step 4: Extracting links from doc content...")
    
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

def step5_process_extracted_data(person, links, doc_text=""):
    """Step 5: Process extracted data and format for CSV matching main system"""
    print("Step 5: Processing extracted data...")
    
    # Create output directory
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    # Filter links to get only meaningful content links, not infrastructure
    meaningful_youtube = []
    meaningful_drive_files = []
    meaningful_drive_folders = []
    
    # Filter YouTube links - only keep actual video/playlist content
    for link in links['youtube']:
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
    
    # Keep all Drive links as they're likely meaningful
    meaningful_drive_files = links['drive_files']
    meaningful_drive_folders = links['drive_folders']
    
    # Remove duplicates
    meaningful_youtube = list(set(meaningful_youtube))
    
    # Create record matching the main system's CSV structure
    record = {
        'row_id': person.get('row_id', ''),
        'name': person['name'],
        'email': person['email'],
        'type': person['type'],
        'link': person.get('doc_link', ''),
        'extracted_links': '|'.join(links['all_links']) if links['all_links'] else '',
        'youtube_playlist': '|'.join(meaningful_youtube) if meaningful_youtube else '',
        'google_drive': '|'.join(meaningful_drive_files + meaningful_drive_folders) if (meaningful_drive_files or meaningful_drive_folders) else '',
        'processed': 'yes',
        'document_text': doc_text,
        'youtube_status': '',
        'youtube_files': '',
        'youtube_media_id': '',
        'drive_status': '',
        'drive_files': '',
        'drive_media_id': '',
        'last_download_attempt': '',
        'download_errors': '',
        'permanent_failure': ''
    }
    
    print(f"✓ Processed record for {person['name']}")
    print(f"  Meaningful YouTube links: {len(meaningful_youtube)}")
    print(f"  Drive Files: {len(meaningful_drive_files)}")
    print(f"  Drive Folders: {len(meaningful_drive_folders)}")
    print(f"  Total links extracted: {len(links['all_links'])}")
    
    return record

def step6_map_data(processed_records):
    """Step 6: Map data to CSV matching main system structure"""
    print("Step 6: Mapping data to CSV format...")
    
    # Create DataFrame with proper column order matching main system
    df = pd.DataFrame(processed_records)
    
    # Ensure all required columns are present
    required_columns = [
        'row_id', 'name', 'email', 'type', 'link', 'extracted_links', 
        'youtube_playlist', 'google_drive', 'processed', 'document_text',
        'youtube_status', 'youtube_files', 'youtube_media_id',
        'drive_status', 'drive_files', 'drive_media_id',
        'last_download_attempt', 'download_errors', 'permanent_failure'
    ]
    
    for col in required_columns:
        if col not in df.columns:
            df[col] = ''
    
    # Reorder columns to match main system
    df = df[required_columns]
    
    # Save to CSV
    df.to_csv(OUTPUT_CSV, index=False)
    
    print(f"✓ Data mapped and saved to {OUTPUT_CSV}")
    print(f"  Total records: {len(df)}")
    print(f"  Records with docs: {len(df[df['link'] != ''])}")
    print(f"  Records with YouTube: {len(df[df['youtube_playlist'] != ''])}")
    print(f"  Records with Drive: {len(df[df['google_drive'] != ''])}")
    
    return df

def main():
    """Run the complete 6-step workflow"""
    print("STARTING SIMPLE 6-STEP WORKFLOW")
    print("=" * 50)
    
    processed_records = []
    
    # Step 1: Download sheet
    html_content = step1_download_sheet()
    
    # Step 2: Extract people data and Google Doc links
    all_people, people_with_docs = step2_extract_people_and_docs(html_content)
    
    # Create records for all people (not just those with docs)
    print(f"\nProcessing all {len(all_people)} people...")
    
    # Process people with docs first
    for i, person in enumerate(people_with_docs[:5]):  # Limit to 5 for testing
        print(f"\nProcessing person with doc {i+1}/{len(people_with_docs)}: {person['name']}")
        
        # Step 3: Scrape doc content and text
        doc_content, doc_text = step3_scrape_doc_contents(person['doc_link'])
        
        # Step 4: Extract links from HTML content and document text
        links = step4_extract_links(doc_content, doc_text)
        
        # Step 5: Process extracted data
        record = step5_process_extracted_data(person, links, doc_text)
        processed_records.append(record)
    
    # Add remaining people without docs (limit to first 20 for testing)
    people_without_docs = [p for p in all_people if not p.get('doc_link')]
    for person in people_without_docs[:20]:
        record = {
            'row_id': person.get('row_id', ''),
            'name': person['name'],
            'email': person['email'],
            'type': person['type'],
            'link': '',
            'extracted_links': '',
            'youtube_playlist': '',
            'google_drive': '',
            'processed': 'yes',
            'document_text': '',
            'youtube_status': '',
            'youtube_files': '',
            'youtube_media_id': '',
            'drive_status': '',
            'drive_files': '',
            'drive_media_id': '',
            'last_download_attempt': '',
            'download_errors': '',
            'permanent_failure': ''
        }
        processed_records.append(record)
    
    # Step 6: Map all data
    if processed_records:
        step6_map_data(processed_records)
    else:
        print("No records to map")
    
    # Cleanup Selenium driver
    cleanup_driver()
    
    print("\n" + "=" * 50)
    print("WORKFLOW COMPLETE")

if __name__ == "__main__":
    main()