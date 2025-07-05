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
            
            # Skip rows without a name
            if not name:
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

def step4_extract_links(doc_content):
    """Step 4: Extract links from scraped content"""
    print("Step 4: Extracting links from doc content...")
    
    links = {
        'youtube': [],
        'drive_files': [],
        'drive_folders': []
    }
    
    # YouTube links
    yt_pattern = r'https?://(?:www\.)?(?:youtube\.com/watch\?v=|youtu\.be/)[a-zA-Z0-9_-]+'
    links['youtube'] = re.findall(yt_pattern, doc_content)
    
    # Drive file links
    drive_file_pattern = r'https://drive\.google\.com/file/d/[a-zA-Z0-9_-]+[^"\s<>]*'
    links['drive_files'] = re.findall(drive_file_pattern, doc_content)
    
    # Drive folder links
    drive_folder_pattern = r'https://drive\.google\.com/drive/folders/[a-zA-Z0-9_-]+[^"\s<>]*'
    links['drive_folders'] = re.findall(drive_folder_pattern, doc_content)
    
    total_links = len(links['youtube']) + len(links['drive_files']) + len(links['drive_folders'])
    print(f"✓ Found {total_links} links (YT: {len(links['youtube'])}, Files: {len(links['drive_files'])}, Folders: {len(links['drive_folders'])})")
    
    return links

def step5_download_links(links, name, email, type_info, doc_text=""):
    """Step 5: Download the links (placeholder - implement as needed)"""
    print("Step 5: Downloading links...")
    
    # Create output directory
    OUTPUT_DIR.mkdir(exist_ok=True)
    
    downloaded_items = []
    
    # For now, just log what we would download
    for yt_link in links['youtube']:
        print(f"  Would download YouTube: {yt_link}")
        downloaded_items.append({
            'type': 'youtube',
            'url': yt_link,
            'name': name,
            'email': email,
            'category': type_info,
            'document_text': doc_text,
            'status': 'pending'
        })
    
    for file_link in links['drive_files']:
        print(f"  Would download Drive file: {file_link}")
        downloaded_items.append({
            'type': 'drive_file',
            'url': file_link,
            'name': name,
            'email': email,
            'category': type_info,
            'document_text': doc_text,
            'status': 'pending'
        })
    
    for folder_link in links['drive_folders']:
        print(f"  Would download Drive folder: {folder_link}")
        downloaded_items.append({
            'type': 'drive_folder',
            'url': folder_link,
            'name': name,
            'email': email,
            'category': type_info,
            'document_text': doc_text,
            'status': 'pending'
        })
    
    print(f"✓ Processed {len(downloaded_items)} items")
    return downloaded_items

def step6_map_data(all_downloaded_items):
    """Step 6: Map downloaded content to Name/Email/Type columns"""
    print("Step 6: Mapping data to preserve Name/Email/Type...")
    
    # Create DataFrame
    df = pd.DataFrame(all_downloaded_items)
    
    # Save to CSV
    df.to_csv(OUTPUT_CSV, index=False)
    
    print(f"✓ Data mapped and saved to {OUTPUT_CSV}")
    print(f"  Total items: {len(df)}")
    print(f"  Unique people: {df['name'].nunique()}")
    
    return df

def main():
    """Run the complete 6-step workflow"""
    print("STARTING SIMPLE 6-STEP WORKFLOW")
    print("=" * 50)
    
    all_downloaded_items = []
    
    # Step 1: Download sheet
    html_content = step1_download_sheet()
    
    # Step 2: Extract people data and Google Doc links
    all_people, people_with_docs = step2_extract_people_and_docs(html_content)
    
    # Process each person with a doc (limit to first 3 for testing)
    for i, person in enumerate(people_with_docs[:3]):
        print(f"\nProcessing person {i+1}/{len(people_with_docs)}: {person['name']}")
        
        # Step 3: Scrape doc content and text
        doc_content, doc_text = step3_scrape_doc_contents(person['doc_link'])
        
        if doc_content or doc_text:
            # Step 4: Extract links from HTML content
            links = step4_extract_links(doc_content)
            
            # Step 5: Download links (with actual person data and document text)
            downloaded_items = step5_download_links(links, person['name'], person['email'], person['type'], doc_text)
            all_downloaded_items.extend(downloaded_items)
    
    # Step 6: Map all data
    if all_downloaded_items:
        step6_map_data(all_downloaded_items)
    else:
        print("No items to map")
    
    # Cleanup Selenium driver
    cleanup_driver()
    
    print("\n" + "=" * 50)
    print("WORKFLOW COMPLETE")

if __name__ == "__main__":
    main()