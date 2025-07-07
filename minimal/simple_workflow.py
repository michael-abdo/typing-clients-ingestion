#!/usr/bin/env python3
"""
SIMPLE 6-STEP WORKFLOW - MINIMAL IMPLEMENTATION
Keep it simple. No over-engineering.
"""

import requests
import pandas as pd
import re
import argparse
import sys
import json
import pickle
from datetime import datetime
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

# Import centralized configuration and utilities (DRY)
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from utils.config import get_config, ensure_directory, load_json_state, save_json_state
from utils.patterns import PatternRegistry, clean_url, get_chrome_options, get_selenium_driver, cleanup_selenium_driver
from utils.csv_manager import CSVManager

# Configuration - centralized in config.yaml (DRY)
config = get_config()



def extract_google_doc_text(url):
    """Enhanced extraction using JavaScript and dynamic waiting for modern Google Docs"""
    driver = get_selenium_driver()
    if not driver:
        print("Failed to initialize Selenium driver")
        return ""
    
    try:
        print(f"Loading Google Doc with enhanced extraction: {url}")
        start_time = time.time()
        driver.get(url)
        
        # Wait for page to load
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        load_time = time.time() - start_time
        print(f"  Page loaded in {load_time:.2f} seconds")
        
        # Dynamic wait for content to stabilize
        print("  Waiting for content to stabilize...")
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
                        print(f"  Content stabilized at {current_content_length} chars")
                        break
                    time.sleep(1)
                else:
                    time.sleep(2)
            except:
                time.sleep(2)
        
        # Enhanced JavaScript-based extraction
        print("  Extracting content with JavaScript...")
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
            print("  Warning: Low content extraction, trying fallback...")
            # Fallback to BeautifulSoup extraction
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            body = soup.find('body')
            if body:
                fallback_text = body.get_text(separator=' ', strip=True)
                if len(fallback_text) > len(text_content):
                    text_content = fallback_text
        
        print(f"✓ Enhanced extraction: {len(text_content)} characters")
        
        # Quality assessment
        if len(text_content) > 200:
            print("  Quality: High content volume")
        elif len(text_content) > 50:
            print("  Quality: Moderate content volume")
        else:
            print("  Quality: Low content volume - may need manual review")
        
        return text_content
        
    except Exception as e:
        print(f"✗ Enhanced extraction failed: {e}")
        # Fallback to basic extraction
        print("  Attempting basic fallback extraction...")
        try:
            soup = BeautifulSoup(driver.page_source, 'html.parser')
            body = soup.find('body')
            if body:
                return body.get_text(separator=' ', strip=True)
        except:
            pass
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
    
    response = requests.get(config.get("google_sheets.url"))
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
    target_div = soup.find("div", {"id": str(config.get("google_sheets.target_div_id"))})
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


def step4_extract_links(doc_content, doc_text=""):
    """Enhanced link extraction with infrastructure filtering and improved patterns"""
    print("Step 4: Enhanced link extraction from content...")
    
    # Combine HTML content and plain text for comprehensive link extraction
    combined_content = doc_content + " " + doc_text
    print(f"  Processing {len(combined_content)} characters of content")
    
    links = {
        'youtube': [],
        'drive_files': [],
        'drive_folders': [],
        'all_links': []
    }
    
    # Google infrastructure domains to filter out
    infrastructure_domains = [
        'gstatic.com', 'apis.google.com', 'accounts.google.com',
        'script.google.com', 'ssl.gstatic.com', 'docs.google.com/static',
        'myaccount.google.com', 'workspace.google.com', 'clients6.google.com',
        'people-pa.clients6.google.com', 'drivefrontend-pa.clients6.google.com',
        'addons-pa.clients6.google.com', 'addons.gsuite.google.com',
        'contacts.google.com', 'meet.google.com', 'chrome.google.com'
    ]
    
    # Enhanced YouTube patterns with better boundary detection
    youtube_patterns = [
        r'https?://(?:www\.)?youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})(?:[^\s<>"{}\\|\^\[\]`]*)?',
        r'https?://youtu\.be/([a-zA-Z0-9_-]{11})(?:[^\s<>"{}\\|\^\[\]`]*)?',
        r'https?://(?:www\.)?youtube\.com/playlist\?list=([a-zA-Z0-9_-]+)(?:[^\s<>"{}\\|\^\[\]`]*)?'
    ]
    
    # URL reconstruction for incomplete playlist URLs
    print("  🔧 Reconstructing incomplete URLs...")
    reconstructed_urls = []
    
    # Patterns for incomplete URLs
    incomplete_patterns = [
        (r'(https?://(?:www\.)?youtube\.com/playlist\?list)(?:$|\s|["\'])', r'\1='),
        (r'(playlist\?list)(?:$|\s|["\'])', r'https://www.youtube.com/\1='),
        (r'playlist\s*\?\s*list\s*=?\s*([a-zA-Z0-9_-]+)', r'https://www.youtube.com/playlist?list=\1'),
        (r'list\s*=\s*([a-zA-Z0-9_-]{10,})', r'https://www.youtube.com/playlist?list=\1'),
    ]
    
    # Apply reconstruction patterns
    for pattern, replacement in incomplete_patterns:
        matches = re.finditer(pattern, combined_content, re.IGNORECASE)
        for match in matches:
            if isinstance(replacement, str) and '\\1' in replacement:
                reconstructed = re.sub(pattern, replacement, match.group(0))
            else:
                reconstructed = replacement
            
            if 'playlist?list=' in reconstructed and len(reconstructed.split('list=')[-1]) >= 10:
                reconstructed_urls.append(reconstructed)
                print(f"    Reconstructed: {reconstructed}")
    
    # Find orphaned playlist IDs
    playlist_id_pattern = r'(?:PL[a-zA-Z0-9_-]{10,})'
    orphaned_ids = re.findall(playlist_id_pattern, combined_content)
    
    for playlist_id in orphaned_ids:
        # Check if this ID is already part of a complete URL
        if not re.search(f'playlist\?list={playlist_id}', combined_content):
            reconstructed_url = f'https://www.youtube.com/playlist?list={playlist_id}'
            reconstructed_urls.append(reconstructed_url)
            print(f"    Reconstructed from orphaned ID: {reconstructed_url}")
    
    # Add reconstructed URLs to the combined content for extraction
    if reconstructed_urls:
        combined_content = combined_content + " " + " ".join(reconstructed_urls)
        print(f"  Added {len(reconstructed_urls)} reconstructed URLs to content")
    
    print("  Extracting YouTube links...")
    for pattern in youtube_patterns:
        matches = re.finditer(pattern, combined_content, re.IGNORECASE)
        for match in matches:
            if 'playlist' in pattern:
                clean_link = f'https://www.youtube.com/playlist?list={match.group(1)}'
            else:
                clean_link = f'https://www.youtube.com/watch?v={match.group(1)}'
            
            if clean_link not in links['youtube']:
                links['youtube'].append(clean_link)
                print(f"    🎥 Found: {clean_link}")
    
    # Enhanced Google Drive patterns with better ID extraction
    drive_patterns = [
        r'https://drive\.google\.com/file/d/([a-zA-Z0-9_-]+)(?:[^\s<>"{}\\|\^\[\]`]*)?',
        r'https://drive\.google\.com/open\?id=([a-zA-Z0-9_-]+)(?:[^\s<>"{}\\|\^\[\]`]*)?',
        r'https://drive\.google\.com/drive/folders/([a-zA-Z0-9_-]+)(?:[^\s<>"{}\\|\^\[\]`]*)?'
    ]
    
    print("  Extracting Drive links...")
    for pattern in drive_patterns:
        matches = re.finditer(pattern, combined_content, re.IGNORECASE)
        for match in matches:
            if 'folders' in pattern:
                clean_link = f'https://drive.google.com/drive/folders/{match.group(1)}'
                if clean_link not in links['drive_folders']:
                    links['drive_folders'].append(clean_link)
                    print(f"    📁 Found folder: {clean_link}")
            else:
                clean_link = f'https://drive.google.com/file/d/{match.group(1)}/view'
                if clean_link not in links['drive_files']:
                    links['drive_files'].append(clean_link)
                    print(f"    📄 Found file: {clean_link}")
    
    # Extract all HTTP(S) links with infrastructure filtering
    print("  Extracting all meaningful links...")
    all_link_pattern = r'https?://[^\s<>"{}\\|\^\[\]`]+[^\s<>"{}\\|\^\[\]`.,;:!?\)\]]'
    all_found_links = re.findall(all_link_pattern, combined_content, re.IGNORECASE)
    
    meaningful_links_found = 0
    infrastructure_links_filtered = 0
    
    for link in all_found_links:
        # Filter out infrastructure links
        is_infrastructure = any(domain in link for domain in infrastructure_domains)
        
        if is_infrastructure:
            infrastructure_links_filtered += 1
            continue
            
        clean_link = clean_url(link)
        if clean_link and clean_link not in links['all_links']:
            links['all_links'].append(clean_link)
            meaningful_links_found += 1
            
            # Additional categorization for missed links
            if ('youtube.com' in clean_link or 'youtu.be' in clean_link) and clean_link not in links['youtube']:
                links['youtube'].append(clean_link)
                print(f"    🎥 Additional YouTube: {clean_link}")
            elif 'drive.google.com/file' in clean_link and clean_link not in links['drive_files']:
                links['drive_files'].append(clean_link)
                print(f"    📄 Additional Drive file: {clean_link}")
            elif 'drive.google.com/drive/folders' in clean_link and clean_link not in links['drive_folders']:
                links['drive_folders'].append(clean_link)
                print(f"    📁 Additional Drive folder: {clean_link}")
    
    print(f"  Filtered out {infrastructure_links_filtered} infrastructure links")
    print(f"  Kept {meaningful_links_found} meaningful links")
    
    # Remove duplicates
    links['youtube'] = list(set(links['youtube']))
    links['drive_files'] = list(set(links['drive_files']))
    links['drive_folders'] = list(set(links['drive_folders']))
    links['all_links'] = list(set(links['all_links']))
    
    # Enhanced reporting
    total_meaningful = len(links['youtube']) + len(links['drive_files']) + len(links['drive_folders'])
    print(f"✓ EXTRACTION SUMMARY:")
    print(f"  YouTube links: {len(links['youtube'])}")
    print(f"  Drive files: {len(links['drive_files'])}")
    print(f"  Drive folders: {len(links['drive_folders'])}")
    print(f"  Total meaningful content links: {total_meaningful}")
    print(f"  All links (including other domains): {len(links['all_links'])}")
    
    # Quality assessment
    if total_meaningful == 0:
        print("  ⚠️ WARNING: No meaningful content links found - may indicate extraction issues")
    elif total_meaningful > 5:
        print("  ✅ High link extraction quality")
    else:
        print("  ✅ Moderate link extraction quality")
    
    return links

def step5_process_extracted_data(person, links, doc_text=""):
    """Step 5: Process extracted data and format for CSV matching main system"""
    print("Step 5: Processing extracted data...")
    
    # Create output directory
    ensure_directory(config.get("paths.simple_downloads_dir"))
    
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


def extract_text_with_retry(doc_url, max_attempts=None):
    """Extract text from document with retry logic"""
    if max_attempts is None:
        max_attempts = config.get("retry.max_attempts", 3)
    
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
                delay = config.get("batch_processing.delay_between_docs", 2)
                print(f"  Retrying in {delay} seconds...")
                time.sleep(delay)
    
    return "", f"Failed after {max_attempts} attempts"

def step6_map_data(processed_records):
    """Step 6: Map data to CSV matching main system structure"""
    print("Step 6: Mapping data to CSV format...")
    
    # Create DataFrame with proper column order matching main system
    df = pd.DataFrame(processed_records)
    
    # Handle different column sets based on processing mode
    if processing_modes['basic_only']:
        # Basic mode: only 5 columns
        required_columns = ['row_id', 'name', 'email', 'type', 'link']
    elif processing_modes['text_only']:
        # Text mode: basic columns + document text + processing info
        required_columns = ['row_id', 'name', 'email', 'type', 'link', 'document_text', 'processed', 'extraction_date']
    else:
        # Full mode: all columns matching main system
        required_columns = [
            'row_id', 'name', 'email', 'type', 'link', 'extracted_links', 
            'youtube_playlist', 'google_drive', 'processed', 'document_text',
            'youtube_status', 'youtube_files', 'youtube_media_id',
            'drive_status', 'drive_files', 'drive_media_id',
            'last_download_attempt', 'download_errors', 'permanent_failure'
        ]
    
    # Ensure all required columns are present
    for col in required_columns:
        if col not in df.columns:
            df[col] = ''
    
    # Reorder columns to match requirements
    df = df[required_columns]
    
    # Save to CSV
    if processing_modes['basic_only']:
        output_file = config.get("paths.output_csv", "simple_output.csv")
    elif processing_modes['text_only']:
        output_file = "text_extraction_output.csv"
    else:
        output_file = config.get("paths.output_csv", "simple_output.csv")
    
    df.to_csv(output_file, index=False)
    
    print(f"✓ Data mapped and saved to {output_file}")
    print(f"  Total records: {len(df)}")
    print(f"  Records with links: {len(df[df['link'] != ''])}")
    
    # Additional stats only for full mode
    if not processing_modes['basic_only'] and not processing_modes['text_only']:
        print(f"  Records with YouTube: {len(df[df['youtube_playlist'] != ''])}")
        print(f"  Records with Drive: {len(df[df['google_drive'] != ''])}")
    
    # Text mode specific stats
    if processing_modes['text_only']:
        if 'document_text' in df.columns:
            successful_extractions = len(df[(df['document_text'] != '') & (~df['document_text'].str.startswith('EXTRACTION_FAILED', na=False))])
            failed_extractions = len(df[df['document_text'].str.startswith('EXTRACTION_FAILED', na=False)])
            print(f"  Successful text extractions: {successful_extractions}")
            print(f"  Failed text extractions: {failed_extractions}")
    
    return df

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Simple 6-Step Workflow - Unified Processing')
    
    # Processing mode options (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--basic', action='store_true', 
                           help='Extract only basic columns (row_id, name, email, type, link)')
    mode_group.add_argument('--text', action='store_true',
                           help='Extract basic columns + document text (batch processing)')
    
    # Processing options
    parser.add_argument('--test-limit', type=int, metavar='N',
                       help='Limit processing to N records for testing')
    parser.add_argument('--batch-size', type=int, metavar='N', default=10,
                       help='Process N documents per batch (default: 10)')
    parser.add_argument('--resume', action='store_true',
                       help='Resume from previous extraction progress')
    parser.add_argument('--retry-failed', action='store_true',
                       help='Retry previously failed extractions')
    parser.add_argument('--output', type=str, metavar='FILE',
                       help='Override output CSV filename')
    
    return parser.parse_args()

def main():
    """Run the complete 6-step workflow"""
    # Parse command line arguments
    args = parse_arguments()
    
    # Configure based on arguments (override config values)
    processing_modes = {
        'basic_only': args.basic,
        'text_only': args.text,
        'test_limit': args.test_limit,
        'batch_size': args.batch_size,
        'output_file': args.output if args.output else config.get("paths.output_csv", "simple_output.csv")
    }
    
    # Display configuration
    if processing_modes['basic_only']:
        mode = "BASIC MODE"
    elif processing_modes['text_only']:
        mode = f"TEXT EXTRACTION MODE (batch size: {processing_modes['batch_size']})"
    else:
        mode = "FULL MODE"
    
    limit_text = f" (limited to {processing_modes['test_limit']})" if processing_modes['test_limit'] else ""
    resume_text = " [RESUMING]" if args.resume else ""
    retry_text = " [RETRY FAILED]" if args.retry_failed else ""
    
    print(f"STARTING SIMPLE 6-STEP WORKFLOW - {mode}{limit_text}{resume_text}{retry_text}")
    print("=" * 80)
    
    processed_records = []
    
    # Step 1: Download sheet
    html_content = step1_download_sheet()
    
    # Step 2: Extract people data and Google Doc links
    all_people, people_with_docs = step2_extract_people_and_docs(html_content)
    
    print(f"\nProcessing ALL {len(all_people)} people...")
    print(f"  - {len(people_with_docs)} people have documents")
    print(f"  - {len(all_people) - len(people_with_docs)} people without documents")
    
    # Create a lookup for people with docs for efficient processing
    people_with_docs_dict = {person['row_id']: person for person in people_with_docs}
    
    # Determine processing approach based on mode
    if processing_modes['basic_only']:
        print(f"\n🚀 BASIC MODE: Processing {len(all_people)} people (basic data only)...")
        # Basic processing - just extract core data, no document processing
        for i, person in enumerate(all_people):
            if processing_modes['test_limit'] and i >= processing_modes['test_limit']:
                print(f"Test limit reached: {processing_modes['test_limit']}")
                break
            record = CSVManager.create_record(person, mode='basic')
            processed_records.append(record)
        print(f"✓ Processed {len(processed_records)} records in basic mode")
    
    elif processing_modes['text_only']:
        print(f"\n🚀 TEXT EXTRACTION MODE: Processing {len(people_with_docs)} documents...")
        
        # Load previous progress if resuming
        progress = load_json_state(config.get("paths.extraction_progress"), {"completed": [], "failed": [], "last_batch": 0, "total_processed": 0}) if args.resume else {"completed": [], "failed": [], "last_batch": 0, "total_processed": 0}
        failed_docs = load_json_state(config.get("paths.failed_extractions"), []) if args.retry_failed else []
        
        # Process all people first (add basic records for those without docs)
        for person in all_people:
            if not person.get('doc_link'):
                record = CSVManager.create_record(person, mode='text')
                processed_records.append(record)
        
        # Determine which documents to process
        if args.retry_failed and failed_docs:
            docs_to_process = [person for person in people_with_docs if person['doc_link'] in failed_docs]
            print(f"  Retrying {len(docs_to_process)} previously failed documents...")
        elif args.resume:
            docs_to_process = [person for person in people_with_docs if person['doc_link'] not in progress['completed']]
            print(f"  Resuming: {len(docs_to_process)} remaining documents...")
        else:
            docs_to_process = people_with_docs
            print(f"  Processing all {len(docs_to_process)} documents...")
        
        # Apply test limit if specified
        if processing_modes['test_limit']:
            docs_to_process = docs_to_process[:processing_modes['test_limit']]
            print(f"  Limited to {len(docs_to_process)} documents for testing")
        
        # Process documents in batches
        current_failed = []
        batch_start = progress.get('last_batch', 0) if args.resume else 0
        
        batch_size = processing_modes['batch_size']
        for i in range(batch_start, len(docs_to_process), batch_size):
            batch = docs_to_process[i:i + batch_size]
            batch_num = (i // batch_size) + 1
            total_batches = (len(docs_to_process) + batch_size - 1) // batch_size
            
            print(f"\n📦 BATCH {batch_num}/{total_batches} ({len(batch)} documents)")
            print("-" * 50)
            
            for j, person in enumerate(batch):
                doc_index = i + j + 1
                print(f"\n[{doc_index}/{len(docs_to_process)}] Processing: {person['name']}")
                print(f"  Document: {person['doc_link']}")
                
                # Extract text with retry logic
                doc_text, error = extract_text_with_retry(person['doc_link'])
                
                if error:
                    print(f"  ✗ Failed: {error}")
                    current_failed.append(person['doc_link'])
                    progress['failed'].append(person['doc_link'])
                    record = CSVManager.create_record(person, mode='text', doc_text=f"EXTRACTION_FAILED: {error}")
                else:
                    print(f"  ✓ Success: {len(doc_text)} characters extracted")
                    progress['completed'].append(person['doc_link'])
                    record = CSVManager.create_record(person, mode='text', doc_text=doc_text)
                
                processed_records.append(record)
                progress['total_processed'] += 1
                
                # Add delay between documents
                if j < len(batch) - 1:  # Don't delay after last document in batch
                    time.sleep(config.get("batch_processing.delay_between_docs", 2))
            
            # Save progress after each batch
            progress['last_batch'] = i + batch_size
            save_json_state(config.get("paths.extraction_progress"), progress)
            save_json_state(config.get("paths.failed_extractions"), current_failed)
            
            print(f"\n✓ Batch {batch_num} complete")
            batch_records = processed_records[-len(batch):]
            successful_in_batch = len([r for r in batch_records if not r.get('document_text', '').startswith('EXTRACTION_FAILED')])
            failed_in_batch = len(batch) - successful_in_batch
            print(f"  Successful: {successful_in_batch}")
            print(f"  Failed: {failed_in_batch}")
        
        print(f"\n🎉 TEXT EXTRACTION COMPLETE")
        print(f"  Total processed: {progress['total_processed']}")
        print(f"  Successful extractions: {len(progress['completed'])}")
        print(f"  Failed extractions: {len(current_failed)}")
    
    else:
        print(f"\n🚀 FULL MODE: Processing {len(all_people)} people (with document processing)...")
        # Full processing of all people (both with and without docs)
        people_to_process = all_people[:processing_modes['test_limit']] if processing_modes['test_limit'] else all_people
        
        for i, person in enumerate(people_to_process):
            print(f"\nProcessing person {i+1}/{len(people_to_process)}: {person['name']} (Row {person.get('row_id', 'Unknown')})")
            
            # Check if this person has a document
            if person.get('row_id') in people_with_docs_dict and person.get('doc_link'):
                print(f"  → Has document: {person['doc_link']}")
                
                # Step 3: Scrape doc content and text
                doc_content, doc_text = step3_scrape_doc_contents(person['doc_link'])
                
                # Step 4: Extract links from HTML content and document text
                links = step4_extract_links(doc_content, doc_text)
                
                # Step 5: Process extracted data
                record = step5_process_extracted_data(person, links, doc_text)
                processed_records.append(record)
            else:
                print(f"  → No document")
                # Create record for person without document
                record = {
                    'row_id': person.get('row_id', ''),
                    'name': person['name'],
                    'email': person['email'],
                    'type': person['type'],
                    'link': person.get('doc_link', ''),  # Include link even if empty
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
    cleanup_selenium_driver()
    
    print("\n" + "=" * 50)
    print("WORKFLOW COMPLETE")

if __name__ == "__main__":
    main()