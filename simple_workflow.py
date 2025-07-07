#!/usr/bin/env python3
"""
SIMPLE 6-STEP WORKFLOW - MINIMAL IMPLEMENTATION
Keep it simple. No over-engineering.
"""

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
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# Import centralized configuration, path utilities, error handling, patterns, and CSV operations (DRY)
from utils.config import get_config, ensure_parent_dir, ensure_directory, format_error_message, load_json_state, save_json_state
from utils.patterns import PatternRegistry, extract_youtube_id, extract_drive_id, clean_url, normalize_whitespace, get_chrome_options, wait_and_scroll_page, get_selenium_driver, cleanup_selenium_driver
from utils.extract_links import extract_google_doc_text, extract_actual_url, extract_text_with_retry
from utils.csv_manager import CSVManager
from utils.http_pool import get as http_get  # Centralized HTTP requests (DRY)

# Import database operations for dual-write functionality
from database.models.person import Person, PersonOperations

# Configuration - centralized in config.yaml (DRY)
config = get_config()

# CSV operations now use centralized CSVManager (DRY)

# Selenium driver functions now imported from patterns.py (DRY consolidation)

# Progress tracking functions (DRY: using centralized state management)
# Progress management functions removed - now using centralized load_json_state/save_json_state (DRY)

def load_failed_docs():
    """Load failed document extraction list"""
    return load_json_state(
        config.get("paths.failed_extractions", "failed_extractions.json"),
        []
    )

def save_failed_docs(failed_list):
    """Save failed document extraction list"""
    save_json_state(
        config.get("paths.failed_extractions", "failed_extractions.json"),
        failed_list
    )

# Selenium driver functions moved to patterns.py (DRY consolidation)

def step1_download_sheet():
    """Step 1: Download a local copy of the Google Sheet"""
    print("Step 1: Downloading Google Sheet...")
    
    response = http_get(config.get("google_sheets.url"))  # Use centralized HTTP pool (DRY)
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
            response = http_get(doc_url)  # Use centralized HTTP pool (DRY)
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
            response = http_get(doc_url)  # Use centralized HTTP pool (DRY)
            response.raise_for_status()
            print("✓ Doc scraped successfully (HTML only)")
            return response.text, ""
        except Exception as e:
            print(f"✗ Failed to scrape doc: {e}")
            return "", ""

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
    
    # Use centralized YouTube patterns (DRY)
    youtube_patterns = [
        PatternRegistry.YOUTUBE_VIDEO_FULL,
        PatternRegistry.YOUTUBE_SHORT_FULL,
        PatternRegistry.YOUTUBE_PLAYLIST_FULL
    ]
    
    for pattern in youtube_patterns:
        matches = pattern.findall(combined_content)
        for match in matches:
            if pattern == PatternRegistry.YOUTUBE_PLAYLIST_FULL:
                clean_link = f'https://www.youtube.com/playlist?list={match}'
            else:
                clean_link = f'https://www.youtube.com/watch?v={match}'
            
            if clean_link not in links['youtube']:
                links['youtube'].append(clean_link)
    
    # Use centralized Google Drive patterns (DRY)
    drive_patterns = [
        PatternRegistry.DRIVE_FILE_FULL,
        PatternRegistry.DRIVE_OPEN_FULL,
        PatternRegistry.DRIVE_FOLDER_FULL
    ]
    
    for pattern in drive_patterns:
        matches = pattern.findall(combined_content)
        for match in matches:
            if pattern == PatternRegistry.DRIVE_FOLDER_FULL:
                clean_link = f'https://drive.google.com/drive/folders/{match}'
                if clean_link not in links['drive_folders']:
                    links['drive_folders'].append(clean_link)
            else:
                clean_link = f'https://drive.google.com/file/d/{match}/view'
                if clean_link not in links['drive_files']:
                    links['drive_files'].append(clean_link)
    
    # Extract all HTTP(S) links for comprehensive coverage using centralized pattern (DRY)
    all_found_links = PatternRegistry.HTTP_URL.findall(combined_content)
    
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
    
    # Use centralized path utility (DRY)
    ensure_directory(config.get("paths.output_dir", "simple_downloads"))
    
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
    
    # Create record using centralized factory (DRY)
    # Prepare links data for factory
    links_data = {
        'youtube': meaningful_youtube,
        'drive_files': meaningful_drive_files,
        'drive_folders': meaningful_drive_folders,
        'all_links': links['all_links']
    }
    
    record = CSVManager.create_record(person, mode='full', doc_text=doc_text, links=links_data)
    
    print(f"✓ Processed record for {person['name']}")
    print(f"  Meaningful YouTube links: {len(meaningful_youtube)}")
    print(f"  Drive Files: {len(meaningful_drive_files)}")
    print(f"  Drive Folders: {len(meaningful_drive_folders)}")
    print(f"  Total links extracted: {len(links['all_links'])}")
    
    return record


# extract_text_with_retry function moved to utils/extract_links.py (DRY consolidation)

def step6_map_data(processed_records, basic_mode=False, text_mode=False, output_file=None):
    """Step 6: Map data to CSV and Database (dual-write for migration)"""
    print("Step 6: Mapping data to CSV and Database (dual-write)...")
    
    # Handle different column sets based on processing mode (DRY: use config)
    if basic_mode:
        # Basic mode: only 5 columns
        required_columns = config.get('csv_columns.basic')
    elif text_mode:
        # Text mode: basic columns + document text + processing info
        required_columns = config.get('csv_columns.text')
    else:
        # Full mode: all columns matching main system
        required_columns = config.get('csv_columns.full')
    
    # Ensure all required columns are present in records
    for record in processed_records:
        for col in required_columns:
            if col not in record:
                record[col] = ''
    
    # Filter records to only include required columns in correct order
    filtered_records = []
    for record in processed_records:
        filtered_record = {col: record.get(col, '') for col in required_columns}
        filtered_records.append(filtered_record)
    
    # Determine output file
    if not output_file:
        if basic_mode:
            output_file = config.get("paths.output_csv", "simple_output.csv")
        elif text_mode:
            output_file = "text_extraction_output.csv"
        else:
            output_file = config.get("paths.output_csv", "simple_output.csv")
    
    # Create DataFrame for CSV operations
    df = pd.DataFrame(filtered_records)
    
    # DUAL-WRITE: Write to both CSV and Database
    csv_success = False
    db_success = False
    
    # 1. Write to CSV (existing functionality)
    print("  📄 Writing to CSV...")
    csv_manager = CSVManager(csv_path=output_file)
    csv_success = csv_manager.safe_csv_write(df, operation_name="step6_workflow_output")
    
    if csv_success:
        print(f"  ✅ CSV: Data saved to {output_file}")
    else:
        print(f"  ❌ CSV: Failed to save data to {output_file}")
    
    # 2. Write to Database (new functionality)
    print("  🗄️  Writing to Database...")
    try:
        # Initialize database operations
        person_ops = PersonOperations()
        
        # Create tables if they don't exist
        if not person_ops.create_table():
            print("  ❌ DB: Failed to create/verify people table")
            db_success = False
        else:
            # Insert/update each person record
            db_insert_count = 0
            db_error_count = 0
            
            for record in filtered_records:
                # Create Person object with core fields
                person = Person(
                    row_id=record['row_id'],
                    name=record['name'],
                    email=record['email'] if record['email'] else None,
                    personality_type=record['type'] if record['type'] else None,
                    source_link=record['link'] if record['link'] else None
                )
                
                # Insert person (with ON CONFLICT UPDATE)
                if person_ops.insert_person(person):
                    db_insert_count += 1
                else:
                    db_error_count += 1
            
            if db_error_count == 0:
                print(f"  ✅ DB: Successfully inserted/updated {db_insert_count} people")
                db_success = True
            else:
                print(f"  ⚠️  DB: {db_insert_count} successful, {db_error_count} failed")
                db_success = False
    
    except Exception as e:
        print(f"  ❌ DB: Database write failed: {e}")
        db_success = False
    
    # Evaluate overall success
    if csv_success and db_success:
        print("  🎉 DUAL-WRITE SUCCESS: Both CSV and Database updated")
    elif csv_success:
        print("  ⚠️  PARTIAL SUCCESS: CSV updated, Database failed")
    elif db_success:
        print("  ⚠️  PARTIAL SUCCESS: Database updated, CSV failed")
    else:
        print("  ❌ DUAL-WRITE FAILED: Both CSV and Database failed")
        return None
    
    print(f"  📊 Total records: {len(df)}")
    print(f"  📊 Records with links: {len(df[df['link'] != ''])}")
    
    # Additional stats only for full mode
    if not basic_mode and not text_mode:
        print(f"  📊 Records with YouTube: {len(df[df['youtube_playlist'] != ''])}")
        print(f"  📊 Records with Drive: {len(df[df['google_drive'] != ''])}")
    
    # Text mode specific stats
    if text_mode:
        if 'document_text' in df.columns:
            successful_extractions = len(df[(df['document_text'] != '') & (~df['document_text'].str.startswith('EXTRACTION_FAILED', na=False))])
            failed_extractions = len(df[df['document_text'].str.startswith('EXTRACTION_FAILED', na=False)])
            print(f"  📊 Successful text extractions: {successful_extractions}")
            print(f"  📊 Failed text extractions: {failed_extractions}")
    
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
    
    # Configure based on arguments
    basic_mode = args.basic
    text_mode = args.text
    batch_size = args.batch_size
    test_limit = args.test_limit if args.test_limit else None
    output_file = args.output if args.output else config.get("paths.output_csv", "simple_output.csv")
    
    # Display configuration
    if basic_mode:
        mode = "BASIC MODE"
    elif text_mode:
        mode = f"TEXT EXTRACTION MODE (batch size: {batch_size})"
    else:
        mode = "FULL MODE"
    
    limit_text = f" (limited to {test_limit})" if test_limit else ""
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
    if basic_mode:
        print(f"\n🚀 BASIC MODE: Processing {len(all_people)} people (basic data only)...")
        # Basic processing - just extract core data, no document processing
        for i, person in enumerate(all_people):
            if test_limit and i >= test_limit:
                print(f"Test limit reached: {test_limit}")
                break
            record = CSVManager.create_record(person, mode='basic')
            processed_records.append(record)
        print(f"✓ Processed {len(processed_records)} records in basic mode")
    
    elif text_mode:
        print(f"\n🚀 TEXT EXTRACTION MODE: Processing {len(people_with_docs)} documents...")
        
        # Load previous progress if resuming
        # Load progress using centralized state management (DRY)
        default_progress = {"completed": [], "failed": [], "last_batch": 0, "total_processed": 0}
        progress = load_json_state(config.get("paths.extraction_progress", "extraction_progress.json"), default_progress) if args.resume else default_progress
        failed_docs = load_failed_docs() if args.retry_failed else []
        
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
        if test_limit:
            docs_to_process = docs_to_process[:test_limit]
            print(f"  Limited to {len(docs_to_process)} documents for testing")
        
        # Process documents in batches
        current_failed = []
        batch_start = progress.get('last_batch', 0) if args.resume else 0
        
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
                    record = CSVManager.create_error_record(person, mode='text', error_message=error)
                else:
                    print(f"  ✓ Success: {len(doc_text)} characters extracted")
                    progress['completed'].append(person['doc_link'])
                    record = CSVManager.create_record(person, mode='text', doc_text=doc_text)
                
                processed_records.append(record)
                progress['total_processed'] += 1
                
                # Add delay between documents
                if j < len(batch) - 1:  # Don't delay after last document in batch
                    delay = config.get("retry.base_delay", 2.0)
                    time.sleep(delay)
            
            # Save progress after each batch
            progress['last_batch'] = i + batch_size
            # Save progress using centralized state management (DRY)
            save_json_state(config.get("paths.extraction_progress", "extraction_progress.json"), progress)
            save_failed_docs(current_failed)
            
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
        people_to_process = all_people[:test_limit] if test_limit else all_people
        
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
                # Create record for person without document using factory (DRY)
                record = CSVManager.create_record(person, mode='full', doc_text='', links=None)
                processed_records.append(record)
    
    # Step 6: Map all data
    if processed_records:
        step6_map_data(processed_records, basic_mode=basic_mode, text_mode=text_mode, output_file=output_file)
    else:
        print("No records to map")
    
    # Cleanup Selenium driver
    cleanup_selenium_driver()
    
    print("\n" + "=" * 50)
    print("WORKFLOW COMPLETE")

if __name__ == "__main__":
    main()