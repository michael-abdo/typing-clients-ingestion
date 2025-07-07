#!/usr/bin/env python3
"""
SIMPLE 6-STEP WORKFLOW - MINIMAL IMPLEMENTATION
Keep it simple. No over-engineering.
"""

import pandas as pd
import argparse
import sys
import json
from datetime import datetime
from pathlib import Path
import os
import time
from extraction_utils import (
    get_selenium_driver, cleanup_driver, extract_actual_url, clean_url,
    download_google_sheet, extract_google_doc_content_and_links,
    extract_people_from_sheet_html, extract_text_with_retry
)

# Graceful handling for database dependencies (DRY: unified approach)
import sys
import os

# Import centralized configuration
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from utils.config import (
    get_google_sheets_url, get_target_div_id, get_database_path, 
    get_output_csv_path, get_batch_size, get_max_retries,
    get_progress_file, get_failed_docs_file, StandardCLIArguments
)
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config import bulk_safe_import

try:
    imports = bulk_safe_import([
        ('database_manager', 'DatabaseManager'),
        ('database_manager', 'Person'),
        ('database_manager', 'Document'),
        ('database_manager', 'ExtractedLink')
    ])
    DatabaseManager = imports['DatabaseManager']
    Person = imports['Person']
    Document = imports['Document']
    ExtractedLink = imports['ExtractedLink']
    DATABASE_AVAILABLE = True
except ImportError:
    DATABASE_AVAILABLE = False
    print("Warning: Database modules not available. Database mode disabled.")
    
    # Create dummy classes for graceful degradation
    class DatabaseManager:
        def __init__(self, *args, **kwargs): pass
        def connect(self): return False
        def disconnect(self): pass
    
    class Person: pass
    class Document: pass
    class ExtractedLink: pass
# Configuration - centralized settings (DRY: absorbed database config)
class Config:
    GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/u/1/d/e/2PACX-1vRqqjqoaj8sEZBfZRw0Og7g8ms_0yTL2MsegTubcjhhBnXr1s1jFBwIVAsbkyj1xD0TMj06LvGTQIHU/pubhtml?pli=1#"
    TARGET_DIV_ID = "1159146182"
    OUTPUT_DIR = Path("simple_downloads")
    OUTPUT_CSV_FULL = "simple_output.csv"
    OUTPUT_CSV_BASIC = "simple_output.csv"  # Unified output file
    
    # Database configuration (DRY: moved from simple_workflow_db.py)
    DATABASE_PATH = "xenodx.db"
    USE_DATABASE = False  # Default to CSV mode for backward compatibility
    OUTPUT_FORMAT = "csv"  # Options: "csv", "db", "both"
    
    # Processing modes
    BASIC_ONLY = False  # True = extract only basic columns, False = full processing
    TEXT_ONLY = False   # True = extract only text (basic + document_text), False = full processing
    TEST_LIMIT = None   # None = process all, int = limit for testing
    
    # Batch processing settings
    BATCH_SIZE = 10     # Process N documents at a time
    RETRY_ATTEMPTS = 3  # Retry failed extractions N times
    DELAY_BETWEEN_DOCS = 2  # Seconds between document extractions
    
    # Row range filtering
    START_ROW = None    # Start from row N (inclusive)
    END_ROW = None      # End at row N (inclusive)
    
    # Incremental processing (DRY: moved from simple_workflow_db.py)
    SKIP_PROCESSED = True
    RETRY_FAILED = False
    
    # Progress tracking
    PROGRESS_FILE = "extraction_progress.json"
    FAILED_DOCS_FILE = "failed_extractions.json"


# Enhanced progress tracking functions
class ProgressTracker:
    """Centralized progress tracking for all modes"""
    def __init__(self, progress_file=None, failed_file=None):
        self.progress_file = progress_file or Config.PROGRESS_FILE
        self.failed_file = failed_file or Config.FAILED_DOCS_FILE
        self.progress = self.load_progress()
        self.failed_docs = self.load_failed_docs()
        self.start_time = datetime.now()
    
    def load_progress(self):
        """Load extraction progress from file"""
        if os.path.exists(self.progress_file):
            try:
                with open(self.progress_file, 'r') as f:
                    return json.load(f)
            except:
                pass
        return {
            "completed": [],
            "failed": [],
            "last_batch": 0,
            "total_processed": 0,
            "mode": None,
            "last_run": None
        }
    
    def save_progress(self):
        """Save extraction progress to file"""
        self.progress["last_run"] = datetime.now().isoformat()
        with open(self.progress_file, 'w') as f:
            json.dump(self.progress, f, indent=2)
    
    def load_failed_docs(self):
        """Load failed document extraction list"""
        if os.path.exists(self.failed_file):
            try:
                with open(self.failed_file, 'r') as f:
                    return json.load(f)
            except:
                pass
        return []
    
    def save_failed_docs(self):
        """Save failed document extraction list"""
        with open(self.failed_file, 'w') as f:
            json.dump(self.failed_docs, f, indent=2)
    
    def mark_completed(self, doc_url):
        """Mark a document as completed"""
        if doc_url not in self.progress["completed"]:
            self.progress["completed"].append(doc_url)
        if doc_url in self.progress["failed"]:
            self.progress["failed"].remove(doc_url)
        self.progress["total_processed"] += 1
    
    def mark_failed(self, doc_url, error=None):
        """Mark a document as failed"""
        if doc_url not in self.progress["failed"]:
            self.progress["failed"].append(doc_url)
        if doc_url not in self.failed_docs:
            self.failed_docs.append(doc_url)
        self.progress["total_processed"] += 1
    
    def should_process(self, doc_url, skip_processed=True):
        """Check if a document should be processed"""
        if not skip_processed:
            return True
        return doc_url not in self.progress["completed"]
    
    def get_stats(self):
        """Get current progress statistics"""
        elapsed = (datetime.now() - self.start_time).total_seconds()
        rate = self.progress["total_processed"] / elapsed if elapsed > 0 else 0
        
        return {
            "completed": len(self.progress["completed"]),
            "failed": len(self.progress["failed"]),
            "total": self.progress["total_processed"],
            "elapsed_seconds": elapsed,
            "rate_per_minute": rate * 60
        }
    
    def display_progress(self, current, total, message=""):
        """Display progress bar and statistics"""
        percentage = (current / total * 100) if total > 0 else 0
        bar_length = 40
        filled_length = int(bar_length * current // total) if total > 0 else 0
        bar = '█' * filled_length + '░' * (bar_length - filled_length)
        
        stats = self.get_stats()
        eta = ((total - current) / stats["rate_per_minute"] if stats["rate_per_minute"] > 0 else 0)
        
        print(f"\r[{bar}] {percentage:.1f}% ({current}/{total}) {message}", end="")
        if current == total:
            print()  # New line at completion





def filter_people_by_row_range(people_data):
    """Filter people data by row range if specified"""
    if not Config.START_ROW and not Config.END_ROW:
        return people_data
    
    filtered_people = []
    for person in people_data:
        try:
            row_id = int(person.get('row_id', 0))
            if Config.START_ROW and row_id < Config.START_ROW:
                continue
            if Config.END_ROW and row_id > Config.END_ROW:
                continue
            filtered_people.append(person)
        except (ValueError, TypeError):
            # Skip rows with invalid row_id
            continue
    
    if Config.START_ROW or Config.END_ROW:
        start_text = f"row {Config.START_ROW}" if Config.START_ROW else "beginning"
        end_text = f"row {Config.END_ROW}" if Config.END_ROW else "end"
        print(f"✓ Filtered to rows {start_text} to {end_text}: {len(filtered_people)} people")
    
    return filtered_people

def extract_and_filter_people(html_content):
    """Extract people data and filter those with documents"""
    print("Step 2: Extracting people data and Google Doc links...")
    
    # Use DRY: extraction_utils function
    people_data = extract_people_from_sheet_html(html_content)
    print(f"✓ Found {len(people_data)} people records")
    
    # Filter to only those with Google Doc links
    people_with_docs = [person for person in people_data if person.get("doc_link")]
    print(f"✓ Found {len(people_with_docs)} people with Google Doc links")
    
    return people_data, people_with_docs




def create_full_record(person, links, doc_text=""):
    """Create a full record with all extracted data for CSV"""
    # Categorize YouTube links
    youtube_playlists = [link for link in links.get('youtube', []) if 'playlist' in link]
    youtube_videos = [link for link in links.get('youtube', []) if 'watch' in link or 'youtu.be' in link]
    all_youtube = list(set(youtube_playlists + youtube_videos))
    
    # Combine Drive links
    all_drive = links.get('drive_files', []) + links.get('drive_folders', [])
    
    # Create record matching the main system's CSV structure
    record = {
        'row_id': person.get('row_id', ''),
        'name': person['name'],
        'email': person['email'],
        'type': person['type'],
        'link': person.get('doc_link', ''),
        'extracted_links': '|'.join(links.get('all_links', [])),
        'youtube_playlist': '|'.join(all_youtube),
        'google_drive': '|'.join(all_drive),
        'processed': 'yes',
        'document_text': doc_text[:10000],  # Limit text length
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
    
    return record

def create_record(person, mode="basic", doc_text="", links=None):
    """Create a record based on mode (DRY principle)"""
    # Base record - always included
    record = {
        'row_id': person.get('row_id', ''),
        'name': person['name'],
        'email': person['email'],
        'type': person['type'],
        'link': person.get('doc_link', '')
    }
    
    # Add fields based on mode
    if mode == "text":
        record.update({
            'document_text': doc_text,
            'processed': 'yes' if doc_text and not doc_text.startswith('[') else 'no',
            'extraction_date': datetime.now().isoformat()
        })
    elif mode == "full":
        # Full mode with all fields
        if links:
            record = create_full_record(person, links, doc_text)
        else:
            # No links - create empty full record
            record.update({
                'extracted_links': '',
                'youtube_playlist': '',
                'google_drive': '',
                'processed': 'yes',
                'document_text': doc_text[:10000],
                'youtube_status': '',
                'youtube_files': '',
                'youtube_media_id': '',
                'drive_status': '',
                'drive_files': '',
                'drive_media_id': '',
                'last_download_attempt': '',
                'download_errors': '',
                'permanent_failure': ''
            })
    
    return record


def step6_map_data(processed_records):
    """Step 6: Map data to CSV and/or database based on configuration"""
    print("Step 6: Mapping data for output...")
    
    # Create DataFrame with proper column order matching main system
    df = pd.DataFrame(processed_records)
    
    # DRY: Handle database output if enabled
    if Config.USE_DATABASE and DATABASE_AVAILABLE:
        print("  → Saving to database...")
        db_manager = DatabaseManager(Config.DATABASE_PATH)
        db_manager.connect()
        
        # Use database-specific processing for the records
        if Config.OUTPUT_FORMAT in ["db", "both"]:
            try:
                # Process records through database
                for record in processed_records:
                    # Insert person if not exists
                    person = Person(
                        row_id=record.get('row_id', ''),
                        name=record.get('name', ''),
                        email=record.get('email', ''),
                        type=record.get('type', '')
                    )
                    db_manager.insert_person(person)
                
                print(f"  ✓ Database saved: {len(processed_records)} records")
                
                # Export to CSV if requested
                if Config.OUTPUT_FORMAT == "both":
                    mode = "basic" if Config.BASIC_ONLY else "full"
                    export_database_to_csv(db_manager, mode)
                    
            except Exception as e:
                print(f"  ✗ Database error: {e}")
                print("  → Falling back to CSV only")
            finally:
                db_manager.disconnect()
    
    # Always handle CSV output (either primary or fallback)  
    if not Config.USE_DATABASE or Config.OUTPUT_FORMAT in ["csv", "both"]:
        print("  → Saving to CSV...")
        
        # Handle different column sets based on processing mode
        if Config.BASIC_ONLY:
            # Basic mode: only 5 columns
            required_columns = ['row_id', 'name', 'email', 'type', 'link']
        elif Config.TEXT_ONLY:
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
    if Config.BASIC_ONLY:
        output_file = Config.OUTPUT_CSV_BASIC
    elif Config.TEXT_ONLY:
        output_file = "text_extraction_output.csv"
    else:
        output_file = Config.OUTPUT_CSV_FULL
    
    df.to_csv(output_file, index=False)
    
    print(f"✓ Data mapped and saved to {output_file}")
    print(f"  Total records: {len(df)}")
    print(f"  Records with links: {len(df[df['link'] != ''])}")
    
    # Additional stats only for full mode
    if not Config.BASIC_ONLY and not Config.TEXT_ONLY:
        print(f"  Records with YouTube: {len(df[df['youtube_playlist'] != ''])}")
        print(f"  Records with Drive: {len(df[df['google_drive'] != ''])}")
    
    # Text mode specific stats
    if Config.TEXT_ONLY:
        if 'document_text' in df.columns:
            successful_extractions = len(df[(df['document_text'] != '') & (~df['document_text'].str.startswith('EXTRACTION_FAILED', na=False))])
            failed_extractions = len(df[df['document_text'].str.startswith('EXTRACTION_FAILED', na=False)])
            print(f"  Successful text extractions: {successful_extractions}")
            print(f"  Failed text extractions: {failed_extractions}")
    
    return df

def parse_arguments():
    """Parse command line arguments using StandardCLIArguments (DRY)"""
    parser = argparse.ArgumentParser(description='Simple 6-Step Workflow - Unified Processing')
    
    # Processing mode options (mutually exclusive)
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--basic', action='store_true', 
                           help='Extract only basic columns (row_id, name, email, type, link)')
    mode_group.add_argument('--text', action='store_true',
                           help='Extract basic columns + document text (batch processing)')
    mode_group.add_argument('--test', action='store_true',
                           help='Run validation tests on extraction system')
    mode_group.add_argument('--validate', action='store_true',
                           help='Validate extraction against known test cases')
    
    # Use StandardCLIArguments for common patterns
    StandardCLIArguments.add_processing_arguments(parser, 
                                                include_max_rows=True,
                                                include_batch_size=True,
                                                include_reset=True,
                                                include_dry_run=False)
    
    StandardCLIArguments.add_file_arguments(parser,
                                          include_csv=False,  # We use --output instead
                                          include_output=True,
                                          include_directory=False)
    
    # Additional processing options not covered by standard arguments
    parser.add_argument('--test-limit', type=int, metavar='N',
                       help='Limit processing to N records for testing')
    parser.add_argument('--resume', action='store_true',
                       help='Resume from previous extraction progress')
    parser.add_argument("--retry-failed", action="store_true",
                       help="Retry previously failed extractions")
    
    # Database options (workflow specific)
    db_group = parser.add_argument_group('database options')
    db_group.add_argument("--db", action="store_true", default=False,
                         help="Use database mode")
    db_group.add_argument("--no-db", dest="db", action="store_false",
                         help="Disable database, use CSV only")
    db_group.add_argument("--db-path", type=str, default=get_database_path(),
                         help=f"Database file path (default: {get_database_path()})")
    
    # Output options (workflow specific)
    output_group = parser.add_argument_group('output options')
    output_group.add_argument("--output-format", choices=["csv", "db", "both"],
                            default="csv", help="Output format (default: csv)")
    output_group.add_argument("--skip-processed", action="store_true", default=True,
                            help="Skip already processed documents (default)")
    output_group.add_argument("--reprocess-all", dest="skip_processed", action="store_false",
                            help="Reprocess all documents")
    
    # Row range options
    range_group = parser.add_argument_group('row range options')
    range_group.add_argument("--start-row", type=int, metavar="N",
                           help="Start processing from row N (inclusive)")
    range_group.add_argument("--end-row", type=int, metavar="N",
                           help="End processing at row N (inclusive)")
    range_group.add_argument("--find-person", type=str, metavar="NAME",
                           help="Find and process specific person by name")
    
    return parser.parse_args()

# DRY: Database-specific functions absorbed from simple_workflow_db.py
def process_people_database_mode(html_content, db_manager):
    """Database-specific people processing with transactions"""
    people_data = extract_people_from_sheet_html(html_content, Config.TARGET_DIV_ID)
    
    with db_manager:
        for person in people_data:
            person_obj = Person(
                row_id=person['row_id'],
                name=person['name'],
                email=person['email'],
                type=person['type'],
                doc_link=person.get('doc_link', '')
            )
            db_manager.insert_person(person_obj)
            db_manager.log_processing(person_obj.id, "person_imported", "completed")
    
    return people_data

def process_documents_database_batch(db_manager, docs_to_process, mode="full"):
    """Database batch processing with transaction safety"""
    with db_manager:
        for person in docs_to_process:
            try:
                # Extract document content
                doc_text, error = extract_text_with_retry(person["doc_link"], Config.RETRY_ATTEMPTS)
                
                # Create document record
                document = Document(
                    person_id=person['row_id'],
                    url=person['doc_link'],
                    document_text=doc_text if not error else f"EXTRACTION_FAILED: {error}",
                    processed=not bool(error),
                    extraction_date=datetime.now().isoformat()
                )
                doc_id = db_manager.insert_document(document)
                
                # Extract and insert links if successful
                if not error and mode == "full":
                    doc_content, _ = scrape_document_content(person['doc_link'])
                    links = extract_links_from_content(doc_content, doc_text)
                    db_manager.insert_links_batch(doc_id, links)
                
                db_manager.log_processing(person['row_id'], "document_processed", "completed")
                
            except Exception as e:
                db_manager.log_processing(person['row_id'], "document_processed", "failed", str(e))
                db_manager.rollback()

def export_database_to_csv(db_manager, mode="full", output_file=None):
    """Export database data to CSV format"""
    data = db_manager.export_to_dataframe(mode)
    
    if not output_file:
        output_file = Config.OUTPUT_CSV_BASIC if mode == "basic" else Config.OUTPUT_CSV_FULL
    
    df = pd.DataFrame(data)
    df.to_csv(output_file, index=False)
    
    print(f"✓ Database exported to {output_file}")
    print(f"  Total records: {len(df)}")
    
    return df

def run_validation_tests():
    """Run comprehensive validation tests"""
    print("🧪 RUNNING VALIDATION TESTS")
    print("=" * 80)
    
    # Test key people from user's verification data
    test_cases = [
        ("James Kirton", "YouTube playlists"),
        ("John Williams", "Multiple videos"),
        ("Olivia Tomlinson", "7 videos"),
        ("Carlos Arthur", "YouTube video")
    ]
    
    # Download sheet and extract people
    html_content = download_google_sheet(Config.GOOGLE_SHEET_URL)
    people_data = extract_people_from_sheet_html(html_content)
    
    results = []
    for name, expected in test_cases:
        print(f"\nTesting: {name} (expected: {expected})")
        
        # Find person
        person = None
        for p in people_data:
            if name.lower() in p['name'].lower():
                person = p
                break
        
        if not person:
            print(f"  ❌ Person not found")
            results.append(False)
            continue
            
        print(f"  ✓ Found: {person['name']} at Row {person['row_id']}")
        
        if not person.get('doc_link'):
            print(f"  ⚠️  No document link")
            results.append(None)
            continue
            
        try:
            # Extract content
            doc_text, _, links = extract_google_doc_content_and_links(person['doc_link'])
            
            if doc_text == "[AUTHENTICATION_REQUIRED]":
                print(f"  ⚠️  Authentication required")
                results.append(None)
            else:
                youtube_count = len(links.get('youtube', []))
                playlist_count = sum(1 for link in links.get('youtube', []) if 'playlist' in link)
                
                print(f"  ✓ Extracted: {len(doc_text)} chars, {youtube_count} YouTube links ({playlist_count} playlists)")
                
                # Basic validation
                if "playlist" in expected and playlist_count > 0:
                    results.append(True)
                elif "video" in expected and youtube_count > 0:
                    results.append(True)
                else:
                    results.append(False)
                    
        except Exception as e:
            print(f"  ❌ Error: {e}")
            results.append(False)
    
    # Summary
    passed = sum(1 for r in results if r is True)
    total = len([r for r in results if r is not None])
    
    print(f"\n📊 VALIDATION SUMMARY: {passed}/{total} tests passed")
    return passed == total

def find_person_by_name(people_data, search_name):
    """Find person by name (case insensitive partial match)"""
    search_lower = search_name.lower()
    matches = []
    
    for person in people_data:
        if search_lower in person['name'].lower():
            matches.append(person)
    
    return matches

def main():
    """Run the complete 6-step workflow with comprehensive error handling"""
    try:
        # Parse command line arguments
        args = parse_arguments()
    except Exception as e:
        print(f"Error parsing arguments: {e}")
        sys.exit(1)
    
    # Handle test mode
    if args.test:
        success = run_validation_tests()
        cleanup_driver()
        sys.exit(0 if success else 1)
    
    # Handle validation mode (same as test for now, can be extended)
    if args.validate:
        success = run_validation_tests()
        cleanup_driver()
        sys.exit(0 if success else 1)
    
    # Configure based on arguments
    Config.BASIC_ONLY = args.basic
    Config.TEXT_ONLY = args.text
    Config.BATCH_SIZE = args.batch_size
    
    if args.test_limit:
        Config.TEST_LIMIT = args.test_limit
    if args.output:
        Config.OUTPUT_CSV_FULL = args.output
        Config.OUTPUT_CSV_BASIC = args.output
    
    # Database configuration (DRY: unified from simple_workflow_db.py)
    Config.USE_DATABASE = args.db
    Config.DATABASE_PATH = args.db_path
    Config.OUTPUT_FORMAT = args.output_format
    Config.SKIP_PROCESSED = args.skip_processed
    Config.RETRY_FAILED = args.retry_failed
    Config.START_ROW = args.start_row
    Config.END_ROW = args.end_row
    
    
    # Validate database dependencies
    if Config.USE_DATABASE and not DATABASE_AVAILABLE:
        print("Error: Database mode requested but database modules not available.")
        print("Please install required dependencies or use CSV mode (--no-db)")
        sys.exit(1)
    # Display configuration
    if Config.BASIC_ONLY:
        mode = "BASIC MODE"
    elif Config.TEXT_ONLY:
        mode = f"TEXT EXTRACTION MODE (batch size: {Config.BATCH_SIZE})"
    else:
        mode = "FULL MODE"
    
    limit_text = f" (limited to {Config.TEST_LIMIT})" if Config.TEST_LIMIT else ""
    resume_text = " [RESUMING]" if args.resume else ""
    retry_text = " [RETRY FAILED]" if args.retry_failed else ""
    db_text = f" [DB: {Config.DATABASE_PATH}]" if Config.USE_DATABASE else " [CSV ONLY]"
    
    # Row range text
    row_range_text = ""
    if Config.START_ROW or Config.END_ROW:
        start_text = f"row {Config.START_ROW}" if Config.START_ROW else "start"
        end_text = f"row {Config.END_ROW}" if Config.END_ROW else "end"
        row_range_text = f" [ROWS {start_text}-{end_text}]"
    
    print(f"STARTING SIMPLE 6-STEP WORKFLOW - {mode}{db_text}{limit_text}{resume_text}{retry_text}{row_range_text}")
    print("=" * 80)
    
    processed_records = []
    
    # Step 1: Download sheet with error handling
    try:
        html_content = download_google_sheet(Config.GOOGLE_SHEET_URL)
    except Exception as e:
        print(f"\n❌ ERROR: Failed to download Google Sheet: {e}")
        sys.exit(1)
    
    # Step 2: Extract people data with error handling
    try:
        all_people, people_with_docs = extract_and_filter_people(html_content)
    except Exception as e:
        print(f"\n❌ ERROR: Failed to extract people data: {e}")
        sys.exit(1)
    
    # Handle find-person mode
    if args.find_person:
        matches = find_person_by_name(all_people, args.find_person)
        if not matches:
            print(f"No person found matching '{args.find_person}'")
            sys.exit(1)
        
        print(f"Found {len(matches)} matches for '{args.find_person}':")
        for person in matches:
            print(f"  Row {person['row_id']}: {person['name']} ({person['email']})")
            if person.get('doc_link'):
                print(f"    Document: {person['doc_link'][:60]}...")
        
        # If single match, process just that person
        if len(matches) == 1:
            print(f"\nProcessing {matches[0]['name']}...")
            Config.START_ROW = int(matches[0]['row_id'])
            Config.END_ROW = int(matches[0]['row_id'])
        else:
            sys.exit(0)
    
    # Apply row range filtering if specified
    all_people = filter_people_by_row_range(all_people)
    people_with_docs = filter_people_by_row_range(people_with_docs)
    
    print(f"\nProcessing {len(all_people)} people...")
    print(f"  - {len(people_with_docs)} people have documents")
    print(f"  - {len(all_people) - len(people_with_docs)} people without documents")
    
    # Create a lookup for people with docs for efficient processing
    people_with_docs_dict = {person['row_id']: person for person in people_with_docs}
    
    # Determine processing approach based on mode
    if Config.BASIC_ONLY:
        print(f"\n🚀 BASIC MODE: Processing {len(all_people)} people (basic data only)...")
        # Basic processing - just extract core data, no document processing
        for i, person in enumerate(all_people):
            if Config.TEST_LIMIT and i >= Config.TEST_LIMIT:
                print(f"Test limit reached: {Config.TEST_LIMIT}")
                break
            record = create_record(person, mode="basic")
            processed_records.append(record)
        print(f"✓ Processed {len(processed_records)} records in basic mode")
    
    elif Config.TEXT_ONLY:
        print(f"\n🚀 TEXT EXTRACTION MODE: Processing {len(people_with_docs)} documents...")
        
        # Initialize progress tracker
        tracker = ProgressTracker()
        if not args.resume:
            # Reset progress if not resuming
            tracker.progress = {"completed": [], "failed": [], "last_batch": 0, "total_processed": 0, "mode": "text"}
        tracker.progress["mode"] = "text"
        
        # Process all people first (add basic records for those without docs)
        for person in all_people:
            if not person.get('doc_link'):
                record = create_record(person, mode="text")
                processed_records.append(record)
        
        # Determine which documents to process
        if args.retry_failed and tracker.failed_docs:
            docs_to_process = [person for person in people_with_docs if person['doc_link'] in tracker.failed_docs]
            print(f"  Retrying {len(docs_to_process)} previously failed documents...")
        elif args.resume:
            docs_to_process = [person for person in people_with_docs if tracker.should_process(person['doc_link'])]
            print(f"  Resuming: {len(docs_to_process)} remaining documents...")
        else:
            docs_to_process = people_with_docs
            print(f"  Processing all {len(docs_to_process)} documents...")
        
        # Apply test limit if specified
        if Config.TEST_LIMIT:
            docs_to_process = docs_to_process[:Config.TEST_LIMIT]
            print(f"  Limited to {len(docs_to_process)} documents for testing")
        
        # Process documents in batches
        batch_start = tracker.progress.get('last_batch', 0) if args.resume else 0
        
        for i in range(batch_start, len(docs_to_process), Config.BATCH_SIZE):
            batch = docs_to_process[i:i + Config.BATCH_SIZE]
            batch_num = (i // Config.BATCH_SIZE) + 1
            total_batches = (len(docs_to_process) + Config.BATCH_SIZE - 1) // Config.BATCH_SIZE
            
            print(f"\n📦 BATCH {batch_num}/{total_batches} ({len(batch)} documents)")
            print("-" * 50)
            
            for j, person in enumerate(batch):
                doc_index = i + j + 1
                print(f"\n[{doc_index}/{len(docs_to_process)}] Processing: {person['name']}")
                print(f"  Document: {person['doc_link']}")
                
                try:
                    # DRY: Use enhanced extraction with retry capability
                    attempts = 0
                    max_attempts = Config.RETRY_ATTEMPTS
                    doc_text = None
                    
                    while attempts < max_attempts:
                        try:
                            doc_text, _, _ = extract_google_doc_content_and_links(person["doc_link"])
                            break  # Success, exit retry loop
                        except Exception as e:
                            attempts += 1
                            if attempts < max_attempts:
                                print(f"    Retry {attempts}/{max_attempts} after error: {e}")
                                time.sleep(Config.DELAY_BETWEEN_DOCS * attempts)  # Exponential backoff
                            else:
                                raise e
                    
                    if doc_text == "[AUTHENTICATION_REQUIRED]":
                        print(f"  ⚠️  Authentication required")
                        tracker.mark_failed(person['doc_link'], "Authentication required")
                        record = create_record(person, mode="text", doc_text=doc_text)
                    elif not doc_text or doc_text.startswith("[ERROR"):
                        print(f"  ✗ Failed: Extraction error")
                        tracker.mark_failed(person['doc_link'], "Extraction error")
                        record = create_record(person, mode="text", doc_text=f"[EXTRACTION_FAILED]")
                    else:
                        print(f"  ✓ Success: {len(doc_text)} characters extracted")
                        tracker.mark_completed(person['doc_link'])
                        record = create_record(person, mode="text", doc_text=doc_text)
                        
                except Exception as e:
                    print(f"  ✗ Failed after {max_attempts} attempts: {e}")
                    tracker.mark_failed(person['doc_link'], str(e))
                    record = create_record(person, mode="text", doc_text=f"[EXTRACTION_FAILED] {str(e)}")
                
                processed_records.append(record)
                
                # Display progress
                tracker.display_progress(doc_index, len(docs_to_process), person['name'])
                
                # Add delay between documents
                if j < len(batch) - 1:  # Don't delay after last document in batch
                    time.sleep(Config.DELAY_BETWEEN_DOCS)
            
            # Save progress after each batch
            tracker.progress['last_batch'] = i + Config.BATCH_SIZE
            tracker.save_progress()
            tracker.save_failed_docs()
            
            print(f"\n✓ Batch {batch_num} complete")
            batch_records = processed_records[-len(batch):]
            successful_in_batch = len([r for r in batch_records if not r.get('document_text', '').startswith('EXTRACTION_FAILED')])
            failed_in_batch = len(batch) - successful_in_batch
            print(f"  Successful: {successful_in_batch}")
            print(f"  Failed: {failed_in_batch}")
        
        # Final statistics
        stats = tracker.get_stats()
        print(f"\n🎉 TEXT EXTRACTION COMPLETE")
        print(f"  Total processed: {stats['total']}")
        print(f"  Successful extractions: {stats['completed']}")
        print(f"  Failed extractions: {stats['failed']}")
        print(f"  Time elapsed: {stats['elapsed_seconds']/60:.1f} minutes")
        print(f"  Average rate: {stats['rate_per_minute']:.1f} docs/minute")
    
    else:
        print(f"\n🚀 FULL MODE: Processing {len(all_people)} people (with document processing)...")
        # Full processing of all people (both with and without docs)
        people_to_process = all_people[:Config.TEST_LIMIT] if Config.TEST_LIMIT else all_people
        
        for i, person in enumerate(people_to_process):
            print(f"\nProcessing person {i+1}/{len(people_to_process)}: {person['name']} (Row {person.get('row_id', 'Unknown')})")
            
            # Check if this person has a document
            if person.get('row_id') in people_with_docs_dict and person.get('doc_link'):
                print(f"  → Has document: {person['doc_link']}")
                
                try:
                    # DRY: Use enhanced extraction function that handles everything
                    doc_text, html_content, links = extract_google_doc_content_and_links(person['doc_link'])
                    
                    # Handle authentication required
                    if doc_text == "[AUTHENTICATION_REQUIRED]":
                        print(f"  ⚠️  Authentication required")
                        doc_text = "[AUTHENTICATION_REQUIRED]"
                        links = {'youtube': [], 'drive_files': [], 'drive_folders': [], 'all_links': []}
                    elif not doc_text:
                        print(f"  ⚠️  No content extracted")
                        doc_text = "[NO_CONTENT]"
                        links = {'youtube': [], 'drive_files': [], 'drive_folders': [], 'all_links': []}
                    else:
                        print(f"  ✓ Extracted {len(doc_text)} characters")
                        print(f"  ✓ Found {sum(len(v) for v in links.values() if isinstance(v, list))} links")
                    
                    # Create full record with all data
                    record = create_record(person, mode="full", doc_text=doc_text, links=links)
                    processed_records.append(record)
                    
                except Exception as e:
                    print(f"  ✗ Error extracting document: {e}")
                    # Create error record
                    record = create_record(person, mode="full", 
                                         doc_text=f"[EXTRACTION_ERROR] {str(e)}", 
                                         links={'youtube': [], 'drive_files': [], 'drive_folders': [], 'all_links': []})
                    processed_records.append(record)
            else:
                print(f"  → No document")
                # Create empty full record for person without document
                record = create_record(person, mode="full")
                processed_records.append(record)
    
    # Step 6: Map all data with error handling
    try:
        if processed_records:
            df = step6_map_data(processed_records)
            
            # Display extraction summary
            if Config.TEXT_ONLY or not Config.BASIC_ONLY:
                successful = len([r for r in processed_records if not any(err in r.get('document_text', '') for err in ['[AUTHENTICATION_REQUIRED]', '[EXTRACTION_FAILED]', '[ERROR'])])
                failed = len(processed_records) - successful
                print(f"\n📊 Extraction Summary:")
                print(f"  Successful: {successful}")
                print(f"  Failed: {failed}")
                if failed > 0:
                    print(f"  Success rate: {successful/len(processed_records)*100:.1f}%")
        else:
            print("No records to map")
    except Exception as e:
        print(f"\n❌ ERROR: Failed to save data: {e}")
        sys.exit(1)
    finally:
        # Always cleanup Selenium driver
        try:
            cleanup_driver()
        except:
            pass
    
    print("\n" + "=" * 50)
    print("WORKFLOW COMPLETE")

if __name__ == "__main__":
    main()