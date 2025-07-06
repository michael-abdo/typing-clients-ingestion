#!/usr/bin/env python3
"""
SIMPLE 6-STEP WORKFLOW - DATABASE EDITION
DRY principles applied: Database operations, extraction logic, and workflow separated
"""

import requests
import pandas as pd
import argparse
import sys
import json
import time
from datetime import datetime
from pathlib import Path
import os

# Import our refactored modules
from database_manager import DatabaseManager, Person, Document, ExtractedLink
from extraction_utils import (
    extract_google_doc_text, extract_links_from_content,
    filter_meaningful_links, extract_people_from_sheet_html,
    determine_document_type, cleanup_driver,
    download_google_sheet, scrape_document_content, extract_text_with_retry
)
# Configuration - centralized settings
class Config:
    GOOGLE_SHEET_URL = "https://docs.google.com/spreadsheets/u/1/d/e/2PACX-1vRqqjqoaj8sEZBfZRw0Og7g8ms_0yTL2MsegTubcjhhBnXr1s1jFBwIVAsbkyj1xD0TMj06LvGTQIHU/pubhtml?pli=1#"
    TARGET_DIV_ID = "1159146182"
    OUTPUT_DIR = Path("simple_downloads")
    
    # Database configuration
    DATABASE_PATH = "xenodex.db"
    USE_DATABASE = True
    OUTPUT_FORMAT = "both"  # Options: "csv", "db", "both"
    
    # Output files
    OUTPUT_CSV_FULL = "simple_output.csv"
    OUTPUT_CSV_BASIC = "simple_output.csv"
    
    # Processing modes
    BASIC_ONLY = False
    TEXT_ONLY = False
    TEST_LIMIT = None
    
    # Batch processing settings
    BATCH_SIZE = 10
    RETRY_ATTEMPTS = 3
    DELAY_BETWEEN_DOCS = 2
    
    # Incremental processing
    SKIP_PROCESSED = True
    RETRY_FAILED = False


def step2_process_people_data(html_content, db_manager):
    """Step 2: Extract people data and save to database"""
    print("Step 2: Processing people data...")
    
    # Extract people from HTML
    people_data = extract_people_from_sheet_html(html_content, Config.TARGET_DIV_ID)
    
    # Save to database if enabled
    if Config.USE_DATABASE and db_manager:
        print("Saving people to database...")
        for person_dict in people_data:
            person = Person(
                row_id=person_dict['row_id'],
                name=person_dict['name'],
                email=person_dict['email'],
                type=person_dict['type'],
                doc_link=person_dict.get('doc_link', '')
            )
            person_id = db_manager.insert_person(person)
            
            # If person has a document, insert it too
            if person.doc_link:
                doc = Document(
                    person_id=person_id,
                    url=person.doc_link,
                    document_type=determine_document_type(person.doc_link)
                )
                db_manager.insert_document(doc)
        
        db_manager.commit()
        print(f"✓ Saved {len(people_data)} people to database")
    
    return people_data


def process_documents_batch(db_manager, docs_to_process, mode="full"):
    """Process documents in batches with database updates"""
    processed_count = 0
    failed_count = 0
    
    for i in range(0, len(docs_to_process), Config.BATCH_SIZE):
        batch = docs_to_process[i:i + Config.BATCH_SIZE]
        batch_num = (i // Config.BATCH_SIZE) + 1
        total_batches = (len(docs_to_process) + Config.BATCH_SIZE - 1) // Config.BATCH_SIZE
        
        print(f"\n📦 BATCH {batch_num}/{total_batches} ({len(batch)} documents)")
        print("-" * 50)
        
        # Start transaction for batch
        if db_manager:
            db_manager.log_processing(None, f"batch_{batch_num}_start", "started", 
                                    f"Processing batch {batch_num} of {total_batches}")
        
        for j, doc_info in enumerate(batch):
            doc_index = i + j + 1
            person_name = doc_info.get('name', 'Unknown')
            doc_url = doc_info.get('url', doc_info.get('doc_link'))
            doc_id = doc_info.get('id')
            person_id = doc_info.get('person_id')
            
            print(f"\n[{doc_index}/{len(docs_to_process)}] Processing: {person_name}")
            print(f"  Document: {doc_url}")
            
            try:
                if mode == "text" or mode == "full":
                    # Extract text with retry
                    doc_text, error = extract_text_with_retry(doc_url, Config.RETRY_ATTEMPTS, Config.DELAY_BETWEEN_DOCS)
                    
                    if error:
                        print(f"  ✗ Failed: {error}")
                        failed_count += 1
                        if db_manager and doc_id:
                            db_manager.update_document_text(doc_id, f"EXTRACTION_FAILED: {error}", False)
                            db_manager.log_processing(person_id, "text_extraction", "failed", error)
                    else:
                        print(f"  ✓ Success: {len(doc_text)} characters extracted")
                        processed_count += 1
                        
                        if db_manager and doc_id:
                            db_manager.update_document_text(doc_id, doc_text, True)
                            db_manager.log_processing(person_id, "text_extraction", "completed", 
                                                   f"Extracted {len(doc_text)} characters")
                
                if mode == "full":
                    # Also extract links
                    doc_content, _ = scrape_document_content(doc_url)
                    links = extract_links_from_content(doc_content, doc_text if 'doc_text' in locals() else "")
                    
                    if db_manager and doc_id:
                        link_count = db_manager.insert_links_batch(doc_id, links)
                        db_manager.log_processing(person_id, "link_extraction", "completed", 
                                               f"Extracted {link_count} links")
                
                # Commit after each document
                if db_manager:
                    db_manager.commit()
                    
            except Exception as e:
                print(f"  ✗ Error processing document: {e}")
                failed_count += 1
                if db_manager and person_id:
                    db_manager.log_processing(person_id, "document_processing", "failed", str(e))
                    db_manager.commit()
            
            # Add delay between documents
            if j < len(batch) - 1:
                time.sleep(Config.DELAY_BETWEEN_DOCS)
        
        # Log batch completion
        if db_manager:
            db_manager.log_processing(None, f"batch_{batch_num}_end", "completed", 
                                    f"Batch {batch_num} complete: {processed_count} processed, {failed_count} failed")
            db_manager.commit()
        
        print(f"\n✓ Batch {batch_num} complete")
        print(f"  Successful: {processed_count - failed_count}")
        print(f"  Failed: {failed_count}")
    
    return processed_count, failed_count

def export_to_csv(db_manager, mode="full", output_file=None):
    """Export database to CSV format"""
    if not output_file:
        output_file = Config.OUTPUT_CSV_FULL if mode == "full" else Config.OUTPUT_CSV_BASIC
    
    print(f"\nExporting to CSV: {output_file}")
    
    # Get data from database
    records = db_manager.export_to_dataframe(mode)
    
    # Convert to DataFrame
    df = pd.DataFrame(records)
    
    # Save to CSV
    df.to_csv(output_file, index=False)
    
    print(f"✓ Exported {len(df)} records to {output_file}")
    return df

def parse_arguments():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='Simple 6-Step Workflow - Database Edition')
    
    # Processing mode options
    mode_group = parser.add_mutually_exclusive_group()
    mode_group.add_argument('--basic', action='store_true', 
                           help='Extract only basic columns')
    mode_group.add_argument('--text', action='store_true',
                           help='Extract basic columns + document text')
    
    # Database options
    parser.add_argument('--db', action='store_true', default=True,
                       help='Use database (default: True)')
    parser.add_argument('--no-db', dest='db', action='store_false',
                       help='Disable database, use CSV only')
    parser.add_argument('--db-path', type=str, default='xenodex.db',
                       help='Database file path (default: xenodex.db)')
    
    # Output options
    parser.add_argument('--output-format', choices=['csv', 'db', 'both'], 
                       default='both', help='Output format (default: both)')
    parser.add_argument('--output', type=str, metavar='FILE',
                       help='Override output CSV filename')
    
    # Processing options
    parser.add_argument('--test-limit', type=int, metavar='N',
                       help='Limit processing to N records')
    parser.add_argument('--batch-size', type=int, metavar='N', default=10,
                       help='Process N documents per batch')
    parser.add_argument('--skip-processed', action='store_true', default=True,
                       help='Skip already processed documents (default)')
    parser.add_argument('--reprocess-all', dest='skip_processed', action='store_false',
                       help='Reprocess all documents')
    parser.add_argument('--retry-failed', action='store_true',
                       help='Retry previously failed extractions')
    
    return parser.parse_args()

def main():
    """Run the complete workflow with database support"""
    # Parse arguments
    args = parse_arguments()
    
    # Configure based on arguments
    Config.BASIC_ONLY = args.basic
    Config.TEXT_ONLY = args.text
    Config.USE_DATABASE = args.db
    Config.DATABASE_PATH = args.db_path
    Config.OUTPUT_FORMAT = args.output_format
    Config.BATCH_SIZE = args.batch_size
    Config.SKIP_PROCESSED = args.skip_processed
    Config.RETRY_FAILED = args.retry_failed
    
    if args.test_limit:
        Config.TEST_LIMIT = args.test_limit
    if args.output:
        Config.OUTPUT_CSV_FULL = args.output
        Config.OUTPUT_CSV_BASIC = args.output
    
    # Display configuration
    if Config.BASIC_ONLY:
        mode = "BASIC MODE"
    elif Config.TEXT_ONLY:
        mode = f"TEXT EXTRACTION MODE"
    else:
        mode = "FULL MODE"
    
    db_text = f" [DB: {Config.DATABASE_PATH}]" if Config.USE_DATABASE else " [CSV ONLY]"
    limit_text = f" (limited to {Config.TEST_LIMIT})" if Config.TEST_LIMIT else ""
    
    print(f"STARTING SIMPLE 6-STEP WORKFLOW - {mode}{db_text}{limit_text}")
    print("=" * 80)
    
    # Initialize database manager if enabled
    db_manager = None
    if Config.USE_DATABASE:
        db_manager = DatabaseManager(Config.DATABASE_PATH)
        db_manager.connect()
        print(f"✓ Connected to database: {Config.DATABASE_PATH}")
        
        # Show current statistics
        stats = db_manager.get_statistics()
        print(f"\nDatabase Statistics:")
        print(f"  People: {stats['total_people']}")
        print(f"  Documents: {stats['total_documents']} ({stats['processed_documents']} processed)")
        print(f"  Links: {stats['total_links']}")
    
    try:
        # Step 1: Download sheet
        html_content = download_google_sheet(Config.GOOGLE_SHEET_URL)
        
        # Step 2: Process people data
        people_data = step2_process_people_data(html_content, db_manager)
        
        # Determine what to process based on mode and database
        if Config.BASIC_ONLY:
            print(f"\n🚀 BASIC MODE: Data extraction complete")
            # Basic mode just extracts people data, no document processing
            
        else:
            # Get documents to process
            if Config.USE_DATABASE and db_manager:
                if Config.RETRY_FAILED:
                    docs_to_process = db_manager.get_documents_for_retry()
                    print(f"\n🔄 Retrying {len(docs_to_process)} failed documents...")
                elif Config.SKIP_PROCESSED:
                    docs_to_process = db_manager.get_unprocessed_documents(Config.TEST_LIMIT)
                    print(f"\n📄 Processing {len(docs_to_process)} unprocessed documents...")
                else:
                    # Reprocess all
                    cursor = db_manager.execute("""
                        SELECT d.*, p.name, p.row_id 
                        FROM documents d 
                        JOIN people p ON d.person_id = p.id
                        ORDER BY p.row_id
                    """)
                    docs_to_process = [dict(row) for row in cursor.fetchall()]
                    if Config.TEST_LIMIT:
                        docs_to_process = docs_to_process[:Config.TEST_LIMIT]
                    print(f"\n📄 Reprocessing {len(docs_to_process)} documents...")
            else:
                # CSV mode - process from people_data
                docs_to_process = [p for p in people_data if p.get('doc_link')]
                if Config.TEST_LIMIT:
                    docs_to_process = docs_to_process[:Config.TEST_LIMIT]
                print(f"\n📄 Processing {len(docs_to_process)} documents...")
            
            # Process documents
            if docs_to_process:
                process_mode = "text" if Config.TEXT_ONLY else "full"
                processed, failed = process_documents_batch(db_manager, docs_to_process, process_mode)
                
                print(f"\n🎉 DOCUMENT PROCESSING COMPLETE")
                print(f"  Total processed: {processed}")
                print(f"  Failed: {failed}")
        
        # Export data based on output format
        if Config.OUTPUT_FORMAT in ["csv", "both"]:
            export_mode = "basic" if Config.BASIC_ONLY else ("text" if Config.TEXT_ONLY else "full")
            export_to_csv(db_manager, export_mode)
        
        # Show final statistics if using database
        if Config.USE_DATABASE and db_manager:
            print("\nFinal Database Statistics:")
            stats = db_manager.get_statistics()
            print(f"  People: {stats['total_people']}")
            print(f"  Documents: {stats['total_documents']} ({stats['processed_documents']} processed)")
            print(f"  Links: {stats['total_links']}")
            if stats.get('links_by_type'):
                print("  Links by type:")
                for link_type, count in stats['links_by_type'].items():
                    print(f"    - {link_type}: {count}")
    
    finally:
        # Cleanup
        cleanup_driver()
        if db_manager:
            db_manager.disconnect()
            print("✓ Database connection closed")
    
    print("\n" + "=" * 50)
    print("WORKFLOW COMPLETE")

if __name__ == "__main__":
    main()