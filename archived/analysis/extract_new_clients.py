#!/usr/bin/env python3
"""Extract specific new clients from Google Sheets."""

import pandas as pd
import os
from datetime import datetime
from utils.extract_links import extract_text_with_retry, process_url
from utils.logging_config import get_logger
from utils.csv_manager import CSVManager

logger = get_logger(__name__)

# New clients to extract
NEW_CLIENTS = [
    {'row_id': 506, 'name': 'Ryan Madera', 'email': 'ryan.madera27@gmail.com', 'type': 'MM-Ne/Ti-CS/P(B) #4'},
    {'row_id': 505, 'name': 'Yasmin Mahmod', 'email': 'yazmiinaax1@gmail.com', 'type': 'FF-Se/Fi-CP/S(B) #4'},
    {'row_id': 504, 'name': 'Navid Ghahremani', 'email': 'navid.ghahremani@gmail.com', 'type': 'FF-Ne/Ti-CS/P(B) #2'},
    {'row_id': 503, 'name': 'Augusto Evangelista', 'email': 'evangelista.augusto2000@gmail.com', 'type': 'MM-Se/Fi-CS/P(B) #4'}
]

# Their Google Doc links (from the extraction output)
DOC_LINKS = {
    506: 'https://docs.google.com/document/d/12vUN54nsKZegG-8Vfg8gjwEdgthHBwFFn8ToQ7B3VHY/edit?tab=t.0',
    505: 'https://docs.google.com/document/d/1X52-a7FXGO2iLITIHlhqgLz4Q_Dug6JF/edit',
    504: None,  # Will need to find
    503: None   # Will need to find
}

def extract_new_clients():
    """Extract the 4 new clients and add them to CSV."""
    
    # Load existing CSV
    csv_path = 'outputs/output.csv'
    if os.path.exists(csv_path):
        df = pd.read_csv(csv_path)
        logger.info(f"Loaded existing CSV with {len(df)} rows")
    else:
        logger.error("CSV file not found!")
        return
    
    # Process each new client
    new_records = []
    
    for client in NEW_CLIENTS:
        row_id = client['row_id']
        name = client['name']
        email = client['email']
        doc_type = client['type']
        doc_link = DOC_LINKS.get(row_id, '')
        
        logger.info(f"\nProcessing {name} (row {row_id})")
        logger.info(f"Email: {email}")
        logger.info(f"Type: {doc_type}")
        
        if doc_link:
            logger.info(f"Doc link: {doc_link}")
            
            # Extract document text
            doc_text, error = extract_text_with_retry(doc_link, max_attempts=1)
            
            if error:
                logger.error(f"Failed to extract text: {error}")
                doc_text = f"EXTRACTION_FAILED: {error}"
            else:
                logger.info(f"Extracted {len(doc_text)} chars of text")
            
            # Extract links
            links, youtube_playlist, drive_links = process_url(doc_link, limit=1000)
            
            # Extract individual YouTube links
            youtube_links = []
            for link in links:
                if 'youtube.com' in link or 'youtu.be' in link:
                    youtube_links.append(link)
            
            logger.info(f"Found {len(youtube_links)} YouTube links")
            logger.info(f"Found {len(drive_links) if drive_links else 0} Drive links")
            
            # Create full record
            record = CSVManager.create_record(
                person={
                    'row_id': row_id,
                    'name': name,
                    'email': email,
                    'type': doc_type,
                    'link': doc_link
                },
                mode='full',
                doc_text=doc_text,
                links={
                    'youtube': youtube_links,
                    'drive_files': drive_links if drive_links else [],
                    'drive_folders': [],
                    'all_links': links
                }
            )
        else:
            # No doc link, create basic record
            logger.warning(f"No doc link for {name}")
            record = CSVManager.create_record(
                person={
                    'row_id': row_id,
                    'name': name,
                    'email': email,
                    'type': doc_type,
                    'link': ''
                },
                mode='full',
                doc_text='',
                links=None
            )
        
        new_records.append(record)
        logger.info(f"✅ Created record for {name}")
    
    # Add new records to DataFrame
    new_df = pd.DataFrame(new_records)
    
    # Ensure all columns exist in original DataFrame
    for col in new_df.columns:
        if col not in df.columns:
            df[col] = ''
    
    # Append new records
    df = pd.concat([df, new_df], ignore_index=True)
    
    # Save updated CSV
    df.to_csv(csv_path, index=False)
    logger.info(f"\n✅ Saved updated CSV with {len(df)} total rows")
    
    # Summary
    print("\n" + "="*80)
    print("EXTRACTION SUMMARY")
    print("="*80)
    for record in new_records:
        print(f"\n{record['name']} (row {record['row_id']}):")
        print(f"  Email: {record['email']}")
        print(f"  Type: {record['type']}")
        if record['youtube_playlist']:
            yt_count = len(record['youtube_playlist'].split('|'))
            print(f"  YouTube videos: {yt_count}")
        if record['google_drive']:
            drive_count = len(record['google_drive'].split('|'))
            print(f"  Drive files: {drive_count}")

if __name__ == "__main__":
    extract_new_clients()