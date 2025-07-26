#!/usr/bin/env python3
"""Extract Google Doc content for a single person."""

import sys
import pandas as pd
from utils.extract_links import extract_text_with_retry, process_url
from utils.logging_config import get_logger
from utils.csv_manager import CSVManager

logger = get_logger(__name__)

def extract_single_person(row_id):
    """Extract Google Doc for a single person by row ID."""
    # Load CSV
    df = pd.read_csv('outputs/output.csv')
    
    # Find the person
    person_df = df[df['row_id'] == row_id]
    if person_df.empty:
        logger.error(f"No person found with row_id {row_id}")
        return False
    
    person = person_df.iloc[0]
    name = person['name']
    doc_link = person['link']
    
    if pd.isna(doc_link) or not doc_link:
        logger.error(f"No document link for {name} (row {row_id})")
        return False
    
    logger.info(f"Extracting document for {name} (row {row_id})")
    logger.info(f"Document URL: {doc_link}")
    
    try:
        # Extract document text
        doc_text, error = extract_text_with_retry(doc_link)
        
        if error:
            logger.error(f"❌ Failed to extract text: {error}")
            doc_text = f"EXTRACTION_FAILED: {error}"
        else:
            logger.info(f"✅ Extracted {len(doc_text)} chars of text")
        
        # Extract links
        links, youtube_playlist, drive_links = process_url(doc_link, limit=1000)
        
        # Extract individual YouTube links from all links
        youtube_links = []
        for link in links:
            if 'youtube.com' in link or 'youtu.be' in link:
                youtube_links.append(link)
        
        logger.info(f"Total links found: {len(links)}")
        logger.info(f"Individual YouTube links: {len(youtube_links)}")
        logger.info(f"Drive links: {len(drive_links) if drive_links else 0}")
        
        # Show YouTube links
        if youtube_links:
            logger.info("YouTube links found:")
            for link in youtube_links:
                logger.info(f"  - {link}")
        
        if drive_links:
            logger.info("Drive links:")
            for link in drive_links[:3]:
                logger.info(f"  - {link}")
        
        # Create full record with all extracted data
        record = CSVManager.create_record(
            person={
                'row_id': row_id,
                'name': name,
                'email': person.get('email', ''),
                'type': person.get('type', ''),
                'link': doc_link
            },
            mode='full',
            doc_text=doc_text,
            links={
                'youtube': youtube_links,  # Use individual links instead of playlist
                'drive_files': drive_links if drive_links else [],
                'drive_folders': [],
                'all_links': links
            }
        )
        
        # Update the CSV row with all the new data
        for col, value in record.items():
            if col in df.columns:
                df.loc[df['row_id'] == row_id, col] = value
            else:
                # Add new column if it doesn't exist
                df[col] = ''
                df.loc[df['row_id'] == row_id, col] = value
        
        # Save updated CSV
        df.to_csv('outputs/output.csv', index=False)
        logger.info(f"✅ Updated CSV with extracted content and links")
        
        return True
            
    except Exception as e:
        logger.error(f"Error extracting document: {e}")
        return False

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("Usage: python extract_single_doc.py <row_id>")
        sys.exit(1)
    
    row_id = int(sys.argv[1])
    success = extract_single_person(row_id)
    sys.exit(0 if success else 1)