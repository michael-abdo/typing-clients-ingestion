#!/usr/bin/env python3
"""
DatabaseManager - Centralized database operations for simple_workflow
Following DRY principles: All database operations in one place
"""

import sqlite3
from typing import Dict, List, Optional, Tuple, Any
from datetime import datetime
import json
from pathlib import Path
from dataclasses import dataclass, field
import csv
import sys
import os

# DRY: Setup consolidated imports once at module level to eliminate multiple sys.path.append calls
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from utils.config import validate_file_exists, read_csv_rows, parse_pipe_separated, format_error

@dataclass
class Person:
    """Data model for a person"""
    row_id: str
    name: str
    email: str
    type: str
    doc_link: str = ""
    id: Optional[int] = None
    
@dataclass
class Document:
    """Data model for a document"""
    person_id: int
    url: str
    document_type: str = "other"
    document_text: str = ""
    processed: bool = False
    extraction_date: Optional[str] = None
    id: Optional[int] = None

@dataclass
class ExtractedLink:
    """Data model for an extracted link"""
    document_id: int
    url: str
    link_type: str = "other"
    id: Optional[int] = None

class DatabaseManager:
    """Manages all database operations"""
    
    def __init__(self, db_path: str = "xenodex.db"):
        self.db_path = db_path
        self.connection = None
        
    def __enter__(self):
        self.connect()
        return self
        
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
        
    def connect(self):
        """Connect to the database"""
        self.connection = sqlite3.connect(self.db_path)
        self.connection.row_factory = sqlite3.Row
        # Enable foreign keys
        self.connection.execute("PRAGMA foreign_keys = ON")
        
    def disconnect(self):
        """Disconnect from the database"""
        if self.connection:
            self.connection.close()
            self.connection = None
            
    def execute(self, query: str, params: tuple = ()) -> sqlite3.Cursor:
        """Execute a query and return cursor"""
        return self.connection.execute(query, params)
        
    def commit(self):
        """Commit the current transaction"""
        self.connection.commit()
        
    def rollback(self):
        """Rollback the current transaction"""
        self.connection.rollback()
    
    def get_connection(self):
        """Get the current database connection"""
        return self.connection
        
    # Person operations
    def get_person_by_row_id_email(self, row_id: str, email: str) -> Optional[Dict]:
        """Get a person by row_id and email"""
        cursor = self.execute(
            "SELECT * FROM people WHERE row_id = ? AND email = ?",
            (row_id, email)
        )
        row = cursor.fetchone()
        return dict(row) if row else None
        
    def insert_person(self, person: Person) -> int:
        """Insert a person and return their ID"""
        # Check if already exists
        existing = self.get_person_by_row_id_email(person.row_id, person.email)
        if existing:
            return existing['id']
            
        cursor = self.execute(
            """INSERT INTO people (row_id, name, email, type) 
               VALUES (?, ?, ?, ?)""",
            (person.row_id, person.name, person.email, person.type)
        )
        return cursor.lastrowid
        
    def get_all_people(self) -> List[Dict]:
        """Get all people from the database"""
        cursor = self.execute("SELECT * FROM people ORDER BY row_id")
        return [dict(row) for row in cursor.fetchall()]
        
    def get_people_without_documents(self) -> List[Dict]:
        """Get people who don't have any documents"""
        cursor = self.execute("""
            SELECT p.* FROM people p
            LEFT JOIN documents d ON p.id = d.person_id
            WHERE d.id IS NULL
            ORDER BY p.row_id
        """)
        return [dict(row) for row in cursor.fetchall()]
        
    # Document operations
    def get_document_by_url(self, url: str) -> Optional[Dict]:
        """Get a document by URL"""
        cursor = self.execute(
            "SELECT * FROM documents WHERE url = ?",
            (url,)
        )
        row = cursor.fetchone()
        return dict(row) if row else None
        
    def insert_document(self, document: Document) -> int:
        """Insert a document and return its ID"""
        # Check if already exists
        existing = self.get_document_by_url(document.url)
        if existing:
            return existing['id']
            
        cursor = self.execute(
            """INSERT INTO documents 
               (person_id, url, document_type, document_text, processed, extraction_date)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (document.person_id, document.url, document.document_type,
             document.document_text, document.processed, document.extraction_date)
        )
        return cursor.lastrowid
        
    def update_document_text(self, document_id: int, text: str, processed: bool = True):
        """Update document text and processing status"""
        self.execute(
            """UPDATE documents 
               SET document_text = ?, processed = ?, extraction_date = ?
               WHERE id = ?""",
            (text, processed, datetime.now().isoformat(), document_id)
        )
        
    def get_unprocessed_documents(self, limit: Optional[int] = None) -> List[Dict]:
        """Get documents that haven't been processed"""
        query = """
            SELECT d.*, p.name, p.email, p.row_id 
            FROM documents d
            JOIN people p ON d.person_id = p.id
            WHERE d.processed = 0
            ORDER BY p.row_id
        """
        if limit:
            query += f" LIMIT {limit}"
            
        cursor = self.execute(query)
        return [dict(row) for row in cursor.fetchall()]
        
    def get_documents_for_retry(self) -> List[Dict]:
        """Get documents that failed extraction (have EXTRACTION_FAILED in text)"""
        cursor = self.execute("""
            SELECT d.*, p.name, p.email, p.row_id 
            FROM documents d
            JOIN people p ON d.person_id = p.id
            WHERE d.document_text LIKE 'EXTRACTION_FAILED%'
            ORDER BY p.row_id
        """)
        return [dict(row) for row in cursor.fetchall()]
        
    # Link operations
    def insert_link(self, link: ExtractedLink) -> int:
        """Insert an extracted link"""
        cursor = self.execute(
            """INSERT INTO extracted_links (document_id, url, link_type)
               VALUES (?, ?, ?)""",
            (link.document_id, link.url, link.link_type)
        )
        return cursor.lastrowid
        
    def insert_links_batch(self, document_id: int, links: Dict[str, List[str]]):
        """Insert multiple links for a document"""
        link_count = 0
        
        # Process YouTube links
        for url in links.get('youtube', []):
            link_type = 'youtube_playlist' if 'playlist' in url else 'youtube_video'
            self.insert_link(ExtractedLink(document_id, url, link_type))
            link_count += 1
            
        # Process Drive files
        for url in links.get('drive_files', []):
            self.insert_link(ExtractedLink(document_id, url, 'google_drive_file'))
            link_count += 1
            
        # Process Drive folders
        for url in links.get('drive_folders', []):
            self.insert_link(ExtractedLink(document_id, url, 'google_drive_folder'))
            link_count += 1
            
        # Process other links
        processed_urls = set(
            links.get('youtube', []) + 
            links.get('drive_files', []) + 
            links.get('drive_folders', [])
        )
        
        for url in links.get('all_links', []):
            if url not in processed_urls:
                self.insert_link(ExtractedLink(document_id, url, 'other'))
                link_count += 1
                
        return link_count
        
    def get_links_for_document(self, document_id: int) -> List[Dict]:
        """Get all links for a document"""
        cursor = self.execute(
            "SELECT * FROM extracted_links WHERE document_id = ?",
            (document_id,)
        )
        return [dict(row) for row in cursor.fetchall()]
        
    # Processing log operations
    def log_processing(self, person_id: Optional[int], action: str, 
                      status: str, details: str = ""):
        """Add an entry to the processing log"""
        self.execute(
            """INSERT INTO processing_log (person_id, action, status, details)
               VALUES (?, ?, ?, ?)""",
            (person_id, action, status, details)
        )
        
    # Export operations
    def export_to_dataframe(self, mode: str = "full") -> List[Dict]:
        """Export data in a format suitable for DataFrame/CSV"""
        if mode == "basic":
            # Basic mode: just core columns
            cursor = self.execute("""
                SELECT 
                    p.row_id,
                    p.name,
                    p.email,
                    p.type,
                    COALESCE(d.url, '') as link
                FROM people p
                LEFT JOIN documents d ON p.id = d.person_id AND d.document_type = 'google_doc'
                ORDER BY p.row_id
            """)
            return [dict(row) for row in cursor.fetchall()]
            
        elif mode == "text":
            # Text mode: basic + document text
            cursor = self.execute("""
                SELECT 
                    p.row_id,
                    p.name,
                    p.email,
                    p.type,
                    COALESCE(d.url, '') as link,
                    COALESCE(d.document_text, '') as document_text,
                    CASE WHEN d.processed THEN 'yes' ELSE 'no' END as processed,
                    COALESCE(d.extraction_date, '') as extraction_date
                FROM people p
                LEFT JOIN documents d ON p.id = d.person_id AND d.document_type = 'google_doc'
                ORDER BY p.row_id
            """)
            return [dict(row) for row in cursor.fetchall()]
            
        else:
            # Full mode: all columns matching original CSV structure
            cursor = self.execute("""
                WITH person_docs AS (
                    SELECT 
                        p.id as person_id,
                        p.row_id,
                        p.name,
                        p.email,
                        p.type,
                        d.id as doc_id,
                        d.url as link,
                        d.document_text,
                        d.processed
                    FROM people p
                    LEFT JOIN documents d ON p.id = d.person_id AND d.document_type = 'google_doc'
                ),
                link_aggregates AS (
                    SELECT 
                        el.document_id,
                        GROUP_CONCAT(CASE WHEN el.link_type IN ('youtube_video', 'youtube_playlist') 
                                     THEN el.url ELSE NULL END, '|') as youtube_playlist,
                        GROUP_CONCAT(CASE WHEN el.link_type IN ('google_drive_file', 'google_drive_folder') 
                                     THEN el.url ELSE NULL END, '|') as google_drive,
                        GROUP_CONCAT(el.url, '|') as extracted_links
                    FROM extracted_links el
                    GROUP BY el.document_id
                )
                SELECT 
                    pd.row_id,
                    pd.name,
                    pd.email,
                    pd.type,
                    COALESCE(pd.link, '') as link,
                    COALESCE(la.extracted_links, '') as extracted_links,
                    COALESCE(la.youtube_playlist, '') as youtube_playlist,
                    COALESCE(la.google_drive, '') as google_drive,
                    CASE WHEN pd.processed THEN 'yes' ELSE '' END as processed,
                    COALESCE(pd.document_text, '') as document_text,
                    '' as youtube_status,
                    '' as youtube_files,
                    '' as youtube_media_id,
                    '' as drive_status,
                    '' as drive_files,
                    '' as drive_media_id,
                    '' as last_download_attempt,
                    '' as download_errors,
                    '' as permanent_failure
                FROM person_docs pd
                LEFT JOIN link_aggregates la ON pd.doc_id = la.document_id
                ORDER BY pd.row_id
            """)
            return [dict(row) for row in cursor.fetchall()]
            
    # Statistics operations
    def get_statistics(self) -> Dict[str, Any]:
        """Get database statistics"""
        stats = {}
        
        # People stats
        cursor = self.execute("SELECT COUNT(*) as count FROM people")
        stats['total_people'] = cursor.fetchone()['count']
        
        # Document stats
        cursor = self.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN processed = 1 THEN 1 ELSE 0 END) as processed,
                SUM(CASE WHEN document_text != '' THEN 1 ELSE 0 END) as with_text
            FROM documents
        """)
        doc_stats = cursor.fetchone()
        stats['total_documents'] = doc_stats['total']
        stats['processed_documents'] = doc_stats['processed']
        stats['documents_with_text'] = doc_stats['with_text']
        
        # Link stats
        cursor = self.execute("""
            SELECT 
                link_type,
                COUNT(*) as count
            FROM extracted_links
            GROUP BY link_type
        """)
        stats['links_by_type'] = {row['link_type']: row['count'] for row in cursor.fetchall()}
        
        cursor = self.execute("SELECT COUNT(*) as count FROM extracted_links")
        stats['total_links'] = cursor.fetchone()['count']
        
        return stats
    
    # CSV Migration functionality (DRY: consolidates duplicate migration classes)
    def migrate_from_csv(self, csv_file: str = 'simple_output.csv') -> Dict[str, Any]:
        """Migrate data from CSV file to database - database-agnostic implementation"""
        # DRY: Use consolidated file validation from utils/config.py (imported at module level)
        validate_file_exists(csv_file, f"CSV file not found: {csv_file}")
            
        stats = {
            'people': 0,
            'documents': 0,
            'links': 0,
            'errors': [],
            'skipped': 0,
            'total_rows': 0
        }
        
        print(f"\nStarting CSV migration from: {csv_file}")
        print("=" * 50)
        
        # DRY: Use consolidated CSV reading from utils/config.py (imported at module level)
        
        for row_num, row in read_csv_rows(csv_file):
            stats['total_rows'] += 1
            
            try:
                # Extract person data
                person = Person(
                    row_id=row.get('row_id', ''),
                    name=row.get('name', ''),
                    email=row.get('email', ''),
                    type=row.get('type', ''),
                    doc_link=row.get('link', '')
                )
                
                # Insert person
                person_id = self.insert_person(person)
                if person_id:
                    stats['people'] += 1
                else:
                    # Person might already exist
                    existing = self.get_person_by_row_id(person.row_id)
                    if existing:
                        person_id = existing['id']
                        stats['skipped'] += 1
                    else:
                        raise Exception("Failed to insert person")
                
                # Insert document if link exists
                if row.get('link'):
                    doc = Document(
                        person_id=person_id,
                        url=row['link'],
                        document_type=self._determine_document_type(row['link']),
                        document_text=row.get('document_text', ''),
                        processed=row.get('processed') == 'yes',
                        extraction_date=datetime.now().isoformat() if row.get('processed') == 'yes' else None
                    )
                    
                    doc_id = self.insert_document(doc)
                    if doc_id:
                        stats['documents'] += 1
                        
                        # Parse and insert links
                        links = self._parse_csv_links(row)
                        link_count = self.insert_links_batch(doc_id, links)
                        stats['links'] += link_count
                
                self.commit()
                
            except Exception as e:
                self.rollback()
                # DRY: Use consolidated error formatting from utils/config.py
                error_msg = format_error("processing row", row.get('row_id', 'unknown'), e)
                stats['errors'].append(error_msg)
                print(f"  ✗ {error_msg}")
        
        # Print migration summary
        print("\nMigration Summary:")
        print("=" * 50)
        print(f"Total rows processed: {stats['total_rows']}")
        print(f"People inserted: {stats['people']}")
        print(f"Documents inserted: {stats['documents']}")
        print(f"Links inserted: {stats['links']}")
        print(f"Skipped (already exists): {stats['skipped']}")
        print(f"Errors: {len(stats['errors'])}")
        
        if stats['errors']:
            print("\nErrors encountered:")
            for error in stats['errors'][:5]:  # Show first 5 errors
                print(f"  - {error}")
            if len(stats['errors']) > 5:
                print(f"  ... and {len(stats['errors']) - 5} more errors")
        
        return stats
    
    def _determine_document_type(self, url: str) -> str:
        """Determine document type from URL"""
        if not url:
            return 'other'
        
        url_lower = url.lower()
        if 'docs.google.com' in url_lower:
            return 'google_doc'
        elif 'youtube.com' in url_lower or 'youtu.be' in url_lower:
            return 'youtube'
        elif 'drive.google.com' in url_lower:
            return 'google_drive'
        return 'other'
    
    def _parse_csv_links(self, row: Dict) -> Dict[str, List[str]]:
        """Parse links from CSV row into categorized dict"""
        links = {'youtube': [], 'drive_files': [], 'drive_folders': [], 'all_links': []}
        
        # Process YouTube links
        if row.get('youtube_playlist'):
            # DRY: Use consolidated pipe-separated parsing from utils/config.py (imported at module level)
            
            links['youtube'] = parse_pipe_separated(row['youtube_playlist'])
        
        # Process Drive links
        if row.get('google_drive'):
            # DRY: Use consolidated pipe-separated parsing from utils/config.py
            drive_links = parse_pipe_separated(row['google_drive'])
            for link in drive_links:
                if '/folders/' in link:
                    links['drive_folders'].append(link)
                else:
                    links['drive_files'].append(link)
        
        # Process all extracted links
        if row.get('extracted_links'):
            # DRY: Use consolidated pipe-separated parsing from utils/config.py
            links['all_links'] = parse_pipe_separated(row['extracted_links'])
        
        return links