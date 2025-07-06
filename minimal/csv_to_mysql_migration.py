#!/usr/bin/env python3
"""
CSV to MySQL Migration Script
Migrates data from simple_output.csv to a normalized MySQL database
"""

import csv
import mysql.connector
from mysql.connector import Error
from datetime import datetime
import argparse
import sys
from typing import Dict, List, Optional

class CSVToMySQLMigrator:
    def __init__(self, host: str, user: str, password: str, database: str):
        self.host = host
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
        self.cursor = None
        
    def connect(self):
        """Establish database connection"""
        try:
            self.connection = mysql.connector.connect(
                host=self.host,
                user=self.user,
                password=self.password,
                database=self.database
            )
            self.cursor = self.connection.cursor()
            print(f"✓ Connected to MySQL database: {self.database}")
            return True
        except Error as e:
            print(f"✗ Error connecting to MySQL: {e}")
            return False
    
    def disconnect(self):
        """Close database connection"""
        if self.cursor:
            self.cursor.close()
        if self.connection and self.connection.is_connected():
            self.connection.close()
            print("✓ Disconnected from MySQL")
    
    def create_tables(self, schema_file: str = 'database_schema.sql'):
        """Create tables from schema file"""
        try:
            with open(schema_file, 'r') as f:
                schema = f.read()
            
            # Execute each statement separately
            statements = [s.strip() for s in schema.split(';') if s.strip()]
            for statement in statements:
                self.cursor.execute(statement)
            
            self.connection.commit()
            print(f"✓ Created database schema from {schema_file}")
            return True
        except Error as e:
            print(f"✗ Error creating tables: {e}")
            self.connection.rollback()
            return False
    
    def migrate_csv(self, csv_file: str = 'simple_output.csv'):
        """Migrate data from CSV to MySQL"""
        try:
            # Track statistics
            stats = {
                'people': 0,
                'documents': 0,
                'links': 0,
                'errors': []
            }
            
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                
                for row in reader:
                    try:
                        # Insert person (handle duplicates)
                        person_id = self._insert_person(row)
                        if person_id:
                            stats['people'] += 1
                        
                        # Insert document if link exists
                        if row.get('link'):
                            doc_id = self._insert_document(person_id, row)
                            if doc_id:
                                stats['documents'] += 1
                                
                                # Insert extracted links
                                link_count = self._insert_extracted_links(doc_id, row)
                                stats['links'] += link_count
                        
                        # Commit after each person
                        self.connection.commit()
                        
                    except Exception as e:
                        self.connection.rollback()
                        error_msg = f"Error processing row {row.get('row_id')}: {str(e)}"
                        stats['errors'].append(error_msg)
                        print(f"✗ {error_msg}")
            
            # Print migration summary
            print("\n=== Migration Summary ===")
            print(f"✓ People migrated: {stats['people']}")
            print(f"✓ Documents migrated: {stats['documents']}")
            print(f"✓ Links extracted: {stats['links']}")
            if stats['errors']:
                print(f"✗ Errors: {len(stats['errors'])}")
                for error in stats['errors'][:5]:  # Show first 5 errors
                    print(f"  - {error}")
            
            return True
            
        except Exception as e:
            print(f"✗ Migration failed: {e}")
            return False
    
    def _insert_person(self, row: Dict) -> Optional[int]:
        """Insert person and return ID"""
        try:
            # Check if person with this row_id and email already exists
            check_query = "SELECT id FROM people WHERE row_id = %s AND email = %s"
            self.cursor.execute(check_query, (row['row_id'], row['email']))
            existing = self.cursor.fetchone()
            
            if existing:
                return existing[0]
            
            # Insert new person
            insert_query = """
                INSERT INTO people (row_id, name, email, type)
                VALUES (%s, %s, %s, %s)
            """
            self.cursor.execute(insert_query, (
                row['row_id'],
                row['name'],
                row['email'],
                row['type']
            ))
            return self.cursor.lastrowid
            
        except Error as e:
            raise Exception(f"Failed to insert person: {e}")
    
    def _insert_document(self, person_id: int, row: Dict) -> Optional[int]:
        """Insert document and return ID"""
        try:
            # Determine document type
            link = row['link']
            doc_type = 'other'
            if 'docs.google.com' in link:
                doc_type = 'google_doc'
            elif 'youtube.com' in link or 'youtu.be' in link:
                doc_type = 'youtube'
            elif 'drive.google.com' in link:
                doc_type = 'google_drive'
            
            # Insert document
            insert_query = """
                INSERT INTO documents (person_id, url, document_type, document_text, processed, extraction_date)
                VALUES (%s, %s, %s, %s, %s, %s)
            """
            
            processed = row.get('processed') == 'yes'
            extraction_date = datetime.now() if processed else None
            
            self.cursor.execute(insert_query, (
                person_id,
                link,
                doc_type,
                row.get('document_text', ''),
                processed,
                extraction_date
            ))
            return self.cursor.lastrowid
            
        except Error as e:
            raise Exception(f"Failed to insert document: {e}")
    
    def _insert_extracted_links(self, document_id: int, row: Dict) -> int:
        """Insert extracted links and return count"""
        count = 0
        
        # Process YouTube links
        youtube_links = row.get('youtube_playlist', '').split('|') if row.get('youtube_playlist') else []
        for link in youtube_links:
            link = link.strip()
            if link:
                link_type = 'youtube_playlist' if 'playlist' in link else 'youtube_video'
                self._insert_link(document_id, link, link_type)
                count += 1
        
        # Process Google Drive links
        drive_links = row.get('google_drive', '').split('|') if row.get('google_drive') else []
        for link in drive_links:
            link = link.strip()
            if link:
                link_type = 'google_drive_folder' if '/folders/' in link else 'google_drive_file'
                self._insert_link(document_id, link, link_type)
                count += 1
        
        # Process other extracted links
        other_links = row.get('extracted_links', '').split('|') if row.get('extracted_links') else []
        for link in other_links:
            link = link.strip()
            # Skip if already processed as YouTube or Drive
            if link and link not in youtube_links and link not in drive_links:
                self._insert_link(document_id, link, 'other')
                count += 1
        
        return count
    
    def _insert_link(self, document_id: int, url: str, link_type: str):
        """Insert a single link"""
        try:
            insert_query = """
                INSERT INTO extracted_links (document_id, url, link_type)
                VALUES (%s, %s, %s)
            """
            self.cursor.execute(insert_query, (document_id, url, link_type))
        except Error as e:
            raise Exception(f"Failed to insert link: {e}")

def main():
    parser = argparse.ArgumentParser(description='Migrate CSV data to MySQL database')
    parser.add_argument('--host', default='localhost', help='MySQL host')
    parser.add_argument('--user', required=True, help='MySQL user')
    parser.add_argument('--password', required=True, help='MySQL password')
    parser.add_argument('--database', required=True, help='MySQL database name')
    parser.add_argument('--csv', default='simple_output.csv', help='CSV file to migrate')
    parser.add_argument('--schema', default='database_schema.sql', help='SQL schema file')
    parser.add_argument('--create-tables', action='store_true', help='Create tables before migration')
    
    args = parser.parse_args()
    
    # Create migrator
    migrator = CSVToMySQLMigrator(
        host=args.host,
        user=args.user,
        password=args.password,
        database=args.database
    )
    
    # Connect to database
    if not migrator.connect():
        sys.exit(1)
    
    try:
        # Create tables if requested
        if args.create_tables:
            if not migrator.create_tables(args.schema):
                sys.exit(1)
        
        # Run migration
        if not migrator.migrate_csv(args.csv):
            sys.exit(1)
        
    finally:
        migrator.disconnect()

if __name__ == '__main__':
    main()