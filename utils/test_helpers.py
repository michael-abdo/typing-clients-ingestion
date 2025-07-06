#!/usr/bin/env python3
"""
Test Helper Utilities (DRY)
Shared test data factories and utilities for tests and benchmarks
"""

from utils.csv_manager import CSVManager


class TestDataFactory:
    """Factory for creating test data (DRY)"""
    
    @staticmethod
    def create_test_person(row_id: str = "100", name: str = "Test User", 
                          email: str = "test@example.com", 
                          personality_type: str = "FF-Ne/Ti-CP/B(S) #3",
                          doc_link: str = "") -> dict:
        """Create a test person record"""
        return {
            'row_id': row_id,
            'name': name,
            'email': email,
            'type': personality_type,
            'doc_link': doc_link
        }
    
    @staticmethod
    def create_test_links(youtube_count: int = 2, drive_count: int = 1) -> dict:
        """Create test link data"""
        youtube_links = [f"https://www.youtube.com/watch?v=test{i:011d}" for i in range(youtube_count)]
        drive_links = [f"https://drive.google.com/file/d/test{i:025d}/view" for i in range(drive_count)]
        
        return {
            'youtube': youtube_links,
            'drive_files': drive_links,
            'drive_folders': [],
            'all_links': youtube_links + drive_links
        }
    
    @staticmethod
    def create_test_document(content_length: int = 1000) -> tuple:
        """Create test document content and text"""
        doc_content = f"<html><body>{'Test content ' * (content_length // 12)}</body></html>"
        doc_text = "Test document text content " * (content_length // 27)
        return doc_content, doc_text
    
    @staticmethod
    def create_test_record(person: dict, links: dict = None, doc_text: str = "") -> dict:
        """Create a test CSV record using the centralized factory"""
        return CSVManager.create_record(person, mode='full', doc_text=doc_text, links=links)
    
    @staticmethod
    def generate_test_batch(num_records: int = 100, with_links: bool = False) -> list:
        """Generate a batch of test records for benchmarking (DRY)"""
        test_data = []
        for i in range(num_records):
            person = TestDataFactory.create_test_person(
                row_id=f"test_{i:04d}",
                name=f"Test Person {i}",
                email=f"test{i}@example.com",
                personality_type=f"Test-Type-{i % 5}",
                doc_link=f"https://example.com/doc_{i}" if i % 3 == 0 else ""
            )
            
            if with_links and i % 3 == 0:
                # Add some records with links for more realistic testing
                links = TestDataFactory.create_test_links(
                    youtube_count=i % 3 + 1,
                    drive_count=i % 2 + 1
                )
                record = TestDataFactory.create_test_record(person, links=links)
            else:
                record = person
                # For basic records, ensure 'link' field exists for compatibility
                record['link'] = record.get('doc_link', '')
            
            test_data.append(record)
        
        return test_data