#!/usr/bin/env python3
"""
Manual test of extraction system for specific rows
"""
import os
import sys
import csv
from pathlib import Path

# Add project paths
project_root = "/home/Mike/projects/xenodex/typing-clients-ingestion"
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, "minimal"))

print("=== MANUAL EXTRACTION TEST ===")

# Test the extraction utilities directly
try:
    from extraction_utils import clean_url, determine_document_type, extract_actual_url
    print("✓ Successfully imported extraction utilities")
    
    # Test URL cleaning functionality
    test_urls = [
        "https://www.youtube.com/watch?v=ABC123",
        "https://youtu.be/XYZ789",
        "https://www.youtube.com/playlist?list=PLtest",
        "https://drive.google.com/file/d/1234567890/view",
        "https://docs.google.com/document/d/abcdef/edit",
        "https://www.google.com/url?q=https://youtu.be/test123&sa=D"
    ]
    
    print("\nTesting URL cleaning:")
    for url in test_urls:
        clean = clean_url(url)
        doc_type = determine_document_type(clean)
        print(f"  {url} -> {clean} ({doc_type})")
    
    # Test Google URL extraction
    print("\nTesting Google redirect URL extraction:")
    google_redirect = "https://www.google.com/url?q=https://youtu.be/test123&sa=D"
    extracted = extract_actual_url(google_redirect)
    print(f"  {google_redirect} -> {extracted}")
    
except ImportError as e:
    print(f"✗ Could not import extraction utilities: {e}")

# Analyze what we have in the CSV files
print("\n=== ANALYZING CSV FILES ===")

csv_files = [
    "/home/Mike/projects/xenodex/typing-clients-ingestion/outputs/simple_output.csv",
    "/home/Mike/projects/xenodex/typing-clients-ingestion/minimal/simple_output.csv",
    "/home/Mike/projects/xenodex/typing-clients-ingestion/minimal/text_extraction_output.csv"
]

for csv_file in csv_files:
    if os.path.exists(csv_file):
        print(f"\n📁 {csv_file}")
        try:
            with open(csv_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                print(f"  📊 Total rows: {len(rows)}")
                
                # Check for target rows
                target_range = range(472, 503)
                target_rows = [row for row in rows if row.get('row_id') and int(row['row_id']) in target_range]
                print(f"  🎯 Rows in target range (472-502): {len(target_rows)}")
                
                # Show sample of target rows
                for row in target_rows[:5]:  # Show first 5
                    row_id = row.get('row_id', 'N/A')
                    name = row.get('name', 'N/A')
                    link = row.get('link', '')
                    processed = row.get('processed', 'N/A')
                    print(f"    Row {row_id}: {name} | Link: {'✓' if link else '✗'} | Processed: {processed}")
                    
        except Exception as e:
            print(f"  ✗ Error reading file: {e}")
    else:
        print(f"\n📁 {csv_file} - NOT FOUND")

# Test if we can create a basic extraction simulation
print("\n=== SIMULATION TEST ===")

# Simulate what the system should do for a few test cases
test_cases = [
    {
        "row_id": "497",
        "name": "James Kirton",
        "expected_assets": "2 playlists",
        "expected_links": ["playlist_link_1", "playlist_link_2"]
    },
    {
        "row_id": "495", 
        "name": "John Williams",
        "expected_assets": "4 videos",
        "expected_links": ["video_1", "video_2", "video_3", "video_4"]
    },
    {
        "row_id": "488",
        "name": "Olivia Tomlinson", 
        "expected_assets": "7 videos",
        "expected_links": ["video_1", "video_2", "video_3", "video_4", "video_5", "video_6", "video_7"]
    },
    {
        "row_id": "501",
        "name": "Dmitriy Golovko",
        "expected_assets": "no assets",
        "expected_links": []
    }
]

print("Expected extraction behavior:")
for case in test_cases:
    print(f"  Row {case['row_id']} ({case['name']}):")
    print(f"    Expected: {case['expected_assets']}")
    print(f"    Should extract: {len(case['expected_links'])} links")
    print(f"    Links: {case['expected_links']}")

# Check if the system files exist
print("\n=== SYSTEM FILES CHECK ===")
critical_files = [
    "/home/Mike/projects/xenodex/typing-clients-ingestion/minimal/extraction_utils.py",
    "/home/Mike/projects/xenodex/typing-clients-ingestion/minimal/simple_workflow.py",
    "/home/Mike/projects/xenodex/typing-clients-ingestion/minimal/database_manager.py",
    "/home/Mike/projects/xenodex/typing-clients-ingestion/minimal/sheet.html",
    "/home/Mike/projects/xenodex/typing-clients-ingestion/minimal/xenodx.db"
]

for file_path in critical_files:
    if os.path.exists(file_path):
        size = os.path.getsize(file_path)
        print(f"  ✓ {file_path} ({size} bytes)")
    else:
        print(f"  ✗ {file_path} - NOT FOUND")

print("\n=== TEST COMPLETE ===")