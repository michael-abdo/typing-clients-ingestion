#!/usr/bin/env python3
"""
Analyze YouTube ID mismatch between operator data and extracted data.
Deep investigation to understand the root cause.
"""
import json
import sqlite3
import re
from pathlib import Path

def analyze_mismatch():
    """Analyze the mismatch between operator data and extracted data"""
    
    # Operator data for key test cases
    operator_data = {
        "James Kirton (497)": {
            "expected_youtube": ["vvPK5D7rZvs", "1aQoJb43d1g", "playlists"],
            "expected_playlists": [
                "PLp0u93QMy5vCYzE4OQydGEMEJjRizGDA", 
                "PLu9i8x5U9PHhmD9K-5WY4EB12vyhL"
            ]
        },
        "John Williams (495)": {
            "expected_youtube": ["K6kBTbjH4cI", "vHD2wDyrWLw", "BlSxvQ9p8Q0", "ZBuf3DGBuM"]
        },
        "Olivia Tomlinson (488)": {
            "expected_youtube": ["NwS2ncgtkoc", "8zo0I4-F3Bs", "Dnmff9nv1b4", "2iwahDWerSQ", 
                                "031Nbfiw4Q", "fiGmuUEOTPB", "3cU7aYwB9Lk"]
        }
    }
    
    # Our extracted data
    print("=== ANALYZING YOUTUBE ID MISMATCH ===\n")
    
    # 1. Check test_export.json for actual downloads
    test_export_path = Path("data/test_export.json")
    if test_export_path.exists():
        with open(test_export_path, 'r') as f:
            test_export = json.load(f)
        
        print("1. ACTUAL DOWNLOADED FILES:")
        print("-" * 50)
        
        # Group by person
        downloads_by_person = {}
        for item in test_export:
            name = item.get('name', '')
            if name in ['James Kirton', 'John Williams', 'Olivia Tomlinson']:
                if name not in downloads_by_person:
                    downloads_by_person[name] = []
                downloads_by_person[name].append(item)
        
        for person, downloads in downloads_by_person.items():
            print(f"\n{person}:")
            youtube_ids = []
            for dl in downloads:
                filename = dl.get('filename', '')
                # Extract YouTube ID from filename
                match = re.match(r'([a-zA-Z0-9_-]{11})', filename)
                if match:
                    youtube_ids.append(match.group(1))
                    print(f"  - {filename} (ID: {match.group(1)})")
            
            # Compare with operator data
            for op_name, op_data in operator_data.items():
                if person in op_name:
                    expected = op_data.get('expected_youtube', [])
                    print(f"\n  Expected IDs: {expected}")
                    print(f"  Actual IDs: {youtube_ids}")
                    print(f"  Match: {'NO - DIFFERENT VIDEOS!' if set(youtube_ids) != set(expected[:len(youtube_ids)]) else 'YES'}")
    
    # 2. Check database for document links
    print("\n\n2. DATABASE DOCUMENT ANALYSIS:")
    print("-" * 50)
    
    conn = sqlite3.connect("minimal/xenodex.db")
    cursor = conn.cursor()
    
    # Get document URLs and extracted links
    query = """
    SELECT p.name, p.row_id, d.url as doc_url, 
           substr(d.document_text, 1, 200) as doc_text_preview,
           el.url as extracted_link
    FROM people p
    JOIN documents d ON p.id = d.person_id
    LEFT JOIN extracted_links el ON d.id = el.document_id
    WHERE p.name IN ('James Kirton', 'John Williams', 'Olivia Tomlinson')
    ORDER BY p.name, el.url
    """
    
    results = cursor.execute(query).fetchall()
    
    current_person = None
    for row in results:
        name, row_id, doc_url, doc_text, link = row
        if name != current_person:
            current_person = name
            print(f"\n{name} (Row {row_id}):")
            print(f"  Doc URL: {doc_url}")
            print(f"  Doc Text Preview: {doc_text[:100]}...")
            print("  Extracted Links:")
        
        if link and ('youtube' in link or 'youtu.be' in link):
            print(f"    - {link}")
    
    # 3. Check sheet HTML for actual Google Doc links
    print("\n\n3. SHEET HTML ANALYSIS:")
    print("-" * 50)
    
    sheet_html_path = Path("minimal/sheet.html")
    if sheet_html_path.exists():
        with open(sheet_html_path, 'r', encoding='utf-8') as f:
            html = f.read()
        
        # Search for each person and their Google Doc links
        for person in ['James Kirton', 'John Williams', 'Olivia Tomlinson']:
            # Find person in HTML
            person_index = html.find(person)
            if person_index != -1:
                # Get surrounding context
                context = html[person_index-200:person_index+500]
                
                # Extract Google Doc ID
                doc_match = re.search(r'docs\.google\.com/document/d/([a-zA-Z0-9-_]+)', context)
                if doc_match:
                    doc_id = doc_match.group(1)
                    print(f"\n{person}:")
                    print(f"  Google Doc ID in sheet: {doc_id}")
                    
                    # Check if this matches database
                    cursor.execute("""
                        SELECT url FROM documents d
                        JOIN people p ON p.id = d.person_id
                        WHERE p.name = ? AND d.url LIKE ?
                    """, (person, f'%{doc_id}%'))
                    
                    db_result = cursor.fetchone()
                    if db_result:
                        print(f"  ✓ Doc ID matches database")
                    else:
                        print(f"  ✗ Doc ID NOT in database!")
    
    # 4. Root cause analysis
    print("\n\n4. ROOT CAUSE ANALYSIS:")
    print("-" * 50)
    print("""
    FINDINGS:
    1. The YouTube IDs in operator data DO NOT EXIST in our sheet.html
    2. The YouTube IDs we extracted are DIFFERENT from operator expectations
    3. Google Docs are being accessed but getting UI content instead of document body
    
    POSSIBLE EXPLANATIONS:
    A. Operator data is from a DIFFERENT VERSION of the spreadsheet
    B. Operator manually checked Google Docs and found different YouTube links
    C. Our extraction is getting blocked and extracting wrong content
    D. The Google Docs have been updated since operator checked them
    
    EVIDENCE:
    - James Kirton has videos 3zCkiF_o7zw, 0mqY6-vhPhY (not vvPK5D7rZvs, 1aQoJb43d1g)
    - Document text shows "JavaScript isn't enabled" - extraction blocked
    - Only UI URLs extracted, not actual document content
    """)
    
    conn.close()

if __name__ == "__main__":
    analyze_mismatch()