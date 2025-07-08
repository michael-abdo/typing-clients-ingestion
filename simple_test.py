#!/usr/bin/env python3
"""Simple test to understand the extraction system"""
import os
import sys
import sqlite3
import json
from pathlib import Path

# Add the project to Python path
project_root = "/home/Mike/projects/xenodex/typing-clients-ingestion"
sys.path.insert(0, project_root)
sys.path.insert(0, os.path.join(project_root, "minimal"))

print("=== ANALYZING EXTRACTION SYSTEM FOR ROWS 472-502 ===")

# Check what we have in the text extraction output
text_extraction_file = "/home/Mike/projects/xenodex/typing-clients-ingestion/minimal/text_extraction_output.csv"
if os.path.exists(text_extraction_file):
    print(f"\nReading: {text_extraction_file}")
    with open(text_extraction_file, 'r', encoding='utf-8') as f:
        lines = f.readlines()
    
    print(f"Total lines: {len(lines)}")
    
    # Parse the CSV and look for our target rows
    target_rows = []
    for line in lines[1:]:  # Skip header
        parts = line.strip().split(',')
        if len(parts) >= 4:
            row_id = parts[0]
            name = parts[1]
            email = parts[2]
            type_info = parts[3]
            if 472 <= int(row_id) <= 502:
                target_rows.append((row_id, name, email, type_info))
    
    print(f"\nFound {len(target_rows)} rows in target range 472-502:")
    for row_id, name, email, type_info in target_rows:
        print(f"  Row {row_id}: {name} ({email}) - {type_info}")
        
    # Look for the specific cases mentioned in the user's request
    specific_cases = {
        "James Kirton": "2 playlists",
        "John Williams": "4 videos", 
        "Olivia Tomlinson": "7 videos",
        "Dmitriy Golovko": "no assets"
    }
    
    print(f"\nLooking for specific test cases:")
    for name, expected in specific_cases.items():
        found = False
        for row_id, row_name, email, type_info in target_rows:
            if name.lower() in row_name.lower():
                print(f"  ✓ Found {name} in row {row_id} - Expected: {expected}")
                found = True
                break
        if not found:
            print(f"  ✗ {name} not found in target range - Expected: {expected}")

# Check database
db_path = "/home/Mike/projects/xenodex/typing-clients-ingestion/minimal/xenodx.db"
if os.path.exists(db_path):
    print(f"\nChecking database: {db_path}")
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Check table structure
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        print(f"Tables: {tables}")
        
        # Check sheet_data table
        if 'sheet_data' in tables:
            cursor.execute("SELECT COUNT(*) FROM sheet_data WHERE CAST(row_id AS INTEGER) BETWEEN 472 AND 502")
            count = cursor.fetchone()[0]
            print(f"Rows 472-502 in sheet_data: {count}")
            
            if count > 0:
                cursor.execute("SELECT row_id, name, email, type, link FROM sheet_data WHERE CAST(row_id AS INTEGER) BETWEEN 472 AND 502 ORDER BY CAST(row_id AS INTEGER)")
                rows = cursor.fetchall()
                print("Sample rows from sheet_data:")
                for row in rows[:10]:
                    print(f"  {row}")
        
        conn.close()
        
    except Exception as e:
        print(f"Database error: {e}")

# Check if we can find the original sheet.html
print(f"\n=== CHECKING FOR SHEET DATA ===")
sheet_files = [
    "/home/Mike/projects/xenodex/typing-clients-ingestion/minimal/sheet.html",
    "/home/Mike/projects/xenodex/typing-clients-ingestion/data/sheet.html",
    "/home/Mike/projects/xenodex/typing-clients-ingestion/sheet.html"
]

for sheet_file in sheet_files:
    if os.path.exists(sheet_file):
        size = os.path.getsize(sheet_file)
        print(f"{sheet_file}: {size} bytes")
        if size > 0:
            print("  Contains data")
        else:
            print("  Empty file")
    else:
        print(f"{sheet_file}: not found")

print("\n=== ANALYSIS COMPLETE ===")