#!/usr/bin/env python3
"""Debug what we're actually extracting from the sheet"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from minimal.simple_workflow import step1_download_sheet, step2_extract_people_and_docs

def main():
    print("🔍 DEBUGGING SHEET EXTRACTION")
    print("="*50)
    
    # Download sheet
    print("\n1. Downloading sheet...")
    html_content = step1_download_sheet()
    print(f"   Downloaded {len(html_content)} characters")
    
    # Extract people data
    print("\n2. Extracting people data...")
    people_data, people_with_docs = step2_extract_people_and_docs(html_content)
    
    print(f"\n📊 EXTRACTION RESULTS:")
    print(f"   Total people found: {len(people_data)}")
    print(f"   People with Google Docs: {len(people_with_docs)}")
    
    # Show first 10 rows
    print(f"\n📋 FIRST 10 ROWS:")
    for i, person in enumerate(people_data[:10]):
        doc_status = "✅ HAS DOC" if person['doc_link'] else "❌ NO DOC"
        print(f"   {person['row_id']}: {person['name']} - {doc_status}")
    
    # Show last 10 rows
    print(f"\n📋 LAST 10 ROWS:")
    for person in people_data[-10:]:
        doc_status = "✅ HAS DOC" if person['doc_link'] else "❌ NO DOC"
        print(f"   {person['row_id']}: {person['name']} - {doc_status}")
    
    # Check for specific people from truth source
    truth_names = [
        "Caroline Chiu", "James Kirton", "John Williams", "Maddie Boyle",
        "Jeremy May", "Olivia Tomlinson", "Joseph Cortone", "Nathalie Bauer"
    ]
    
    print(f"\n🎯 CHECKING TRUTH SOURCE NAMES:")
    found_names = set(person['name'] for person in people_data)
    
    for name in truth_names:
        if name in found_names:
            person = next(p for p in people_data if p['name'] == name)
            doc_status = "✅ HAS DOC" if person['doc_link'] else "❌ NO DOC"
            print(f"   ✅ {name} (Row {person['row_id']}) - {doc_status}")
        else:
            print(f"   ❌ {name} - NOT FOUND")
    
    # Show all people with docs
    if people_with_docs:
        print(f"\n🔗 ALL PEOPLE WITH GOOGLE DOCS ({len(people_with_docs)}):")
        for person in people_with_docs:
            print(f"   Row {person['row_id']}: {person['name']}")
            print(f"      URL: {person['doc_link'][:80]}...")
    
    # Check row number range
    if people_data:
        row_ids = [int(p['row_id']) for p in people_data if p['row_id'].isdigit()]
        if row_ids:
            print(f"\n📊 ROW RANGE:")
            print(f"   Min row: {min(row_ids)}")
            print(f"   Max row: {max(row_ids)}")
            print(f"   Total numeric rows: {len(row_ids)}")
            
            # Check if we have rows 472-501
            target_range = set(range(472, 502))
            found_range = set(row_ids)
            missing_from_target = target_range - found_range
            
            print(f"   Rows 472-501 found: {len(target_range & found_range)}/30")
            if missing_from_target:
                print(f"   Missing rows: {sorted(list(missing_from_target))[:10]}...")

if __name__ == "__main__":
    main()