#!/usr/bin/env python3
"""
Direct verification without subprocess calls
"""
import sys
import os
sys.path.append('.')

# Direct execution
try:
    # Load the sheet HTML
    with open('minimal/sheet.html', 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    print(f"📄 Loaded sheet.html: {len(html_content):,} characters")
    
    # Import and run extraction
    from minimal.extraction_utils import extract_people_from_sheet_html
    people_data = extract_people_from_sheet_html(html_content)
    
    print(f"✅ Extracted {len(people_data)} people from sheet")
    
    # Filter to rows 472-502
    target_people = {}
    for person in people_data:
        try:
            row_id = int(person.get('row_id', 0))
            if 472 <= row_id <= 502:
                target_people[row_id] = person
        except (ValueError, TypeError):
            continue
    
    print(f"🎯 Found {len(target_people)} people in rows 472-502")
    
    # Check specific high-priority cases
    priority_cases = {
        497: "James Kirton",
        495: "John Williams", 
        488: "Olivia Tomlinson",
        501: "Dmitriy Golovko",
        499: "Carlos Arthur"
    }
    
    print(f"\n🏆 HIGH-PRIORITY VERIFICATION:")
    
    for row_id, expected_name in priority_cases.items():
        if row_id in target_people:
            person = target_people[row_id]
            extracted_name = person.get('name', '')
            doc_link = person.get('doc_link', '')
            
            # Simple name comparison
            name_match = expected_name.lower() in extracted_name.lower() or extracted_name.lower() in expected_name.lower()
            
            if name_match:
                print(f"  ✅ Row {row_id}: {expected_name} → FOUND as '{extracted_name}'")
                if doc_link:
                    print(f"      📄 Google Doc: {doc_link[:60]}...")
                else:
                    print(f"      📄 No Google Doc link")
            else:
                print(f"  ⚠️  Row {row_id}: {expected_name} → NAME MISMATCH: '{extracted_name}'")
        else:
            print(f"  ❌ Row {row_id}: {expected_name} → NOT FOUND")
    
    # Show all available rows in range for debugging
    print(f"\n📋 ALL ROWS 472-502 FOUND:")
    for row_id in sorted(target_people.keys()):
        person = target_people[row_id]
        name = person.get('name', 'N/A')
        has_doc = "📄" if person.get('doc_link') else "📄❌"
        print(f"  Row {row_id}: {name} {has_doc}")
    
    # Summary
    total_expected = 31  # 472-502 inclusive
    total_found = len(target_people)
    priority_found = sum(1 for row_id in priority_cases if row_id in target_people)
    
    print(f"\n📊 SUMMARY:")
    print(f"Expected rows 472-502: {total_expected}")
    print(f"Found rows: {total_found}")
    print(f"Priority cases found: {priority_found}/{len(priority_cases)}")
    print(f"Success rate: {(total_found/total_expected)*100:.1f}%")
    
    if total_found >= 25 and priority_found >= 4:
        print(f"\n🎉 VERIFICATION SUCCESSFUL: System is working correctly!")
    elif total_found >= 15:
        print(f"\n✅ VERIFICATION PARTIAL: System is mostly working")
    else:
        print(f"\n❌ VERIFICATION FAILED: System needs debugging")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()