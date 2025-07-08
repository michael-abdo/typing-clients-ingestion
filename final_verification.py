#!/usr/bin/env python3
"""
Final verification of direct links fix
"""
import sys
import os
sys.path.append('.')

print("🔍 FINAL VERIFICATION OF DIRECT LINKS FIX")
print("=" * 60)

try:
    from minimal.extraction_utils import extract_people_from_sheet_html
    
    # Load sheet HTML
    with open('minimal/sheet.html', 'r', encoding='utf-8') as f:
        html_content = f.read()
    
    people_data = extract_people_from_sheet_html(html_content)
    
    # Expected vs Actual comparison
    test_cases = {
        499: {
            "name": "Carlos Arthur",
            "expected": "YouTube video link",
            "expected_link": "https://www.youtube.com/watch?v=UD2X2hJTq4Y"
        },
        493: {
            "name": "Kiko",
            "expected": "Google Drive folder",
            "expected_link": "https://drive.google.com/drive/folders/1-4mmoEIuZKq4xOuJpzeBdnxbkYZQJZTl"
        },
        490: {
            "name": "Dan Jane",
            "expected": "Google Drive file",
            "expected_link": "https://drive.google.com/file/d/1LRw22Qv0RS-12vJ61PauCWQHGaga7JEd/view"
        }
    }
    
    results = {"passed": 0, "failed": 0}
    
    for person in people_data:
        try:
            row_id = int(person.get('row_id', 0))
            if row_id in test_cases:
                expected = test_cases[row_id]
                print(f"\n📍 Row {row_id}: {person.get('name')}")
                print(f"   Expected: {expected['name']} with {expected['expected']}")
                
                # Collect all links
                all_links = []
                all_links.extend(person.get('direct_youtube', []))
                all_links.extend(person.get('direct_drive_files', []))
                all_links.extend(person.get('direct_drive_folders', []))
                
                # Check if expected link is found
                if expected['expected_link'] in all_links:
                    print(f"   ✅ PASS: Found expected link!")
                    print(f"      Link: {expected['expected_link']}")
                    results["passed"] += 1
                else:
                    print(f"   ❌ FAIL: Expected link not found!")
                    print(f"      Expected: {expected['expected_link']}")
                    print(f"      Found: {all_links}")
                    results["failed"] += 1
                
                # Show what was extracted
                if person.get('direct_youtube'):
                    print(f"   YouTube: {person['direct_youtube']}")
                if person.get('direct_drive_files'):
                    print(f"   Drive Files: {person['direct_drive_files']}")
                if person.get('direct_drive_folders'):
                    print(f"   Drive Folders: {person['direct_drive_folders']}")
                    
        except:
            continue
    
    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY:")
    print(f"✅ Passed: {results['passed']}/3")
    print(f"❌ Failed: {results['failed']}/3")
    
    if results['passed'] == 3:
        print("\n🎉 SUCCESS: All direct links are being extracted correctly!")
        print("The fix is working as expected!")
    else:
        print("\n⚠️  PARTIAL SUCCESS: Some links are not being extracted")
        print("The fix may need adjustment")
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()