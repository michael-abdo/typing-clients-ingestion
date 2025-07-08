#!/usr/bin/env python3
"""
Debug the extraction pipeline step by step
"""
import sys
import os
sys.path.append('.')

# Import the extraction utilities
try:
    from minimal.extraction_utils import extract_people_from_sheet_html
    print("✅ Successfully imported extract_people_from_sheet_html")
except ImportError as e:
    print(f"❌ Failed to import extract_people_from_sheet_html: {e}")
    sys.exit(1)

def debug_extraction_pipeline():
    """Debug the complete extraction pipeline"""
    
    print("🔍 DEBUG: Testing extraction pipeline...")
    
    # Step 1: Load the existing sheet.html file
    sheet_html_path = "minimal/sheet.html"
    
    try:
        with open(sheet_html_path, 'r', encoding='utf-8') as f:
            html_content = f.read()
        
        print(f"✅ Loaded sheet.html: {len(html_content)} characters")
        
    except Exception as e:
        print(f"❌ Failed to load {sheet_html_path}: {e}")
        return False
    
    # Step 2: Test the extraction function
    try:
        print("\n🔍 Testing extract_people_from_sheet_html function...")
        people_data = extract_people_from_sheet_html(html_content)
        
        print(f"✅ Extraction completed: {len(people_data)} people found")
        
        # Step 3: Analyze the extracted data
        if people_data:
            print("\n📊 Sample extracted data:")
            for i, person in enumerate(people_data[:5]):  # Show first 5
                print(f"  Row {person.get('row_id', 'N/A')}: {person.get('name', 'N/A')} ({person.get('email', 'N/A')})")
                if person.get('doc_link'):
                    print(f"    Doc: {person['doc_link'][:60]}...")
                else:
                    print(f"    Doc: [No link]")
            
            if len(people_data) > 5:
                print(f"  ... and {len(people_data) - 5} more")
            
            # Step 4: Search for specific test subjects
            test_subjects = ['James Kirton', 'John Williams', 'Olivia Tomlinson', 'Dmitriy Golovko']
            print(f"\n🎯 Searching for test subjects: {', '.join(test_subjects)}")
            
            found_subjects = []
            for person in people_data:
                if person.get('name') in test_subjects:
                    found_subjects.append(person)
                    print(f"  ✅ Found: Row {person['row_id']} - {person['name']}")
                    if person.get('doc_link'):
                        print(f"      Doc: {person['doc_link'][:80]}...")
                    else:
                        print(f"      Doc: [No link]")
            
            if not found_subjects:
                print("  ❌ None of the test subjects found in extracted data")
                
                # Debug: Show some actual names for comparison
                print("\n🔍 Sample names from extracted data:")
                for person in people_data[:10]:
                    print(f"    '{person.get('name', 'N/A')}'")
            
            # Step 5: Check people with doc links
            people_with_docs = [p for p in people_data if p.get('doc_link')]
            print(f"\n📄 People with Google Doc links: {len(people_with_docs)}")
            
            if people_with_docs:
                for person in people_with_docs[:3]:  # Show first 3
                    print(f"  Row {person['row_id']}: {person['name']}")
                    print(f"    Doc: {person['doc_link'][:80]}...")
            
            return True
            
        else:
            print("❌ No people data extracted!")
            
            # Debug the HTML structure
            print("\n🔍 Debugging HTML structure...")
            from bs4 import BeautifulSoup
            
            soup = BeautifulSoup(html_content, "html.parser")
            
            # Check for target div
            target_div = soup.find("div", {"id": "1159146182"})
            if target_div:
                print("  ✅ Target div '1159146182' found")
                
                # Check for table
                table = target_div.find("table")
                if table:
                    rows = table.find_all("tr")
                    print(f"  ✅ Table found with {len(rows)} rows")
                    
                    if len(rows) > 1:
                        # Check first data row
                        first_row = rows[1]
                        cells = first_row.find_all("td")
                        print(f"  ✅ First data row has {len(cells)} cells")
                        
                        if len(cells) >= 5:
                            print(f"    Cell 0 (row_id): '{cells[0].get_text(strip=True)}'")
                            print(f"    Cell 2 (name): '{cells[2].get_text(strip=True)}'")
                            print(f"    Cell 3 (email): '{cells[3].get_text(strip=True)}'")
                            print(f"    Cell 4 (type): '{cells[4].get_text(strip=True)}'")
                        else:
                            print(f"    ❌ Not enough cells in first row")
                    else:
                        print(f"  ❌ No data rows found")
                else:
                    print("  ❌ No table found in target div")
            else:
                print("  ❌ Target div '1159146182' not found")
                
                # Check for any tables
                tables = soup.find_all("table")
                print(f"  Found {len(tables)} tables total")
            
            return False
        
    except Exception as e:
        print(f"❌ Extraction failed: {e}")
        import traceback
        traceback.print_exc()
        return False

if __name__ == "__main__":
    success = debug_extraction_pipeline()
    sys.exit(0 if success else 1)