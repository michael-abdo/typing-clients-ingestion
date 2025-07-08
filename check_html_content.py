#!/usr/bin/env python3
"""
Check what's in the HTML content that contains YouTube links
"""
import sqlite3
import re

def check_html_content():
    """Check where YouTube links are coming from in the HTML"""
    
    conn = sqlite3.connect("minimal/xenodex.db")
    cursor = conn.cursor()
    
    # Get John Williams' document since it has extracted YouTube links
    query = """
    SELECT p.name, d.id, d.url, substr(d.document_text, 1, 500) as text_preview
    FROM people p
    JOIN documents d ON p.id = d.person_id
    WHERE p.name = 'John Williams'
    """
    
    result = cursor.execute(query).fetchone()
    if result:
        name, doc_id, doc_url, text_preview = result
        print(f"Document for {name}:")
        print(f"URL: {doc_url}")
        print(f"Text preview: {text_preview}")
        print("\n" + "="*80 + "\n")
        
        # Get all extracted links
        links_query = """
        SELECT url FROM extracted_links 
        WHERE document_id = ? AND url LIKE '%youtu%'
        ORDER BY url
        """
        
        links = cursor.execute(links_query, (doc_id,)).fetchall()
        print("Extracted YouTube links:")
        for link in links:
            url = link[0]
            print(f"  - {url}")
            # Extract video ID
            match = re.search(r'[?&]v=([a-zA-Z0-9_-]{11})', url)
            if not match:
                match = re.search(r'youtu\.be/([a-zA-Z0-9_-]{11})', url)
            if match:
                print(f"    Video ID: {match.group(1)}")
        
        print("\n" + "="*80 + "\n")
        print("ANALYSIS:")
        print("Despite 'JavaScript isn't enabled' message, YouTube links were extracted.")
        print("This means the HTML returned by Selenium contains these URLs somewhere.")
        print("\nPossible locations in HTML:")
        print("1. Meta tags (og:video, twitter:player)")
        print("2. JavaScript variables")
        print("3. Hidden form fields")
        print("4. Data attributes")
        print("5. Comments in HTML")
        print("6. Structured data (JSON-LD)")
        
        # Compare with operator expected data
        print("\n" + "="*80 + "\n")
        print("COMPARISON WITH OPERATOR DATA:")
        print("Operator expected: K6kBTbjH4cI, vHD2wDyvWLw, BlSxvQ9p8Q0, ZBuf3DGBuM")
        print("We extracted: BtSNvQ9Rc90, ZBuff3DGbUM, vHD2wDyvWLw")
        print("\nMATCHES:")
        print("✓ vHD2wDyvWLw - Found in both")
        print("✗ K6kBTbjH4cI - Not found in our extraction")
        print("✗ BlSxvQ9p8Q0 - Not found in our extraction") 
        print("? ZBuf3DGBuM vs ZBuff3DGbUM - Almost match (extra 'f')")
        print("+ BtSNvQ9Rc90 - Found by us but not in operator data")
    
    conn.close()

if __name__ == "__main__":
    check_html_content()