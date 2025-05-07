#!/usr/bin/env python3
import csv
import os
import requests
from bs4 import BeautifulSoup
import sys

# Increase CSV field size limit to handle large fields
csv.field_size_limit(sys.maxsize)

# Google Sheets published URL
URL = "https://docs.google.com/spreadsheets/u/1/d/e/2PACX-1vRqqjqoaj8sEZBfZRw0Og7g8ms_0yTL2MsegTubcjhhBnXr1s1jFBwIVAsbkyj1xD0TMj06LvGTQIHU/pubhtml?pli=1#"
CACHE_FILE = "google_sheet_cache.html"

def download_sheet():
    """Download the Google Sheet HTML and save it locally"""
    print(f"Downloading Google Sheet from {URL}...")
    response = requests.get(URL)
    response.raise_for_status()
    
    with open(CACHE_FILE, "w", encoding="utf-8") as f:
        f.write(response.text)
    
    print(f"Google Sheet HTML saved to {CACHE_FILE}")
    return response.text

def get_sheet_html():
    """Get the sheet HTML, either from cache or by downloading"""
    if os.path.exists(CACHE_FILE):
        print(f"Using cached Google Sheet HTML from {CACHE_FILE}")
        with open(CACHE_FILE, "r", encoding="utf-8") as f:
            return f.read()
    else:
        return download_sheet()

def extract_actual_url(google_url):
    """Extract the actual URL from a Google redirect URL"""
    if not google_url.startswith("https://www.google.com/url?q="):
        return google_url
    
    # Extract the 'q' parameter which contains the actual URL
    start_idx = google_url.find("q=") + 2
    end_idx = google_url.find("&", start_idx)
    if end_idx == -1:
        actual_url = google_url[start_idx:]
    else:
        actual_url = google_url[start_idx:end_idx]
    
    # URL decode
    import urllib.parse
    return urllib.parse.unquote(actual_url)

def fetch_table_data():
    html = get_sheet_html()
    soup = BeautifulSoup(html, "html.parser")
    print("HTML retrieved, looking for specific div ID 1159146182...")
    
    # Look for the specific div with ID 1159146182
    target_div = soup.find("div", {"id": "1159146182"})
    if target_div:
        print("Found the target div with ID 1159146182")
        # Find table inside this div
        table = target_div.find("table")
    else:
        print("Div with ID 1159146182 not found, falling back to generic table search")
        # Try different table selectors
        table = soup.find("table", {"class": "waffle"})
        if not table:
            # Try to find any table with content
            tables = soup.find_all("table")
            print(f"Found {len(tables)} tables")
            if tables:
                table = tables[0]  # Use the first table found
    
    records = []
    if table:
        rows = table.find_all("tr")
        print(f"Found {len(rows)} rows in the table")
        
        # First, print more information about the table structure
        print(f"Printing first 10 rows to diagnose structure:")
        for i in range(min(10, len(rows))):
            print(f"Row {i}, cells: {len(rows[i].find_all('td'))}")
        
        # Add detailed inspection of row 15, which we know has links
        if len(rows) > 15:
            print("\nDetailed inspection of row 15 (known to have links):")
            row15 = rows[15]
            cells = row15.find_all("td")
            for j, cell in enumerate(cells):
                print(f"\nCell {j} content:")
                # Get the HTML of the cell (truncated)
                cell_html = str(cell)
                if len(cell_html) > 200:
                    cell_html = cell_html[:200] + "..."
                print(f"HTML: {cell_html}")
                    
                # Get text content
                text = cell.get_text(strip=True)
                print(f"Text: {text}")
                
                # Check for links
                links = cell.find_all("a")
                for k, link in enumerate(links):
                    href = link.get("href", "")
                    link_text = link.get_text(strip=True)
                    print(f"Link {k}: text='{link_text}', href='{href}'")
                    if href.startswith("https://www.google.com/url?q="):
                        actual_url = extract_actual_url(href)
                        print(f"  Extracted URL: {actual_url}")
            
        # Let's look at the cells in row 15 which previously had links
        if len(rows) > 15:
            print(f"Examining row 15 which previously had links:")
            row15_cells = rows[15].find_all("td")
            print(f"Number of cells: {len(row15_cells)}")
            if len(row15_cells) > 2:
                link_cell = row15_cells[2]
                print(f"Link cell text: {link_cell.get_text(strip=True)}")
                a_tags = link_cell.find_all("a")
                print(f"Number of <a> tags: {len(a_tags)}")
                for a_tag in a_tags:
                    print(f"<a> tag href: {a_tag.get('href', 'None')}")
        
        # Now process all rows, starting from row index 15 (0-based) to try to find links
        # We'll try different row indices since the structure might have changed
        print("Trying different row indices to find links...")
        for start_row in [15, 16, 17, 18, 19, 20]:
            print(f"Trying from row index {start_row}...")
            temp_records = []
            for row_index, row in enumerate(rows[start_row:start_row+10], start_row):
                cells = row.find_all("td")
                if len(cells) >= 3:
                    # Extract cell values
                    chapter = cells[0].get_text(strip=True) if len(cells) > 0 else ""
                    doc_name = cells[1].get_text(strip=True) if len(cells) > 1 else ""
                    
                    # Look for link in cell 2
                    link_cell = cells[2]
                    link_text = link_cell.get_text(strip=True)
                    
                    # First try to find a link tag
                    a_tag = link_cell.find("a")
                    if a_tag and a_tag.has_attr("href"):
                        google_url = a_tag["href"]
                        # Extract the actual URL from Google redirect
                        link = extract_actual_url(google_url)
                        
                        if link and (link.startswith("http") or link.startswith("/")):
                            # Only add if both chapter and doc_name exist and aren't empty
                            if chapter and doc_name:
                                # Format the name properly
                                entry_name = f"{chapter} - {doc_name}"
                                
                                print(f"Found valid link in row {row_index}: {link}")
                                temp_records.append({
                                    "name": entry_name,
                                    "link": link,
                                    "type": chapter
                                })
                    # If no link tag but text looks like a URL
                    elif link_text and link_text.startswith("http"):
                        # Format the name properly
                        if chapter and doc_name:
                            entry_name = f"{chapter} - {doc_name}"
                            
                            print(f"Found URL text in row {row_index}: {link_text}")
                            temp_records.append({
                                "name": entry_name,
                                "link": link_text,
                                "type": chapter
                            })
            
            if temp_records:
                print(f"Found {len(temp_records)} records from start row {start_row}")
                records.extend(temp_records)
                break
                
        # If we still don't have records, try extracting from the Google URLs directly
        if not records:
            print("Looking for any Google redirect URLs in all rows...")
            # First look for header rows to determine column indexes
            header_row = None
            name_index = None
            type_index = None
            
            for i, row in enumerate(rows[:10]):  # Search in first 10 rows for header
                cells = row.find_all("td")
                if len(cells) < 5:
                    continue
                    
                for idx, cell in enumerate(cells):
                    text = cell.get_text(strip=True)
                    if text == "Name":
                        name_index = idx
                    elif text == "Type":
                        type_index = idx
                
                if name_index is not None and type_index is not None:
                    header_row = i
                    break
            
            if header_row is None or name_index is None or type_index is None:
                print("Could not find header row with 'Name' and 'Type' columns")
                # Fall back to default indexes
                name_index = 2  # Try 2 instead of 3
                type_index = 5  # Try 5 instead of 6
            
            print(f"Using columns: Name={name_index}, Type={type_index}")
            
            # Figure out what cell has the actual document link - based on our inspection
            # Cell 2 (name_index) has the name and document link
            
            # Based on our inspection, row 15 is "Christina" in cell 2 with a link
            # Cell 2 (name_index=2) contains the name with a hyperlink to the document
            name_index = 2  # Hard-code the name index to 2
            type_index = 4  # Hard-code the type index to 4
            
            print(f"Using hard-coded indices: Name={name_index}, Type={type_index}")
            
            # Loop through all the rows
            for row_index in range(15, len(rows)):  # Start from row 15 which we know has links
                row = rows[row_index]  # Get the current row based on the row_index
                cells = row.find_all("td")
                
                # Need at least enough cells
                if len(cells) <= max(name_index, type_index):
                    continue
                
                # Get cells data
                name_cell = cells[name_index]  # Column 2 (Name)
                name = name_cell.get_text(strip=True)
                
                email_cell = cells[3]  # Column 3 (Email)
                email = email_cell.get_text(strip=True)
                
                type_cell = cells[type_index]  # Column 4 (Type)
                type_val = type_cell.get_text(strip=True)
                
                # Skip rows without a name
                if not name:
                    continue
                
                # Look for a link in the name cell
                doc_link = None
                a_tags = name_cell.find_all("a")  
                if a_tags:
                    # Use the link from the name cell
                    a_tag = a_tags[0]  # Take the first link
                    if a_tag.has_attr("href"):
                        href = a_tag["href"]
                        if href.startswith("https://www.google.com/url?q="):
                            # Extract the actual URL
                            doc_link = extract_actual_url(href)
                
                # Add the record regardless of whether it has a link
                print(f"Found record: Name={name}, Email={email}, Type={type_val}, Link={doc_link}")
                records.append({
                    "name": name,
                    "email": email,
                    "type": type_val,
                    "link": doc_link if doc_link else ""
                })
    else:
        print("No table found in the HTML")
    return records

def update_csv():
    filename = "output.csv"
    existing_links = set()
    
    # Check if file exists and read existing links
    if os.path.exists(filename):
        with open(filename, "r", newline="", encoding="utf-8") as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                if row.get("link"):
                    existing_links.add(row["link"])
    
    # Fetch the data from the Google Sheet
    data = fetch_table_data()
    print(f"Found {len(data)} records in the Google Sheet")
    
    # Filter to only new records with links not already in CSV
    new_records = []
    for record in data:
        if record["link"] and record["link"] in existing_links:
            continue  # Skip if link exists
        new_records.append(record)
    
    print(f"{len(new_records)} new records to add to CSV")
    
    # If we have new records, update the CSV file
    if new_records:
        # Create a new CSV if it doesn't exist or is empty
        write_header = not os.path.exists(filename) or os.stat(filename).st_size == 0
        
        if write_header:
            # Write a new file with header and all records
            with open(filename, "w", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=["name", "email", "type", "link"])
                writer.writeheader()
                writer.writerows(data)
            print(f"Created new CSV with {len(data)} records")
        else:
            # Append just the new records to the existing file
            with open(filename, "a", newline="", encoding="utf-8") as csvfile:
                writer = csv.DictWriter(csvfile, fieldnames=["name", "email", "type", "link"])
                writer.writerows(new_records)
            print(f"Added {len(new_records)} new records to CSV")

if __name__ == "__main__":
    print("Fetching data from Google Sheets...")
    data = fetch_table_data()
    print(f"Found {len(data)} records")
    update_csv()
    print("CSV update complete")