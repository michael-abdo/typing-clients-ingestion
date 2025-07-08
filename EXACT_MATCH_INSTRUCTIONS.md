# Exact Match Instructions for Operator Data

## Required Modifications

### 1. **Update Column Mapping**
In `extraction_utils.py`, modify `extract_people_from_sheet_html()`:
```python
# Check for direct links in additional columns (beyond column 4)
if len(cells) > 5:  # Look for link columns
    for cell in cells[5:]:
        links = cell.find_all("a")
        for link in links:
            # Extract YouTube/Drive links directly from sheet
```

### 2. **Document Processing with Authentication**
```python
# Add to extraction_utils.py
def extract_with_auth(url, cookies=None):
    # Use Selenium with authenticated session
    driver = get_selenium_driver()
    
    # Load cookies if provided
    if cookies:
        driver.get("https://accounts.google.com")
        for cookie in cookies:
            driver.add_cookie(cookie)
    
    # Now access the document
    driver.get(url)
```

### 3. **Link Categorization Logic**
```python
def categorize_asset_type(has_doc, links):
    if not has_doc and not links:
        return "No asset"
    
    # Count link types
    youtube_videos = [l for l in links if 'youtube.com/watch' in l or 'youtu.be' in l]
    youtube_playlists = [l for l in links if 'youtube.com/playlist' in l]
    drive_files = [l for l in links if 'drive.google.com/file' in l]
    drive_folders = [l for l in links if 'drive.google.com/drive/folders' in l]
    
    # Build description
    parts = []
    if youtube_videos:
        parts.append(f"{len(youtube_videos)} YouTube video{'s' if len(youtube_videos) > 1 else ''}")
    if youtube_playlists:
        parts.append(f"{len(youtube_playlists)} YouTube playlist{'s' if len(youtube_playlists) > 1 else ''}")
    if drive_files:
        parts.append("Google Drive video file")
    if drive_folders:
        parts.append("Google Drive folder")
    
    if has_doc and parts:
        return f"Google Doc → {', '.join(parts)}"
    elif has_doc:
        return "Google Doc (no video links)"
    else:
        return ', '.join(parts) if parts else "No asset"
```

### 4. **Complete Extraction Command**
```bash
# Full extraction with document processing
python3 minimal/simple_workflow.py \
    --text \
    --start-row 472 \
    --end-row 502 \
    --batch-size 5 \
    --retry-failed \
    --output operator_exact_match.csv

# Database mode for better tracking
python3 minimal/simple_workflow.py \
    --db \
    --text \
    --start-row 472 \
    --end-row 502 \
    --db-path operator_data.db
```

## Expected Challenges & Solutions

### Authentication Issues
- **Problem**: Private Google Docs return 403/404 errors
- **Solution**: Use Selenium with logged-in Chrome profile:
```bash
# Use existing Chrome profile
export CHROME_USER_DATA_DIR="$HOME/.config/google-chrome"
export CHROME_PROFILE="Default"
```

### Direct Sheet Links
- **Problem**: Rows 499, 493, 490 have links directly in sheet, not in docs
- **Solution**: Modify extraction to check multiple columns for links

### Link Normalization
- **Problem**: YouTube URLs have tracking parameters (&si=)
- **Solution**: Strip parameters during extraction:
```python
def normalize_youtube_url(url):
    # Remove tracking parameters
    base_url = url.split('&si=')[0].split('?si=')[0]
    return base_url
```

## Verification Process

1. **Run extraction on rows 472-502**
2. **Compare output against operator data**
3. **Check for mismatches in:**
   - Names (handle special characters like ‑ vs -)
   - Asset type descriptions
   - Link counts and formats
4. **Iterate until 100% match achieved**

## Success Criteria

✅ All 31 rows extracted (472-502)
✅ Asset types match exactly
✅ Link counts match for each person
✅ "No asset" correctly identified
✅ Google Doc indicators present where expected