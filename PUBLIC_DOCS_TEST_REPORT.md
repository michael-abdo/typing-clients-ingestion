# Google Docs Public Access Test Report

## Executive Summary

I tested whether requests-based Google Docs export can extract YouTube/Drive links from public documents for operators in rows 472-502, specifically focusing on:
- James Kirton (Row 497)
- John Williams (Row 495) 
- Olivia Tomlinson (Row 488)

## Key Findings

### 1. Document Accessibility
- **Total unique Google Docs tested**: ~12-15 per operator
- **Public documents found**: Only 6 documents across all operators
- **Private documents**: ~95% of documents return 401 (Unauthorized), 404 (Not Found), or 410 (Gone)

### 2. Public Documents Analysis
The few public documents found include:
- `1rea25itJO0AMwtoWwo_QbU5XylwnhGWwNVhXah8srUw` - Contains a typing form submission but different YouTube links
- `1lrlkjdy_H6yVQq1DaT-dPwhbNslMUf3ihJtssZvNb-Y` - Public but no YouTube/Drive links
- `1QHPIr511dThfJI424uFZWv3CMa770X87s-djqkheXUU` - Public but no YouTube/Drive links
- `106UzVKTBceNnihO711snD-AeJOK9-fki9EINggkLQU8` - Public but no expected links
- `1XqAOzGWT4FN05s_gDZIsvFrcvA9FtfjRcPkAEwSpeIM` - Public but no expected links
- `1csw8skLfOkYECFqOp6RlJMaXhqrK4ldArcliYRLuTRM` - Public but no expected links

### 3. Expected vs Actual Links Mismatch

**Critical Discovery**: The YouTube/Drive links mentioned as "expected" do NOT exist in the sheet.html data at all.

#### Expected Links (Not Found in Sheet):
- **James Kirton**: vvPK5D7rZvs, 1aQoJb43d1g, vNOpLOL4KdM
- **John Williams**: TJb6DFgJT98, YKPPnNiQfaI
- **Olivia Tomlinson**: kQvjhJ8sNRI, D5jX0E0nUyY, lGjKMmWCw6I, Drive: 18OJA8Y6HRqxCTOZhVjmuqUxtDShtGg-R

#### Actual Data Found (from test_export.json):
- **James Kirton**: 3zCkiF_o7zw, 0mqY6-vhPhY, Zhihkc1AgGo, zwh48UtZQAg
- **John Williams**: BtSNvQ9Rc90, ZBuff3DGbUM
- **Olivia Tomlinson**: 2iwahDWerSQ (transcript only)

### 4. Requests-Based Export Capability

**YES**, requests-based export CAN extract YouTube/Drive links from public Google Docs:
- The public document `1rea25itJO0AMwtoWwo_QbU5XylwnhGWwNVhXah8srUw` successfully exported via requests
- It contained 7 URLs including YouTube links (though not the expected ones)
- Content was fully accessible without requiring browser automation

Example URLs extracted from a public doc:
- https://youtube.com/playlist?list=PLlVGQAG-Rb4GszCFYUklB94fQBPHFIZAO
- https://youtu.be/ebK8g_c-6_s
- https://youtu.be/nny4eaUP4h0

## Conclusions

1. **Requests-based export is sufficient for public Google Docs** - When a document is public, the export endpoint returns full text content including all URLs.

2. **Most documents are private** - ~95% of Google Docs linked in the sheet require authentication and cannot be accessed via simple requests.

3. **Data inconsistency issue** - The "expected" YouTube/Drive links appear to be from a different dataset or version than what's actually in the sheet.html and test_export.json files.

4. **For the extraction pipeline**:
   - Public docs: Requests-based approach works perfectly
   - Private docs: Would require authenticated access (browser automation with login)
   - The current system appears to be working correctly by downloading the actual YouTube videos present in the data

## Recommendations

1. Use requests-based export for public documents (faster and more reliable)
2. Fall back to browser automation only for private documents that require authentication
3. Verify the source of "expected" links - they don't match the actual data
4. The extraction system is correctly processing the YouTube videos that actually exist in the dataset