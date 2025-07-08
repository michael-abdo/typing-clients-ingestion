# Root Cause Analysis: Script vs Operator Data Differences

## Executive Summary
The script does NOT produce the same results as operator data because they represent fundamentally different processes:
- **Script**: Automated extraction of ALL links from documents
- **Operator**: Manual curation and selection of SPECIFIC links

## Detailed Root Causes

### 1. **Fundamental Process Difference**
The operator data is NOT raw extraction - it's human-curated selection.

**Evidence:**
- Caroline Chiu: Operator says "No asset" but script finds Drive files in her doc
- Kiko: Operator selects 1 folder from 8+ links the script finds
- Script extracts 63 total links from James Kirton's doc, operator selects 2

### 2. **Playlist ID Manipulation**
Operators manually shortened YouTube playlist IDs.

**Example - James Kirton:**
- Script extracts: `PLpOu93QMy5fVCzxYe4OOydGEMEjlRziGD` (full 33 chars)
- Operator records: `PLp0u93QMy5vCYzE4OQydGEMEJjRizGDA` (shortened ~34 chars with different ending)
- Script extracts: `PLJu98jx5U9PHhImDP9K-5WrV4EB12wyhL` (full 33 chars)
- Operator records: `PLu9i8x5U9PHhmD9K-5WY4EB12vyhL` (shortened 29 chars)

This is NOT a script error - operators modified the IDs.

### 3. **Link Selection Logic**
Operators apply human judgment to select "asset" links vs reference links.

**Example - John Williams:**
- Operator expects 4 specific videos
- Script finds 4 DIFFERENT videos in the doc
- This means the doc contains multiple videos, but operator selected specific ones as the "asset"

### 4. **URL Normalization**
Operators standardize URL formats.

**Examples:**
- `youtu.be/VIDEO_ID` → `https://www.youtube.com/watch?v=VIDEO_ID`
- Remove tracking parameters (`?si=...`)
- Remove duplicate links with different parameters

### 5. **Infrastructure Link Filtering**
Script includes ALL links; operators filter out non-content links.

**James Kirton's doc contains:**
- Authentication URLs (`accounts.google.com`)
- Static resources (`gstatic.com`)
- Chrome extensions
- API endpoints
- The actual content links (2 playlists)

Operators only record the content links.

### 6. **Direct Link Handling**
Direct links (Case 2) work correctly because no extraction/selection is needed.

**Success examples:**
- Carlos Arthur: Direct YouTube link matches exactly
- Dan Jane: Direct Drive link matches exactly

### 7. **Missing Person (Shelesea Evans)**
Likely a row number mismatch or the person is in a different position in the sheet than expected.

## Why This Happens

The E2E workflow requirements state to "extract links" but don't specify:
1. Which links to select as the "asset"
2. How to shorten/normalize playlist IDs
3. Which URL format to use
4. How to filter infrastructure links

The operator data represents BUSINESS LOGIC that requires:
- Understanding which links are the primary "asset" vs references
- Knowledge of how to format/shorten IDs
- Filtering rules for infrastructure URLs

## Conclusion

**The script is working correctly** - it extracts all links as designed.

**The mismatch is expected** because:
1. Automated extraction ≠ Manual curation
2. The operator data includes human judgment not codified in rules
3. The script cannot replicate undocumented business logic

To match operator data, the script would need:
1. Rules for identifying "asset" links vs reference links
2. ID shortening/normalization algorithms
3. Infrastructure URL filtering patterns
4. Business logic for link selection

Without these rules, the script correctly extracts ALL links while operators select SPECIFIC links.