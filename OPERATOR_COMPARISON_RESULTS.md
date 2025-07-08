# Operator Data vs Script Results Comparison

## Summary
- **Success Rate**: 33.3% (6/18 matches)
- **Key Issues**: The script extracts different links than operator expects

## Detailed Comparison Table

| Row | Name | Operator Expected | Script Actual | Status | Notes |
|-----|------|------------------|---------------|--------|-------|
| 501 | Dmitriy Golovko | No asset | No links | ✅ MATCH | Correct |
| 502 | Furva Nakamura-Saleem | No asset | No links | ✅ MATCH | Correct |
| 500 | Seth Dossett | No asset | No links | ✅ MATCH | Correct |
| 499 | Carlos Arthur | YouTube video: `UD2X2hJTq4Y` | YouTube: `UD2X2hJTq4Y` | ✅ MATCH | Direct link handled correctly |
| 498 | Caroline Chiu | No asset | 2 Drive files | ❌ MISMATCH | Script found unexpected Drive files |
| 497 | James Kirton | 2 YouTube playlists (short IDs) | 2 YouTube playlists (full IDs) | ⚠️ PARTIAL | Different playlist ID lengths |
| 496 | Florence | No video links | No links | ✅ MATCH | Correct |
| 495 | John Williams | 4 specific YouTube videos | 4 different YouTube videos | ❌ MISMATCH | Different video IDs extracted |
| 493 | Kiko | 1 Drive folder | 8 links (including folder) | ❌ MISMATCH | Script found many extra links |
| 490 | Dan Jane | Drive video: `1LRw22Qv0RS...` | Drive: `1LRw22Qv0RS...` | ✅ MATCH | Direct link handled correctly |
| 489 | Jeremy May | YouTube: `d6IR17a0M2o` | YouTube: `d6IR17a0M2o` | ⚠️ PARTIAL | Same video, different URL format |
| 487 | Shelesea Evans | Drive video | NOT FOUND | ❌ MISMATCH | Person missing from output |
| 485 | Emilie | 1 Drive folder | 2 Drive folders | ⚠️ PARTIAL | Duplicate with/without ?usp param |
| 483 | Brandon Donahue | YouTube: `x2ejX4YbrA` | YouTube: `x2jejX4YbrA` | ⚠️ PARTIAL | Similar but slightly different ID |
| 482 | Joseph Cortone | 2 YouTube videos | 2 YouTube videos | ⚠️ PARTIAL | Similar IDs with typos |
| 475 | Brenden Ohlsson | 1 Drive video | 2 Drive videos | ⚠️ PARTIAL | Duplicate with/without ?usp param |
| 474 | Nathalie Bauer | YouTube: `kK68tiL-RMo` | YouTube: `eK68til-RMo` | ❌ MISMATCH | Different video ID |
| 472 | James Yu | 1 YouTube video | 2 YouTube playlists | ❌ MISMATCH | Found playlists instead of video |

## Key Differences

### 1. **Direct Links (Case 2)**
- ✅ **Working**: Carlos Arthur, Dan Jane - direct links handled correctly
- ⚠️ **Format differences**: Jeremy May - same video but `youtu.be` vs `youtube.com/watch`

### 2. **Google Docs (Case 3)**
- ❌ **Different content extracted**: Script finds different links than operator expects
- ⚠️ **Playlist ID lengths**: James Kirton - operator expects short IDs, script extracts full IDs
- ❌ **Wrong links**: John Williams, James Yu - completely different videos/playlists found

### 3. **Extra Links**
- Script includes infrastructure/auth links (e.g., accounts.google.com)
- Script includes duplicate links with different parameters (?usp=sharing)
- Caroline Chiu and Kiko have unexpected Drive files

### 4. **Missing Data**
- Shelesea Evans not found in output (possible name variation or row mismatch)

## Conclusion

**No, the script does NOT produce the same results as the operator data.**

Main issues:
1. **Different extraction logic**: Script extracts ALL links from docs, operator expects specific curated links
2. **ID format differences**: Playlist IDs have different lengths, video URLs have format variations
3. **Extra noise**: Script includes auth/infrastructure links that operator filters out
4. **Content mismatch**: For several docs, completely different videos are found than expected

The 33.3% success rate indicates the script needs significant adjustments to match operator expectations.