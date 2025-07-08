#!/usr/bin/env python3
"""
Validate if YouTube links are real/genuine by checking if they exist
"""
import requests
import re
from typing import Dict, List, Tuple

def validate_youtube_url(video_id: str) -> Tuple[bool, str]:
    """
    Check if a YouTube video ID is valid by trying to access it.
    Returns (is_valid, title_or_error)
    """
    urls_to_try = [
        f"https://www.youtube.com/watch?v={video_id}",
        f"https://youtu.be/{video_id}"
    ]
    
    for url in urls_to_try:
        try:
            # YouTube returns 200 even for non-existent videos, so check content
            response = requests.get(url, timeout=10)
            
            if response.status_code == 200:
                # Check if it's a valid video page
                content = response.text
                
                # Look for signs of a valid video
                if '"playabilityStatus":' in content:
                    # Extract video title if possible
                    title_match = re.search(r'"title":\s*"([^"]+)"', content)
                    if title_match:
                        title = title_match.group(1)
                        return True, f"Valid - Title: {title[:50]}..."
                    
                    # Check if video is unavailable
                    if '"status":"ERROR"' in content or '"status":"UNPLAYABLE"' in content:
                        return False, "Video unavailable or deleted"
                    
                    return True, "Valid video (title extraction failed)"
                else:
                    return False, "Not a valid video page"
            else:
                return False, f"HTTP {response.status_code}"
                
        except Exception as e:
            return False, f"Error: {str(e)}"
    
    return False, "Could not validate"

def main():
    """Validate all YouTube IDs from operator vs extracted data"""
    
    print("=== YOUTUBE LINK VALIDATION ===\n")
    
    # Define the YouTube IDs to check
    test_cases = {
        "James Kirton - Operator Data": [
            "vvPK5D7rZvs",
            "1aQoJb43d1g"
        ],
        "James Kirton - Our Extraction": [
            "3zCkiF_o7zw",
            "0mqY6-vhPhY",
            "Zhihkc1AgGo",
            "zwh48UtZQAg"
        ],
        "John Williams - Operator Data": [
            "K6kBTbjH4cI",
            "vHD2wDyrWLw", 
            "BlSxvQ9p8Q0",
            "ZBuf3DGBuM"
        ],
        "John Williams - Our Extraction": [
            "BtSNvQ9Rc90",
            "ZBuff3DGbUM",
            "vHD2wDyrWLw"
        ],
        "Olivia Tomlinson - Operator Data": [
            "NwS2ncgtkoc",
            "8zo0I4-F3Bs",
            "Dnmff9nv1b4",
            "2iwahDWerSQ"
        ]
    }
    
    # Validate each set
    for category, video_ids in test_cases.items():
        print(f"\n{category}:")
        print("-" * 60)
        
        for video_id in video_ids:
            is_valid, message = validate_youtube_url(video_id)
            status = "✅" if is_valid else "❌"
            print(f"{status} {video_id}: {message}")
    
    print("\n\nADDITIONAL VALIDATION METHODS:")
    print("-" * 60)
    print("""
    1. YOUTUBE OEMBED API (No Auth Required):
       curl "https://www.youtube.com/oembed?url=https://youtube.com/watch?v=VIDEO_ID&format=json"
       
    2. YOUTUBE THUMBNAIL CHECK:
       https://img.youtube.com/vi/VIDEO_ID/default.jpg
       - Returns 404 if video doesn't exist
       - Returns image if video exists
       
    3. MANUAL VERIFICATION:
       - Open each link in a browser
       - Check if video loads and plays
       - Note the video title and uploader
       
    4. PATTERN ANALYSIS:
       - All YouTube IDs are exactly 11 characters
       - Use characters: a-z, A-Z, 0-9, _, -
       - Our extracted IDs follow this pattern ✓
    """)
    
    print("\nCONCLUSION:")
    print("-" * 60)
    print("""
    If both operator and extracted links are valid:
    → They're looking at DIFFERENT CONTENT
    → Operator may have manually checked docs
    → We're extracting from HTML metadata
    
    If operator links are invalid:
    → Operator data may be outdated
    → Videos may have been deleted
    → Transcription error in operator data
    """)

if __name__ == "__main__":
    main()