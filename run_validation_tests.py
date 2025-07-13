#!/usr/bin/env python3
"""
Run validation tests directly
"""

import sys
import os
sys.path.insert(0, '/home/Mike/projects/xenodex/typing-clients-ingestion-minimal')

# Import and run the tests
from utils.validation import is_valid_youtube_url, is_valid_drive_url, get_url_type

def run_tests():
    """Run all validation tests"""
    print("Running validation function tests...\n")
    
    # Test is_valid_youtube_url
    print("Testing is_valid_youtube_url:")
    
    # Valid YouTube URLs
    valid_urls = [
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        "https://youtube.com/watch?v=dQw4w9WgXcQ",
        "https://youtu.be/dQw4w9WgXcQ",
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ&t=10s",
        "https://youtu.be/dQw4w9WgXcQ?t=10",
    ]
    
    for url in valid_urls:
        result = is_valid_youtube_url(url)
        print(f"  ✓ {url} -> {result}")
        assert result == True, f"Expected True for {url}"
    
    # Invalid YouTube URLs
    invalid_urls = [
        "",
        "https://google.com",
        "https://youtube.com/watch?v=invalid",
        "https://youtube.com/watch?v=toolong123456789",
        "javascript:alert('xss')",
        "https://youtube.com/watch?v=dQw4w9WgX",  # Too short
    ]
    
    for url in invalid_urls:
        result = is_valid_youtube_url(url)
        print(f"  ✗ {url} -> {result}")
        assert result == False, f"Expected False for {url}"
    
    print("  ✓ All is_valid_youtube_url tests passed!")
    
    # Test is_valid_drive_url
    print("\nTesting is_valid_drive_url:")
    
    # Valid Drive URLs
    valid_urls = [
        "https://drive.google.com/file/d/1abc123def456/view",
        "https://docs.google.com/document/d/1abc123def456/edit",
        "https://drive.google.com/file/d/1u4vS-_WKpW-RHCpIS4hls6K5grOii56F/view",
        "https://drive.google.com/drive/folders/1abc123def456",
    ]
    
    for url in valid_urls:
        result = is_valid_drive_url(url)
        print(f"  ✓ {url} -> {result}")
        assert result == True, f"Expected True for {url}"
    
    # Invalid Drive URLs
    invalid_urls = [
        "",
        "https://google.com",
        "https://youtube.com/watch?v=dQw4w9WgXcQ",
        "javascript:alert('xss')",
        "https://drive.google.com/file/d/",  # Missing ID
    ]
    
    for url in invalid_urls:
        result = is_valid_drive_url(url)
        print(f"  ✗ {url} -> {result}")
        assert result == False, f"Expected False for {url}"
    
    print("  ✓ All is_valid_drive_url tests passed!")
    
    # Test get_url_type
    print("\nTesting get_url_type:")
    
    # Test cases with expected results
    test_cases = [
        ("https://www.youtube.com/watch?v=dQw4w9WgXcQ", "youtube"),
        ("https://youtu.be/dQw4w9WgXcQ", "youtube"),
        ("https://drive.google.com/file/d/1abc123def456/view", "drive_file"),
        ("https://docs.google.com/document/d/1abc123def456/edit", "drive_file"),
        ("https://drive.google.com/drive/folders/1abc123def456", "drive_folder"),
        ("https://google.com", "unknown"),
        ("", "unknown"),
        ("javascript:alert('xss')", "unknown"),
    ]
    
    for url, expected in test_cases:
        result = get_url_type(url)
        print(f"  {url} -> {result} (expected: {expected})")
        assert result == expected, f"Expected {expected} for {url}, got {result}"
    
    print("  ✓ All get_url_type tests passed!")
    
    print("\n🎉 All tests passed successfully!")

if __name__ == "__main__":
    run_tests()