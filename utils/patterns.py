#!/usr/bin/env python3
"""
Centralized Regex Pattern Registry (DRY)
Consolidates scattered regex patterns throughout the codebase for consistency
"""

import re
from typing import Pattern, Dict


class PatternRegistry:
    """Central registry for all regex patterns used across the application"""
    
    # URL Patterns
    YOUTUBE_VIDEO_ID = re.compile(r'[a-zA-Z0-9_-]{11}')
    YOUTUBE_VIDEO_URL = re.compile(r'https?://(?:www\.)?youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})')
    YOUTUBE_SHORT_URL = re.compile(r'https?://youtu\.be/([a-zA-Z0-9_-]{11})')
    YOUTUBE_PLAYLIST_URL = re.compile(r'https?://(?:www\.)?youtube\.com/playlist\?list=([a-zA-Z0-9_-]+)')
    
    # Google Drive Patterns
    DRIVE_FILE_ID = re.compile(r'[a-zA-Z0-9_-]{25,}')
    DRIVE_FILE_URL = re.compile(r'https://drive\.google\.com/file/d/([a-zA-Z0-9_-]+)')
    DRIVE_OPEN_URL = re.compile(r'https://drive\.google\.com/open\?id=([a-zA-Z0-9_-]+)')
    DRIVE_FOLDER_URL = re.compile(r'https://drive\.google\.com/drive/folders/([a-zA-Z0-9_-]+)')
    
    # Enhanced URL extraction patterns
    YOUTUBE_VIDEO_FULL = re.compile(r'https?://(?:www\.)?youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})[^\s<>"]*')
    YOUTUBE_SHORT_FULL = re.compile(r'https?://youtu\.be/([a-zA-Z0-9_-]{11})[^\s<>"]*')
    YOUTUBE_PLAYLIST_FULL = re.compile(r'https?://(?:www\.)?youtube\.com/playlist\?list=([a-zA-Z0-9_-]+)[^\s<>"]*')
    DRIVE_FILE_FULL = re.compile(r'https://drive\.google\.com/file/d/([a-zA-Z0-9_-]+)[^\s<>"]*')
    DRIVE_OPEN_FULL = re.compile(r'https://drive\.google\.com/open\?id=([a-zA-Z0-9_-]+)[^\s<>"]*')
    DRIVE_FOLDER_FULL = re.compile(r'https://drive\.google\.com/drive/folders/([a-zA-Z0-9_-]+)[^\s<>"]*')
    
    # Generic URL patterns  
    HTTP_URL = re.compile(r'https?://[^\s<>"{}\\|^\[\]`]+[^\s<>"{}\\|^\[\]`.,;:!?\)\]]')
    
    # Text cleaning patterns
    WHITESPACE_CLEANUP = re.compile(r'\s+')
    URL_TRAILING_PUNCTUATION = re.compile(r'[.,;:!?\)\]\}"\'>]+$')
    
    # YouTube parameter extraction
    YOUTUBE_VIDEO_PARAM = re.compile(r'v=([a-zA-Z0-9_-]{11})')
    YOUTUBE_LIST_PARAM = re.compile(r'[&?]list=([a-zA-Z0-9_-]+)')


# Pattern groups for related operations
YOUTUBE_PATTERNS = {
    'video': PatternRegistry.YOUTUBE_VIDEO_URL,
    'short': PatternRegistry.YOUTUBE_SHORT_URL,
    'playlist': PatternRegistry.YOUTUBE_PLAYLIST_URL,
    'video_full': PatternRegistry.YOUTUBE_VIDEO_FULL,
    'short_full': PatternRegistry.YOUTUBE_SHORT_FULL,
    'playlist_full': PatternRegistry.YOUTUBE_PLAYLIST_FULL,
}

DRIVE_PATTERNS = {
    'file': PatternRegistry.DRIVE_FILE_URL,
    'open': PatternRegistry.DRIVE_OPEN_URL,
    'folder': PatternRegistry.DRIVE_FOLDER_URL,
    'file_full': PatternRegistry.DRIVE_FILE_FULL,
    'open_full': PatternRegistry.DRIVE_OPEN_FULL,
    'folder_full': PatternRegistry.DRIVE_FOLDER_FULL,
}

CLEANING_PATTERNS = {
    'whitespace': PatternRegistry.WHITESPACE_CLEANUP,
    'trailing_punctuation': PatternRegistry.URL_TRAILING_PUNCTUATION,
}


# Convenience functions for common operations
def extract_youtube_id(url: str) -> str:
    """Extract YouTube video ID from URL"""
    for pattern in [PatternRegistry.YOUTUBE_VIDEO_URL, PatternRegistry.YOUTUBE_SHORT_URL]:
        match = pattern.search(url)
        if match:
            return match.group(1)
    return ""


def extract_drive_id(url: str) -> str:
    """Extract Google Drive file ID from URL"""
    for pattern in [PatternRegistry.DRIVE_FILE_URL, PatternRegistry.DRIVE_OPEN_URL]:
        match = pattern.search(url)
        if match:
            return match.group(1)
    return ""


def clean_url(url: str) -> str:
    """Clean URL using centralized patterns"""
    if not url:
        return url
    
    # Remove trailing punctuation
    url = PatternRegistry.URL_TRAILING_PUNCTUATION.sub('', url)
    
    return url.strip()


def normalize_whitespace(text: str) -> str:
    """Normalize whitespace using centralized pattern"""
    return PatternRegistry.WHITESPACE_CLEANUP.sub(' ', text).strip()


def is_youtube_url(url: str) -> bool:
    """Check if URL is a YouTube URL"""
    return any(pattern.search(url) for pattern in YOUTUBE_PATTERNS.values())


def is_drive_url(url: str) -> bool:
    """Check if URL is a Google Drive URL"""
    return any(pattern.search(url) for pattern in DRIVE_PATTERNS.values())


# Example usage
if __name__ == "__main__":
    # Test pattern registry
    test_urls = [
        "https://www.youtube.com/watch?v=dQw4w9WgXcQ",
        "https://youtu.be/dQw4w9WgXcQ",
        "https://drive.google.com/file/d/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms/view",
        "https://drive.google.com/drive/folders/1BxiMVs0XRA5nFMdKvBdBZjgmUUqptlbs74OgvE2upms"
    ]
    
    for url in test_urls:
        print(f"URL: {url}")
        print(f"  YouTube ID: {extract_youtube_id(url)}")
        print(f"  Drive ID: {extract_drive_id(url)}")
        print(f"  Is YouTube: {is_youtube_url(url)}")
        print(f"  Is Drive: {is_drive_url(url)}")
        print()