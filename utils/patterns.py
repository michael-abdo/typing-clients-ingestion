#!/usr/bin/env python3
"""
Centralized Regex Pattern Registry (DRY)
Consolidates scattered regex patterns throughout the codebase for consistency
Also includes Selenium helpers for consistent web automation
"""

import re
import time
from typing import Pattern, Dict
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By


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


# Selenium helper functions (DRY)
def get_chrome_options() -> Options:
    """Get standardized Chrome options for Selenium WebDriver (DRY)"""
    chrome_options = Options()
    chrome_options.add_argument("--headless=new")  # Use new headless mode
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--no-sandbox")
    
    # Additional options to fix Chrome crashes
    chrome_options.add_argument("--disable-setuid-sandbox")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-plugins")
    chrome_options.add_argument("--disable-images")
    chrome_options.add_argument("--disable-blink-features=AutomationControlled")
    
    # Use temporary directory in /tmp for Chrome user data
    import tempfile
    import os
    temp_dir = tempfile.mkdtemp(prefix="chrome_temp_", dir="/tmp")
    chrome_options.add_argument(f"--user-data-dir={temp_dir}")
    chrome_options.add_argument(f"--crash-dumps-dir={temp_dir}")
    
    # Try to find Chrome binary
    chrome_paths = [
        "/usr/bin/google-chrome",
        "/usr/bin/google-chrome-stable",
        "/usr/bin/chromium",
        "/usr/bin/chromium-browser",
        "/snap/bin/chromium",
        "/usr/local/bin/google-chrome"
    ]
    
    for path in chrome_paths:
        if os.path.exists(path):
            chrome_options.binary_location = path
            break
    
    # Additional stability options
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging", "enable-automation"])
    chrome_options.add_experimental_option("useAutomationExtension", False)
    chrome_options.add_experimental_option('prefs', {
        'profile.default_content_setting_values.notifications': 2,
        'profile.default_content_settings.popups': 0
    })
    
    return chrome_options


def wait_and_scroll_page(driver, wait_timeout: int = 30, scroll_delay: float = 0.1) -> None:
    """Wait for page load and scroll to ensure all content is loaded (DRY)
    
    Args:
        driver: Selenium WebDriver instance
        wait_timeout: Timeout in seconds for page load
        scroll_delay: Delay in seconds between scroll steps
    """
    # Wait for page to load
    WebDriverWait(driver, wait_timeout).until(
        EC.presence_of_element_located((By.TAG_NAME, "body"))
    )
    
    # Extra wait for dynamic content (e.g., Google Docs)
    time.sleep(3)
    
    # Scroll to ensure all content is loaded
    height = driver.execute_script("return document.body.scrollHeight")
    for i in range(0, height, 300):
        driver.execute_script(f"window.scrollTo(0, {i});")
        time.sleep(scroll_delay)
    
    # Scroll back to top
    driver.execute_script("window.scrollTo(0, 0);")
    time.sleep(1)


# Global selenium driver with enhanced management
import atexit
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.service import Service

# Initialize logger and error handling
try:
    from logging_config import get_logger
    from error_handling import with_standard_error_handling
    logger = get_logger(__name__)
except ImportError:
    try:
        from .logging_config import get_logger
        from .error_handling import with_standard_error_handling
        logger = get_logger(__name__)
    except ImportError:
        import logging
        logger = logging.getLogger(__name__)
        # Fallback decorator if error_handling not available
        def with_standard_error_handling(operation_name, return_on_error):
            def decorator(func):
                return func
            return decorator

_driver = None

@with_standard_error_handling("Selenium driver initialization", None)
def get_selenium_driver():
    """Get initialized Selenium WebDriver with standardized options and enhanced error handling (DRY)"""
    global _driver
    if _driver is None:
        logger.info("Initializing Selenium Chrome driver...")
        chrome_options = get_chrome_options()
        
        try:
            _driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=chrome_options)
        except Exception as e:
            logger.error(f"Error initializing Selenium driver: {str(e)}")
            # Try fallback method - requires Chrome driver to be installed manually
            try:
                _driver = webdriver.Chrome(options=chrome_options)
            except Exception as e:
                logger.error(f"Could not initialize Selenium driver: {str(e)}")
                return None
    
    # Ensure driver is still alive
    try:
        _driver.title  # Simple check to see if driver is responsive
    except Exception:
        logger.warning("Driver was closed, reinitializing...")
        _driver = None
        return get_selenium_driver()
    
    return _driver

@with_standard_error_handling("Selenium driver cleanup", None)
def cleanup_selenium_driver():
    """Cleanup global Selenium driver with enhanced error handling (DRY)"""
    global _driver
    if _driver is not None:
        try:
            _driver.quit()
            _driver = None
            logger.info("Selenium driver cleaned up successfully")
        except Exception as e:
            logger.error(f"Error cleaning up Selenium driver: {e}")

# Register cleanup function to run on exit
atexit.register(cleanup_selenium_driver)


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