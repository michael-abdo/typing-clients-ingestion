#!/usr/bin/env python3
"""
GOOGLE DOCS EXTRACTION DIAGNOSTIC TOOL
Analyzes current extraction failures with detailed logging and DOM inspection
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from bs4 import BeautifulSoup
import time
import re
import json

def get_diagnostic_driver():
    """Get Selenium driver with diagnostic capabilities"""
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    
    try:
        driver = webdriver.Chrome(options=options)
        return driver
    except Exception as e:
        print(f"Failed to initialize Chrome driver: {e}")
        return None

def diagnostic_extract_google_doc(url, detailed_logging=True):
    """Extract Google Doc with comprehensive diagnostic logging"""
    print(f"🔍 DIAGNOSTIC EXTRACTION: {url}")
    print("=" * 80)
    
    driver = get_diagnostic_driver()
    if not driver:
        return None
    
    try:
        # Step 1: Load page with timing
        print("📥 STEP 1: Loading page...")
        start_time = time.time()
        driver.get(url)
        
        # Wait for body
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        load_time = time.time() - start_time
        print(f"   ✓ Page loaded in {load_time:.2f} seconds")
        
        # Step 2: Initial page analysis
        print("\n📊 STEP 2: Initial page analysis...")
        page_title = driver.title
        current_url = driver.current_url
        print(f"   Title: {page_title}")
        print(f"   Final URL: {current_url}")
        
        # Check for access issues
        if "AccessDenied" in page_title or "Sign in" in page_title:
            print("   ⚠️  ACCESS ISSUE DETECTED")
            return None
        
        # Step 3: Wait and analyze content loading
        print("\n⏱️  STEP 3: Content loading analysis...")
        
        # Progressive content checks
        content_checks = []
        for wait_time in [1, 3, 5, 10, 15]:
            time.sleep(wait_time - (content_checks[-1][0] if content_checks else 0))
            
            html_size = len(driver.page_source)
            body_text_length = len(driver.find_element(By.TAG_NAME, "body").text)
            
            content_checks.append((wait_time, html_size, body_text_length))
            print(f"   At {wait_time}s: HTML={html_size:,} chars, Body text={body_text_length:,} chars")
        
        # Analyze content stability
        last_check = content_checks[-1]
        second_last = content_checks[-2] if len(content_checks) > 1 else content_checks[-1]
        
        html_stable = abs(last_check[1] - second_last[1]) < 1000
        text_stable = abs(last_check[2] - second_last[2]) < 100
        
        print(f"   Content stability: HTML={'✓' if html_stable else '✗'}, Text={'✓' if text_stable else '✗'}")
        
        # Step 4: Scrolling analysis
        print("\n📜 STEP 4: Scrolling and lazy loading...")
        
        initial_height = driver.execute_script("return document.body.scrollHeight")
        print(f"   Initial height: {initial_height}px")
        
        # Perform scrolling
        scroll_positions = []
        for i in range(0, initial_height, 300):
            driver.execute_script(f"window.scrollTo(0, {i});")
            time.sleep(0.1)
            current_height = driver.execute_script("return document.body.scrollHeight")
            scroll_positions.append((i, current_height))
        
        final_height = scroll_positions[-1][1]
        height_changed = final_height != initial_height
        print(f"   Final height: {final_height}px (changed: {'✓' if height_changed else '✗'})")
        
        # Scroll back to top
        driver.execute_script("window.scrollTo(0, 0);")
        time.sleep(1)
        
        # Step 5: DOM structure analysis
        print("\n🏗️  STEP 5: DOM structure analysis...")
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # Test current selectors
        current_selectors = [
            '.kix-pagesettings-protected-text',
            '.kix-page',
            '.doc-content',
            '.google-docs-content'
        ]
        
        selector_results = {}
        for selector in current_selectors:
            elements = soup.select(selector)
            if elements:
                total_text = ' '.join([elem.get_text(separator=' ', strip=True) for elem in elements])
                selector_results[selector] = {
                    'found': True,
                    'element_count': len(elements),
                    'text_length': len(total_text),
                    'sample_text': total_text[:200] + "..." if len(total_text) > 200 else total_text
                }
            else:
                selector_results[selector] = {'found': False}
        
        print("   Current selector analysis:")
        for selector, result in selector_results.items():
            if result['found']:
                print(f"     ✓ {selector}: {result['element_count']} elements, {result['text_length']} chars")
                if detailed_logging:
                    print(f"       Sample: {result['sample_text']}")
            else:
                print(f"     ✗ {selector}: No elements found")
        
        # Step 6: Discover active selectors
        print("\n🔍 STEP 6: Discovering active content selectors...")
        
        # Look for Google Docs specific patterns
        potential_selectors = [
            # Current patterns
            '.kix-pagesettings-protected-text', '.kix-page', '.doc-content', '.google-docs-content',
            # Common Google Docs patterns
            '.kix-canvas-tile-content', '.kix-page-paginated', '.kix-page-content-wrap',
            '.kix-pagesettings-protected-text', '.kix-canvas-content', '.kix-canvas',
            # Generic content patterns
            '[role="document"]', '.document-content', '#contents', '.content',
            # Backup patterns
            'div[style*="font-family"]', 'div[style*="font-size"]'
        ]
        
        discovered_selectors = {}
        for selector in potential_selectors:
            try:
                elements = soup.select(selector)
                if elements:
                    total_text = ' '.join([elem.get_text(separator=' ', strip=True) for elem in elements])
                    if len(total_text) > 100:  # Only meaningful content
                        discovered_selectors[selector] = {
                            'element_count': len(elements),
                            'text_length': len(total_text),
                            'sample_text': total_text[:200] + "..." if len(total_text) > 200 else total_text
                        }
            except Exception:
                continue
        
        print(f"   Found {len(discovered_selectors)} potentially useful selectors:")
        for selector, info in sorted(discovered_selectors.items(), key=lambda x: x[1]['text_length'], reverse=True)[:5]:
            print(f"     📍 {selector}: {info['element_count']} elements, {info['text_length']} chars")
            if detailed_logging:
                print(f"       Sample: {info['sample_text']}")
        
        # Step 7: Link extraction analysis
        print("\n🔗 STEP 7: Link extraction analysis...")
        
        # Extract all links
        all_links = soup.find_all('a', href=True)
        print(f"   Total anchor tags found: {len(all_links)}")
        
        # Categorize links
        youtube_links = []
        drive_links = []
        other_links = []
        
        for link in all_links:
            href = link.get('href', '')
            if 'youtube.com' in href or 'youtu.be' in href:
                youtube_links.append(href)
            elif 'drive.google.com' in href:
                drive_links.append(href)
            elif href.startswith('http'):
                other_links.append(href)
        
        print(f"   YouTube links found: {len(youtube_links)}")
        for yt_link in youtube_links[:3]:
            print(f"     📺 {yt_link}")
        
        print(f"   Drive links found: {len(drive_links)}")
        for drive_link in drive_links[:3]:
            print(f"     💾 {drive_link}")
        
        print(f"   Other HTTP links: {len(other_links)}")
        
        # Step 8: Text content analysis for embedded URLs
        print("\n📝 STEP 8: Text content analysis...")
        
        body_text = soup.get_text()
        
        # Look for URLs in text that might not be linked
        url_pattern = r'https?://[^\s<>"{}\\|^`\[\]]+[^\s<>"{}\\|^`\[\].,;:!?)]'
        text_urls = re.findall(url_pattern, body_text)
        
        text_youtube = [url for url in text_urls if 'youtube.com' in url or 'youtu.be' in url]
        text_drive = [url for url in text_urls if 'drive.google.com' in url]
        
        print(f"   URLs in text (non-linked): {len(text_urls)}")
        print(f"   YouTube URLs in text: {len(text_youtube)}")
        print(f"   Drive URLs in text: {len(text_drive)}")
        
        # Step 9: Summary and recommendations
        print("\n📋 STEP 9: Summary and recommendations...")
        
        working_selectors = [s for s, r in selector_results.items() if r.get('found', False)]
        best_discovered = max(discovered_selectors.items(), key=lambda x: x[1]['text_length']) if discovered_selectors else None
        
        total_meaningful_links = len(youtube_links) + len(drive_links) + len(text_youtube) + len(text_drive)
        
        print(f"   ✓ Working current selectors: {len(working_selectors)}")
        print(f"   ✓ Best discovered selector: {best_discovered[0] if best_discovered else 'None'}")
        print(f"   ✓ Total meaningful links found: {total_meaningful_links}")
        print(f"   ✓ Content stability: {'Good' if html_stable and text_stable else 'Unstable'}")
        
        # Return comprehensive diagnostic data
        return {
            'url': url,
            'load_time': load_time,
            'page_title': page_title,
            'final_url': current_url,
            'content_checks': content_checks,
            'content_stable': html_stable and text_stable,
            'height_changed': height_changed,
            'selector_results': selector_results,
            'discovered_selectors': discovered_selectors,
            'youtube_links': youtube_links,
            'drive_links': drive_links,
            'text_youtube': text_youtube,
            'text_drive': text_drive,
            'total_links': total_meaningful_links,
            'recommendations': {
                'best_selector': best_discovered[0] if best_discovered else None,
                'needs_longer_wait': not (html_stable and text_stable),
                'needs_better_selectors': len(working_selectors) == 0,
                'has_meaningful_content': total_meaningful_links > 0
            }
        }
        
    except Exception as e:
        print(f"✗ Diagnostic extraction failed: {e}")
        return None
    finally:
        driver.quit()

def main():
    """Run diagnostic on known problematic documents"""
    # Test URLs from the actual data
    test_urls = [
        # Caroline Chiu (Row 497) - has Drive link but missing YouTube content
        "https://docs.google.com/document/d/1Ael_iSce9tO3SECHp5X5N0yYgahDnnCY837aYsC21PE/edit?tab=t.0",
        # James Kirton (Row 496) - has incomplete YouTube playlist URL
        "https://docs.google.com/document/d/106UzVKTBceNnihO711snD-AeJOK9-fki9EINggkLQU8/edit?tab=t.0"
    ]
    
    results = []
    
    for i, url in enumerate(test_urls, 1):
        print(f"\n{'='*100}")
        print(f"DIAGNOSTIC TEST {i}/{len(test_urls)}")
        print(f"{'='*100}")
        
        result = diagnostic_extract_google_doc(url)
        if result:
            results.append(result)
            
            # Save individual results
            with open(f'diagnostic_result_{i}.json', 'w') as f:
                json.dump(result, f, indent=2)
        
        print(f"\n✓ Diagnostic {i} complete")
        
        if i < len(test_urls):
            print("Waiting 5 seconds before next test...")
            time.sleep(5)
    
    # Save combined results
    with open('extraction_diagnostic_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n🎉 DIAGNOSTIC COMPLETE")
    print(f"Results saved to extraction_diagnostic_results.json")
    print(f"Individual results: diagnostic_result_1.json, diagnostic_result_2.json")

if __name__ == "__main__":
    main()