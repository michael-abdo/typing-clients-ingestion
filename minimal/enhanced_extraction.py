#!/usr/bin/env python3
"""
ENHANCED GOOGLE DOCS EXTRACTION
Implements dynamic wait strategy with content verification and JavaScript-based extraction
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
from bs4 import BeautifulSoup
import time
import re
import json

def get_enhanced_driver():
    """Get Selenium driver optimized for Google Docs extraction"""
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    # Additional options for better Google Docs compatibility
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    try:
        driver = webdriver.Chrome(options=options)
        # Hide automation indicators
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        return driver
    except Exception as e:
        print(f"Failed to initialize Chrome driver: {e}")
        return None

def dynamic_wait_for_content(driver, max_wait_time=60):
    """
    Dynamic wait strategy that waits for content to actually load and stabilize
    Returns True if content is ready, False if timeout
    """
    print("🔄 Using dynamic wait strategy...")
    
    start_time = time.time()
    previous_content_length = 0
    stable_checks = 0
    required_stable_checks = 3
    
    while time.time() - start_time < max_wait_time:
        try:
            # Check content stability using JavaScript
            current_content_length = driver.execute_script("""
                var content = '';
                
                // Method 1: Get all text content
                content += document.body.innerText || '';
                
                // Method 2: Check for contenteditable areas
                var editables = document.querySelectorAll('[contenteditable="true"]');
                for (var i = 0; i < editables.length; i++) {
                    content += editables[i].innerText || '';
                }
                
                // Method 3: Check for document areas
                var docElements = document.querySelectorAll('[role="document"], [role="textbox"]');
                for (var i = 0; i < docElements.length; i++) {
                    content += docElements[i].innerText || '';
                }
                
                return content.length;
            """)
            
            if current_content_length > previous_content_length:
                # Content is still loading
                print(f"   Content growing: {current_content_length} chars (+{current_content_length - previous_content_length})")
                previous_content_length = current_content_length
                stable_checks = 0
                time.sleep(2)
            elif current_content_length == previous_content_length and current_content_length > 100:
                # Content appears stable
                stable_checks += 1
                print(f"   Content stable: {current_content_length} chars (check {stable_checks}/{required_stable_checks})")
                
                if stable_checks >= required_stable_checks:
                    print(f"   ✅ Content stabilized at {current_content_length} chars")
                    return True
                
                time.sleep(1)
            else:
                # Very little content, keep waiting
                time.sleep(2)
                
        except Exception as e:
            print(f"   Warning: Dynamic wait check failed: {e}")
            time.sleep(2)
    
    print(f"   ⚠️ Timeout after {max_wait_time}s (final content: {previous_content_length} chars)")
    return previous_content_length > 100  # Return True if we got some content

def extract_content_with_javascript(driver):
    """
    Extract document content using JavaScript methods that work with modern Google Docs
    """
    print("🔧 Extracting content with JavaScript...")
    
    try:
        # Comprehensive JavaScript extraction
        extraction_result = driver.execute_script("""
            var result = {
                content: '',
                links: [],
                errors: []
            };
            
            try {
                // Method 1: Extract from contenteditable areas (main document content)
                var editables = document.querySelectorAll('[contenteditable="true"]');
                console.log('Found ' + editables.length + ' contenteditable elements');
                
                for (var i = 0; i < editables.length; i++) {
                    var text = editables[i].innerText || editables[i].textContent || '';
                    if (text.length > 20) {  // Only meaningful content
                        result.content += text + '\\n';
                    }
                    
                    // Extract links from this area
                    var links = editables[i].querySelectorAll('a[href]');
                    for (var j = 0; j < links.length; j++) {
                        result.links.push(links[j].href);
                    }
                }
                
                // Method 2: Extract from document/textbox roles
                var docElements = document.querySelectorAll('[role="document"], [role="textbox"]');
                console.log('Found ' + docElements.length + ' document/textbox elements');
                
                for (var i = 0; i < docElements.length; i++) {
                    var text = docElements[i].innerText || docElements[i].textContent || '';
                    if (text.length > 20 && result.content.indexOf(text.substring(0, 50)) === -1) {
                        result.content += text + '\\n';
                    }
                    
                    // Extract links
                    var links = docElements[i].querySelectorAll('a[href]');
                    for (var j = 0; j < links.length; j++) {
                        if (result.links.indexOf(links[j].href) === -1) {
                            result.links.push(links[j].href);
                        }
                    }
                }
                
                // Method 3: Look for Google Docs specific content areas
                var kixElements = document.querySelectorAll('[class*="kix"]');
                console.log('Found ' + kixElements.length + ' kix elements');
                
                for (var i = 0; i < kixElements.length; i++) {
                    var text = kixElements[i].innerText || kixElements[i].textContent || '';
                    if (text.length > 50 && result.content.indexOf(text.substring(0, 50)) === -1) {
                        result.content += text + '\\n';
                    }
                    
                    // Extract links
                    var links = kixElements[i].querySelectorAll('a[href]');
                    for (var j = 0; j < links.length; j++) {
                        if (result.links.indexOf(links[j].href) === -1) {
                            result.links.push(links[j].href);
                        }
                    }
                }
                
                // Method 4: Try to access iframe content (if accessible)
                var iframes = document.querySelectorAll('iframe');
                console.log('Found ' + iframes.length + ' iframe elements');
                
                for (var i = 0; i < iframes.length; i++) {
                    try {
                        var iframeDoc = iframes[i].contentDocument || iframes[i].contentWindow.document;
                        if (iframeDoc && iframeDoc.body) {
                            var text = iframeDoc.body.innerText || iframeDoc.body.textContent || '';
                            if (text.length > 50 && result.content.indexOf(text.substring(0, 50)) === -1) {
                                result.content += text + '\\n';
                            }
                            
                            // Extract links from iframe
                            var links = iframeDoc.querySelectorAll('a[href]');
                            for (var j = 0; j < links.length; j++) {
                                if (result.links.indexOf(links[j].href) === -1) {
                                    result.links.push(links[j].href);
                                }
                            }
                        }
                    } catch (e) {
                        // Cross-origin iframe, skip
                        result.errors.push('Cross-origin iframe: ' + e.message);
                    }
                }
                
                // Method 5: Extract all links as fallback
                var allLinks = document.querySelectorAll('a[href]');
                console.log('Found ' + allLinks.length + ' total links');
                
                for (var i = 0; i < allLinks.length; i++) {
                    if (result.links.indexOf(allLinks[i].href) === -1) {
                        result.links.push(allLinks[i].href);
                    }
                }
                
                // Clean up content
                result.content = result.content.trim();
                
                console.log('Extraction complete: ' + result.content.length + ' chars, ' + result.links.length + ' links');
                
            } catch (e) {
                result.errors.push('JavaScript extraction error: ' + e.message);
            }
            
            return result;
        """)
        
        content = extraction_result.get('content', '')
        links = extraction_result.get('links', [])
        errors = extraction_result.get('errors', [])
        
        print(f"   ✅ JavaScript extraction:")
        print(f"      Content: {len(content)} characters")
        print(f"      Links: {len(links)} found")
        
        if errors:
            print(f"      Warnings: {len(errors)}")
            for error in errors[:3]:  # Show first 3 errors
                print(f"        - {error}")
        
        return content, links
        
    except Exception as e:
        print(f"   ❌ JavaScript extraction failed: {e}")
        return "", []

def extract_links_from_content(content, links_list):
    """
    Extract meaningful YouTube and Drive links from content and link lists
    """
    print("🔗 Analyzing extracted links...")
    
    # Combine content and links for comprehensive search
    all_text = content + " " + " ".join(links_list)
    
    extracted_links = {
        'youtube': [],
        'drive_files': [],
        'drive_folders': [],
        'all_links': []
    }
    
    # Enhanced patterns for better link extraction
    youtube_patterns = [
        r'https?://(?:www\.)?youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})[^\s<>"{}\\|^`\[\]]*',
        r'https?://youtu\.be/([a-zA-Z0-9_-]{11})[^\s<>"{}\\|^`\[\]]*',
        r'https?://(?:www\.)?youtube\.com/playlist\?list=([a-zA-Z0-9_-]+)[^\s<>"{}\\|^`\[\]]*'
    ]
    
    drive_patterns = [
        r'https?://drive\.google\.com/file/d/([a-zA-Z0-9_-]+)[^\s<>"{}\\|^`\[\]]*',
        r'https?://drive\.google\.com/open\?id=([a-zA-Z0-9_-]+)[^\s<>"{}\\|^`\[\]]*',
        r'https?://drive\.google\.com/drive/folders/([a-zA-Z0-9_-]+)[^\s<>"{}\\|^`\[\]]*'
    ]
    
    # Extract YouTube links
    for pattern in youtube_patterns:
        matches = re.finditer(pattern, all_text, re.IGNORECASE)
        for match in matches:
            full_url = match.group(0)
            if 'playlist' in pattern:
                clean_url = f'https://www.youtube.com/playlist?list={match.group(1)}'
            else:
                clean_url = f'https://www.youtube.com/watch?v={match.group(1)}'
            
            if clean_url not in extracted_links['youtube']:
                extracted_links['youtube'].append(clean_url)
                print(f"   🎥 YouTube: {clean_url}")
    
    # Extract Drive links
    for pattern in drive_patterns:
        matches = re.finditer(pattern, all_text, re.IGNORECASE)
        for match in matches:
            full_url = match.group(0)
            if 'folders' in pattern:
                clean_url = f'https://drive.google.com/drive/folders/{match.group(1)}'
                if clean_url not in extracted_links['drive_folders']:
                    extracted_links['drive_folders'].append(clean_url)
                    print(f"   📁 Drive Folder: {clean_url}")
            else:
                clean_url = f'https://drive.google.com/file/d/{match.group(1)}/view'
                if clean_url not in extracted_links['drive_files']:
                    extracted_links['drive_files'].append(clean_url)
                    print(f"   📄 Drive File: {clean_url}")
    
    # Extract all meaningful links
    all_link_pattern = r'https?://[^\s<>"{}\\|^`\[\]]+[^\s<>"{}\\|^`\[\].,;:!?\)\]]'
    all_found_links = re.findall(all_link_pattern, all_text, re.IGNORECASE)
    
    for link in all_found_links:
        # Filter out Google infrastructure links
        if not any(infra in link for infra in [
            'gstatic.com', 'apis.google.com', 'accounts.google.com',
            'script.google.com', 'ssl.gstatic.com', 'docs.google.com/static',
            'myaccount.google.com', 'workspace.google.com', 'clients6.google.com'
        ]):
            if link not in extracted_links['all_links']:
                extracted_links['all_links'].append(link)
    
    print(f"   📊 Summary:")
    print(f"      YouTube links: {len(extracted_links['youtube'])}")
    print(f"      Drive files: {len(extracted_links['drive_files'])}")
    print(f"      Drive folders: {len(extracted_links['drive_folders'])}")
    print(f"      Total meaningful links: {len(extracted_links['all_links'])}")
    
    return extracted_links

def enhanced_google_doc_extraction(url, max_wait_time=60):
    """
    Enhanced Google Doc extraction with dynamic waiting and JavaScript-based content access
    """
    print(f"🚀 ENHANCED EXTRACTION: {url}")
    print("=" * 80)
    
    driver = get_enhanced_driver()
    if not driver:
        return None
    
    try:
        # Step 1: Load the document
        print("📥 Loading document...")
        start_time = time.time()
        driver.get(url)
        
        # Wait for basic page load
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        load_time = time.time() - start_time
        print(f"   ✅ Page loaded in {load_time:.2f} seconds")
        
        # Step 2: Dynamic wait for content
        content_ready = dynamic_wait_for_content(driver, max_wait_time)
        
        if not content_ready:
            print("   ⚠️ Content may not be fully loaded, proceeding anyway...")
        
        # Step 3: Extract content with JavaScript
        content, links = extract_content_with_javascript(driver)
        
        # Step 4: Extract meaningful links
        extracted_links = extract_links_from_content(content, links)
        
        # Step 5: Verify extraction quality
        total_meaningful_links = (len(extracted_links['youtube']) + 
                                len(extracted_links['drive_files']) + 
                                len(extracted_links['drive_folders']))
        
        extraction_quality = {
            'content_length': len(content),
            'has_substantial_content': len(content) > 200,
            'meaningful_links_found': total_meaningful_links,
            'has_youtube': len(extracted_links['youtube']) > 0,
            'has_drive': (len(extracted_links['drive_files']) + len(extracted_links['drive_folders'])) > 0,
            'extraction_success': len(content) > 200 or total_meaningful_links > 0
        }
        
        print(f"\n📊 EXTRACTION QUALITY ASSESSMENT:")
        print(f"   Content length: {extraction_quality['content_length']} chars")
        print(f"   Substantial content: {'✅' if extraction_quality['has_substantial_content'] else '❌'}")
        print(f"   Meaningful links: {extraction_quality['meaningful_links_found']}")
        print(f"   Has YouTube: {'✅' if extraction_quality['has_youtube'] else '❌'}")
        print(f"   Has Drive: {'✅' if extraction_quality['has_drive'] else '❌'}")
        print(f"   Overall success: {'✅' if extraction_quality['extraction_success'] else '❌'}")
        
        return {
            'url': url,
            'load_time': load_time,
            'content': content,
            'extracted_links': extracted_links,
            'quality_assessment': extraction_quality,
            'success': extraction_quality['extraction_success']
        }
        
    except Exception as e:
        print(f"❌ Enhanced extraction failed: {e}")
        return None
    finally:
        driver.quit()

def main():
    """Test enhanced extraction on problematic documents"""
    test_urls = [
        # Caroline Chiu - should find Drive link and any YouTube content
        "https://docs.google.com/document/d/1Ael_iSce9tO3SECHp5X5N0yYgahDnnCY837aYsC21PE/edit?tab=t.0",
        # James Kirton - should find complete YouTube playlist links
        "https://docs.google.com/document/d/106UzVKTBceNnihO711snD-AeJOK9-fki9EINggkLQU8/edit?tab=t.0"
    ]
    
    results = []
    
    for i, url in enumerate(test_urls, 1):
        print(f"\n{'='*100}")
        print(f"ENHANCED EXTRACTION TEST {i}/{len(test_urls)}")
        print(f"{'='*100}")
        
        result = enhanced_google_doc_extraction(url)
        if result:
            results.append(result)
            
            # Save individual results
            with open(f'enhanced_extraction_{i}.json', 'w') as f:
                json.dump(result, f, indent=2, default=str)
            
            print(f"\n✅ Test {i} completed successfully")
        else:
            print(f"\n❌ Test {i} failed")
        
        if i < len(test_urls):
            print("\nWaiting 5 seconds before next test...")
            time.sleep(5)
    
    # Save combined results
    with open('enhanced_extraction_results.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    # Summary
    print(f"\n🎉 ENHANCED EXTRACTION TESTING COMPLETE")
    print(f"Results saved to enhanced_extraction_results.json")
    
    successful_extractions = len([r for r in results if r.get('success', False)])
    print(f"Success rate: {successful_extractions}/{len(results)} ({100*successful_extractions/len(results) if results else 0:.0f}%)")

if __name__ == "__main__":
    main()