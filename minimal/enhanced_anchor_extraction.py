#!/usr/bin/env python3
"""
ENHANCED ANCHOR TAG EXTRACTION
Comprehensive extraction of hyperlinked URLs from Google Docs
Focuses on capturing complete href attributes from anchor tags
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import time
import re
import json

def get_selenium_driver():
    """Get Selenium driver optimized for Google Docs"""
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-gpu')
    options.add_argument('--window-size=1920,1080')
    options.add_argument('--disable-blink-features=AutomationControlled')
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option('useAutomationExtension', False)
    
    try:
        driver = webdriver.Chrome(options=options)
        driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
        return driver
    except Exception as e:
        print(f"Failed to initialize Chrome driver: {e}")
        return None

def extract_google_doc_text_with_anchors(url):
    """Enhanced extraction that prioritizes anchor tag href attributes"""
    driver = get_selenium_driver()
    if not driver:
        return "", {}
    
    try:
        print(f"🔗 Enhanced anchor extraction for: {url}")
        start_time = time.time()
        driver.get(url)
        
        # Wait for page load
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Dynamic wait for content stabilization
        print("  ⏳ Waiting for content to stabilize...")
        previous_anchor_count = 0
        stable_checks = 0
        max_wait = 30
        start_wait = time.time()
        
        while time.time() - start_wait < max_wait:
            try:
                current_anchor_count = driver.execute_script("""
                    return document.querySelectorAll('a[href]').length;
                """)
                
                if current_anchor_count > previous_anchor_count:
                    previous_anchor_count = current_anchor_count
                    stable_checks = 0
                    time.sleep(2)
                elif current_anchor_count == previous_anchor_count:
                    stable_checks += 1
                    if stable_checks >= 3:
                        print(f"  ✅ Content stabilized with {current_anchor_count} anchor tags")
                        break
                    time.sleep(1)
            except:
                time.sleep(2)
        
        # Comprehensive anchor extraction
        print("  🎯 Extracting all anchor tags and content...")
        extraction_result = driver.execute_script("""
            var result = {
                text_content: '',
                anchors: [],
                links: {
                    youtube: [],
                    drive_files: [],
                    drive_folders: [],
                    all_links: []
                }
            };
            
            // Extract text content first
            var contentMethods = [
                // Method 1: Contenteditable areas
                function() {
                    var text = '';
                    document.querySelectorAll('[contenteditable="true"]').forEach(el => {
                        text += (el.innerText || el.textContent || '') + '\\n';
                    });
                    return text;
                },
                // Method 2: Document/textbox roles
                function() {
                    var text = '';
                    document.querySelectorAll('[role="document"], [role="textbox"]').forEach(el => {
                        var elText = el.innerText || el.textContent || '';
                        if (text.indexOf(elText.substring(0, 50)) === -1) {
                            text += elText + '\\n';
                        }
                    });
                    return text;
                },
                // Method 3: Body text fallback
                function() {
                    return document.body.innerText || document.body.textContent || '';
                }
            ];
            
            // Try each method until we get content
            for (var i = 0; i < contentMethods.length; i++) {
                var content = contentMethods[i]();
                if (content && content.length > 100) {
                    result.text_content = content;
                    break;
                }
            }
            
            // Extract ALL anchor tags with multiple methods
            var anchorMethods = [
                // Method 1: Standard querySelectorAll
                function() {
                    return Array.from(document.querySelectorAll('a[href]'));
                },
                // Method 2: Walk through all elements
                function() {
                    var anchors = [];
                    var allElements = document.getElementsByTagName('a');
                    for (var i = 0; i < allElements.length; i++) {
                        if (allElements[i].href) {
                            anchors.push(allElements[i]);
                        }
                    }
                    return anchors;
                },
                // Method 3: Check contenteditable areas specifically
                function() {
                    var anchors = [];
                    document.querySelectorAll('[contenteditable="true"] a[href]').forEach(a => {
                        anchors.push(a);
                    });
                    return anchors;
                }
            ];
            
            var allAnchors = new Map();
            
            // Collect all unique anchors
            anchorMethods.forEach(method => {
                var anchors = method();
                anchors.forEach(anchor => {
                    var href = anchor.href || anchor.getAttribute('href') || '';
                    if (href && !allAnchors.has(href)) {
                        allAnchors.set(href, anchor);
                    }
                });
            });
            
            // Process each unique anchor
            allAnchors.forEach((anchor, href) => {
                var anchorData = {
                    href: href,
                    text: anchor.textContent || anchor.innerText || '',
                    title: anchor.title || '',
                    rel: anchor.rel || '',
                    target: anchor.target || '',
                    className: anchor.className || '',
                    outerHTML: anchor.outerHTML.substring(0, 500),
                    parent: anchor.parentElement ? anchor.parentElement.tagName : '',
                    computed_href: anchor.href // Computed absolute URL
                };
                
                result.anchors.push(anchorData);
                
                // Categorize links
                if (href.includes('youtube.com') || href.includes('youtu.be')) {
                    result.links.youtube.push(href);
                    console.log('Found YouTube link:', href);
                } else if (href.includes('drive.google.com')) {
                    if (href.includes('/folders/') || href.includes('/drive/folders/')) {
                        result.links.drive_folders.push(href);
                    } else {
                        result.links.drive_files.push(href);
                    }
                    console.log('Found Drive link:', href);
                }
                
                // Add to all links if not Google infrastructure
                var infrastructureDomains = [
                    'gstatic.com', 'apis.google.com', 'accounts.google.com',
                    'script.google.com', 'ssl.gstatic.com', 'docs.google.com/static'
                ];
                
                var isInfrastructure = infrastructureDomains.some(domain => href.includes(domain));
                if (!isInfrastructure && href.startsWith('http')) {
                    result.links.all_links.push(href);
                }
            });
            
            // Also extract plain text URLs that might not be anchors
            var urlRegex = /https?:\\/\\/[^\\s<>"{}\\\\|\\^\\[\\]`]+[^\\s<>"{}\\\\|\\^\\[\\]`.,;:!?\\)\\]]/gi;
            var textUrls = (result.text_content.match(urlRegex) || []);
            
            textUrls.forEach(url => {
                // Check if this URL is already in our anchor list
                var isAnchor = result.anchors.some(a => a.href === url);
                
                if (!isAnchor) {
                    // This is a plain text URL, not an anchor
                    if (url.includes('youtube.com') || url.includes('youtu.be')) {
                        if (!result.links.youtube.includes(url)) {
                            result.links.youtube.push(url);
                            console.log('Found plain text YouTube URL:', url);
                        }
                    } else if (url.includes('drive.google.com')) {
                        if (url.includes('/folders/')) {
                            if (!result.links.drive_folders.includes(url)) {
                                result.links.drive_folders.push(url);
                            }
                        } else {
                            if (!result.links.drive_files.includes(url)) {
                                result.links.drive_files.push(url);
                            }
                        }
                    }
                }
            });
            
            // Remove duplicates
            result.links.youtube = [...new Set(result.links.youtube)];
            result.links.drive_files = [...new Set(result.links.drive_files)];
            result.links.drive_folders = [...new Set(result.links.drive_folders)];
            result.links.all_links = [...new Set(result.links.all_links)];
            
            console.log('Extraction complete:', {
                anchors: result.anchors.length,
                youtube: result.links.youtube.length,
                drive: result.links.drive_files.length + result.links.drive_folders.length
            });
            
            return result;
        """)
        
        text_content = extraction_result.get('text_content', '')
        links = extraction_result.get('links', {})
        anchors = extraction_result.get('anchors', [])
        
        print(f"  ✅ Extraction complete in {time.time() - start_time:.2f} seconds")
        print(f"     Text content: {len(text_content)} characters")
        print(f"     Total anchors: {len(anchors)}")
        print(f"     YouTube links: {len(links.get('youtube', []))}")
        print(f"     Drive files: {len(links.get('drive_files', []))}")
        print(f"     Drive folders: {len(links.get('drive_folders', []))}")
        
        # Detailed anchor analysis
        if anchors:
            print("\n  📊 ANCHOR TAG DETAILS:")
            youtube_anchors = [a for a in anchors if 'youtube' in a['href'].lower() or 'youtu.be' in a['href'].lower()]
            if youtube_anchors:
                print(f"     YouTube anchors: {len(youtube_anchors)}")
                for anchor in youtube_anchors[:3]:  # Show first 3
                    print(f"       href: {anchor['href']}")
                    print(f"       text: {anchor['text'][:50]}...")
                    if 'playlist' in anchor['href']:
                        print(f"       🎯 PLAYLIST DETECTED")
        
        # Check for truncated playlist URLs
        if links.get('youtube'):
            print("\n  🔍 PLAYLIST URL ANALYSIS:")
            for url in links['youtube']:
                if 'playlist' in url:
                    if re.search(r'list=[a-zA-Z0-9_-]{10,}', url):
                        print(f"     ✅ Complete playlist: {url}")
                    else:
                        print(f"     ⚠️ Truncated playlist: {url}")
        
        return text_content, links
        
    except Exception as e:
        print(f"  ❌ Enhanced extraction failed: {e}")
        import traceback
        traceback.print_exc()
        return "", {}
    finally:
        driver.quit()

def test_anchor_extraction():
    """Test enhanced anchor extraction on James Kirton's document"""
    test_url = "https://docs.google.com/document/d/106UzVKTBceNnihO711snD-AeJOK9-fki9EINggkLQU8/edit?tab=t.0"
    
    print("🧪 TESTING ENHANCED ANCHOR EXTRACTION")
    print("Document: James Kirton (Row 497)")
    print("Expected: 2 complete YouTube playlist URLs")
    print("=" * 80)
    
    text_content, links = extract_google_doc_text_with_anchors(test_url)
    
    # Analyze results
    results = {
        'url': test_url,
        'text_length': len(text_content),
        'text_sample': text_content[:500] + "..." if len(text_content) > 500 else text_content,
        'links': links,
        'success': False,
        'analysis': {}
    }
    
    # Check for expected playlist URLs
    expected_playlists = [
        "PLp0u93QMy5vCYzE4OQydGEMEJjRizGDA",
        "PLu9i8x5U9PHhmD9K-5WY4EB12vyhL"
    ]
    
    found_playlists = []
    if links.get('youtube'):
        for url in links['youtube']:
            for playlist_id in expected_playlists:
                if playlist_id in url:
                    found_playlists.append(url)
                    results['success'] = True
    
    results['analysis'] = {
        'expected_playlist_ids': expected_playlists,
        'found_complete_playlists': found_playlists,
        'all_youtube_links': links.get('youtube', []),
        'extraction_method': 'enhanced_anchor_extraction'
    }
    
    # Save results
    with open('anchor_extraction_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print("\n📊 EXTRACTION RESULTS:")
    print(f"   Expected playlists: {len(expected_playlists)}")
    print(f"   Found playlists: {len(found_playlists)}")
    print(f"   Success: {'✅ YES' if results['success'] else '❌ NO'}")
    
    if found_playlists:
        print("\n   🎯 FOUND PLAYLISTS:")
        for playlist in found_playlists:
            print(f"      {playlist}")
    
    print(f"\n💾 Results saved to anchor_extraction_results.json")
    
    return results

if __name__ == "__main__":
    test_anchor_extraction()