#!/usr/bin/env python3
"""
YOUTUBE PLAYLIST URL DIAGNOSTIC
Analyzes how YouTube playlist URLs are stored in Google Docs DOM
Focuses on finding why playlist IDs are being truncated
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
    """Initialize Selenium driver for diagnostics"""
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

def diagnose_playlist_urls(url):
    """Comprehensive diagnostic of how playlist URLs are stored"""
    print(f"🔬 PLAYLIST URL DIAGNOSTIC: {url}")
    print("=" * 80)
    
    driver = get_diagnostic_driver()
    if not driver:
        return None
    
    try:
        # Load document
        print("📥 Loading document...")
        driver.get(url)
        
        # Wait for page load
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Allow content to fully render
        print("⏳ Waiting for dynamic content...")
        time.sleep(10)
        
        results = {
            'url': url,
            'diagnostics': {},
            'found_urls': []
        }
        
        # 1. Check raw page source for playlist URLs
        print("\n📄 1. ANALYZING RAW PAGE SOURCE")
        page_source = driver.page_source
        
        # Look for any YouTube playlist patterns
        playlist_patterns = [
            r'youtube\.com/playlist\?list=[a-zA-Z0-9_-]+',
            r'youtube\.com/playlist\?list(?:=|%3D)[a-zA-Z0-9_-]+',
            r'playlist\?list=[a-zA-Z0-9_-]+',
            r'list=[a-zA-Z0-9_-]+(?:&|$|\s|")',
            r'PLp0u93QMy5vCYzE4OQydGEMEJjRizGDA',  # Specific known playlist ID
            r'PLu9i8x5U9PHhmD9K-5WY4EB12vyhL'      # Another known playlist ID
        ]
        
        for pattern in playlist_patterns:
            matches = re.findall(pattern, page_source, re.IGNORECASE)
            if matches:
                print(f"   ✅ Found with pattern '{pattern}': {len(matches)} matches")
                for match in matches[:3]:  # Show first 3
                    print(f"      → {match}")
                results['diagnostics'][f'pattern_{pattern}'] = matches
        
        # 2. Extract all anchor tags
        print("\n🔗 2. ANALYZING ANCHOR TAGS")
        anchor_results = driver.execute_script("""
            var results = {
                total_anchors: 0,
                youtube_anchors: [],
                playlist_anchors: [],
                all_hrefs: []
            };
            
            var anchors = document.querySelectorAll('a[href]');
            results.total_anchors = anchors.length;
            
            for (var i = 0; i < anchors.length; i++) {
                var href = anchors[i].href;
                var text = anchors[i].textContent || anchors[i].innerText || '';
                
                results.all_hrefs.push({
                    href: href,
                    text: text.substring(0, 50),
                    outerHTML: anchors[i].outerHTML.substring(0, 200)
                });
                
                if (href.includes('youtube.com') || href.includes('youtu.be')) {
                    results.youtube_anchors.push({
                        href: href,
                        text: text,
                        innerHTML: anchors[i].innerHTML,
                        outerHTML: anchors[i].outerHTML
                    });
                    
                    if (href.includes('playlist')) {
                        results.playlist_anchors.push({
                            href: href,
                            text: text,
                            parent: anchors[i].parentElement.tagName
                        });
                    }
                }
            }
            
            return results;
        """)
        
        print(f"   Total anchors found: {anchor_results['total_anchors']}")
        print(f"   YouTube anchors: {len(anchor_results['youtube_anchors'])}")
        print(f"   Playlist anchors: {len(anchor_results['playlist_anchors'])}")
        
        if anchor_results['playlist_anchors']:
            print("   🎯 PLAYLIST ANCHORS FOUND:")
            for anchor in anchor_results['playlist_anchors']:
                print(f"      href: {anchor['href']}")
                print(f"      text: {anchor['text']}")
                print(f"      parent: {anchor['parent']}")
                results['found_urls'].append(anchor['href'])
        
        results['diagnostics']['anchors'] = anchor_results
        
        # 3. Check for encoded or partial URLs
        print("\n🔍 3. CHECKING FOR ENCODED/PARTIAL URLS")
        encoded_check = driver.execute_script("""
            var results = {
                encoded_urls: [],
                partial_urls: [],
                text_nodes_with_urls: []
            };
            
            // Check all text content
            var allText = document.body.innerText || document.body.textContent || '';
            
            // Look for encoded URLs
            var encodedMatches = allText.match(/youtube[^\\s]*list[^\\s]*/gi);
            if (encodedMatches) {
                results.encoded_urls = encodedMatches;
            }
            
            // Look for partial URLs
            var partialMatches = allText.match(/playlist\\?list(?:=|%3D)?[^\\s&]*/gi);
            if (partialMatches) {
                results.partial_urls = partialMatches;
            }
            
            // Walk through all text nodes
            var walker = document.createTreeWalker(
                document.body,
                NodeFilter.SHOW_TEXT,
                null,
                false
            );
            
            var node;
            while (node = walker.nextNode()) {
                var text = node.nodeValue;
                if (text && (text.includes('playlist') || text.includes('youtube'))) {
                    results.text_nodes_with_urls.push({
                        text: text.trim().substring(0, 200),
                        parent: node.parentElement.tagName,
                        parentClass: node.parentElement.className
                    });
                }
            }
            
            return results;
        """)
        
        if encoded_check['encoded_urls']:
            print(f"   Found {len(encoded_check['encoded_urls'])} encoded URL patterns")
            for url in encoded_check['encoded_urls'][:5]:
                print(f"      → {url}")
        
        if encoded_check['partial_urls']:
            print(f"   Found {len(encoded_check['partial_urls'])} partial URL patterns")
            for url in encoded_check['partial_urls'][:5]:
                print(f"      → {url}")
        
        results['diagnostics']['encoded_check'] = encoded_check
        
        # 4. Check contenteditable and special areas
        print("\n📝 4. CHECKING CONTENTEDITABLE AREAS")
        editable_check = driver.execute_script("""
            var results = {
                contenteditable_count: 0,
                urls_in_editable: [],
                special_containers: []
            };
            
            // Check contenteditable areas
            var editables = document.querySelectorAll('[contenteditable="true"]');
            results.contenteditable_count = editables.length;
            
            for (var i = 0; i < editables.length; i++) {
                var html = editables[i].innerHTML;
                var text = editables[i].innerText || editables[i].textContent || '';
                
                // Look for YouTube URLs in content
                if (text.includes('youtube') || text.includes('playlist')) {
                    // Extract all anchors in this editable area
                    var anchors = editables[i].querySelectorAll('a[href]');
                    for (var j = 0; j < anchors.length; j++) {
                        if (anchors[j].href.includes('youtube') || anchors[j].href.includes('playlist')) {
                            results.urls_in_editable.push({
                                href: anchors[j].href,
                                text: anchors[j].textContent,
                                html_context: anchors[j].outerHTML
                            });
                        }
                    }
                    
                    // Also check for plain text URLs
                    var urlMatches = text.match(/https?:\\/\\/[^\\s]+/g);
                    if (urlMatches) {
                        for (var k = 0; k < urlMatches.length; k++) {
                            if (urlMatches[k].includes('youtube') || urlMatches[k].includes('playlist')) {
                                results.urls_in_editable.push({
                                    href: urlMatches[k],
                                    text: 'Plain text URL',
                                    html_context: 'Found in text content'
                                });
                            }
                        }
                    }
                }
            }
            
            // Check special containers
            var specialSelectors = [
                '.kix-cursor-caret', '.kix-selection-overlay', '.kix-zoomdocumentplugin',
                '[role="link"]', '[role="presentation"]', 'span[style*="color"]'
            ];
            
            for (var s = 0; s < specialSelectors.length; s++) {
                var elements = document.querySelectorAll(specialSelectors[s]);
                if (elements.length > 0) {
                    results.special_containers.push({
                        selector: specialSelectors[s],
                        count: elements.length,
                        sample: elements[0].outerHTML.substring(0, 100)
                    });
                }
            }
            
            return results;
        """)
        
        print(f"   Contenteditable areas: {editable_check['contenteditable_count']}")
        print(f"   URLs in editable areas: {len(editable_check['urls_in_editable'])}")
        
        if editable_check['urls_in_editable']:
            print("   🎯 URLS IN EDITABLE AREAS:")
            for url_info in editable_check['urls_in_editable']:
                print(f"      href: {url_info['href']}")
                print(f"      text: {url_info['text']}")
                results['found_urls'].append(url_info['href'])
        
        results['diagnostics']['editable_check'] = editable_check
        
        # 5. DOM Mutation Observer to catch dynamic content
        print("\n🔄 5. OBSERVING DOM MUTATIONS")
        mutation_results = driver.execute_script("""
            return new Promise((resolve) => {
                var results = {
                    mutations_detected: 0,
                    new_urls_found: []
                };
                
                var observer = new MutationObserver(function(mutations) {
                    results.mutations_detected += mutations.length;
                    
                    mutations.forEach(function(mutation) {
                        if (mutation.type === 'childList') {
                            mutation.addedNodes.forEach(function(node) {
                                if (node.nodeType === 1) { // Element node
                                    var anchors = node.querySelectorAll ? node.querySelectorAll('a[href]') : [];
                                    for (var i = 0; i < anchors.length; i++) {
                                        if (anchors[i].href.includes('youtube') || anchors[i].href.includes('playlist')) {
                                            results.new_urls_found.push(anchors[i].href);
                                        }
                                    }
                                }
                            });
                        }
                    });
                });
                
                observer.observe(document.body, {
                    childList: true,
                    subtree: true,
                    attributes: true,
                    attributeFilter: ['href']
                });
                
                // Wait 5 seconds for any mutations
                setTimeout(() => {
                    observer.disconnect();
                    resolve(results);
                }, 5000);
            });
        """)
        
        print(f"   Mutations detected: {mutation_results['mutations_detected']}")
        print(f"   New URLs found: {len(mutation_results['new_urls_found'])}")
        
        results['diagnostics']['mutations'] = mutation_results
        
        # 6. Final comprehensive extraction attempt
        print("\n🎯 6. FINAL COMPREHENSIVE EXTRACTION")
        final_extraction = driver.execute_script("""
            var urls = new Set();
            
            // Method 1: All anchors
            document.querySelectorAll('a[href]').forEach(a => {
                if (a.href.includes('youtube') || a.href.includes('playlist')) {
                    urls.add(a.href);
                }
            });
            
            // Method 2: Search all text content with regex
            var allText = document.body.innerHTML;
            var urlRegex = /https?:\\/\\/(?:www\\.)?(?:youtube\\.com|youtu\\.be)\\/[^\\s<>"']+/gi;
            var matches = allText.match(urlRegex);
            if (matches) {
                matches.forEach(url => urls.add(url));
            }
            
            // Method 3: Check data attributes
            document.querySelectorAll('[data-url], [data-href], [data-link]').forEach(el => {
                ['data-url', 'data-href', 'data-link'].forEach(attr => {
                    var val = el.getAttribute(attr);
                    if (val && (val.includes('youtube') || val.includes('playlist'))) {
                        urls.add(val);
                    }
                });
            });
            
            return Array.from(urls);
        """)
        
        print(f"   Total unique YouTube/playlist URLs found: {len(final_extraction)}")
        for url in final_extraction:
            print(f"      → {url}")
            results['found_urls'].append(url)
        
        # Remove duplicates from found_urls
        results['found_urls'] = list(set(results['found_urls']))
        
        # Analysis summary
        print("\n📊 DIAGNOSTIC SUMMARY")
        print(f"   Total unique URLs found: {len(results['found_urls'])}")
        
        # Check for truncated playlist URLs
        truncated_count = 0
        complete_count = 0
        for url in results['found_urls']:
            if 'playlist?list' in url and not re.search(r'list=[a-zA-Z0-9_-]+', url):
                truncated_count += 1
                print(f"   ⚠️ TRUNCATED URL: {url}")
            elif 'playlist?list=' in url and len(url.split('list=')[1]) > 10:
                complete_count += 1
                print(f"   ✅ COMPLETE URL: {url}")
        
        print(f"\n   Truncated playlist URLs: {truncated_count}")
        print(f"   Complete playlist URLs: {complete_count}")
        
        if truncated_count > 0:
            print("\n   🔍 TRUNCATION ANALYSIS:")
            print("   Possible causes:")
            print("   1. URL is split across multiple DOM nodes")
            print("   2. Special characters in playlist ID causing regex to stop")
            print("   3. JavaScript rendering incomplete")
            print("   4. URL encoding issues")
        
        return results
        
    except Exception as e:
        print(f"❌ Diagnostic failed: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        driver.quit()

def main():
    """Run diagnostic on James Kirton's document"""
    # James Kirton's document URL
    test_url = "https://docs.google.com/document/d/106UzVKTBceNnihO711snD-AeJOK9-fki9EINggkLQU8/edit?tab=t.0"
    
    print("🔬 YOUTUBE PLAYLIST URL DIAGNOSTIC TOOL")
    print("Testing James Kirton's document for playlist URL extraction issues")
    print("Expected to find 2 complete playlist URLs")
    
    results = diagnose_playlist_urls(test_url)
    
    if results:
        # Save diagnostic results
        with open('playlist_diagnostic_results.json', 'w') as f:
            json.dump(results, f, indent=2)
        
        print(f"\n💾 Diagnostic results saved to playlist_diagnostic_results.json")
        
        # Recommendations
        print("\n💡 RECOMMENDATIONS:")
        if results['found_urls']:
            complete_playlists = [url for url in results['found_urls'] if re.search(r'playlist\?list=[a-zA-Z0-9_-]{10,}', url)]
            if len(complete_playlists) >= 2:
                print("   ✅ Successfully found complete playlist URLs!")
                print("   → Implement the extraction method that worked")
            else:
                print("   ⚠️ Found URLs but not complete playlists")
                print("   → Check anchor tag extraction and href attributes")
                print("   → May need to handle URL encoding/decoding")
        else:
            print("   ❌ No playlist URLs found")
            print("   → Document may require login or special permissions")
            print("   → Content may be in iframes or shadow DOM")

if __name__ == "__main__":
    main()