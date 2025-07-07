#!/usr/bin/env python3
"""
UNIFIED EXTRACTION TEST
Combines all enhanced extraction methods to test on James Kirton's document
Tests diagnostic, anchor extraction, dynamic observation, and URL reconstruction
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from url_reconstruction import URLReconstructor
import time
import json
import re

def get_selenium_driver():
    """Get optimized Selenium driver"""
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

def unified_extraction(url):
    """Perform unified extraction using all enhanced methods"""
    driver = get_selenium_driver()
    if not driver:
        return None
    
    try:
        print(f"🚀 UNIFIED EXTRACTION TEST: {url}")
        print("=" * 80)
        
        # Load document
        print("📥 Loading document...")
        start_time = time.time()
        driver.get(url)
        
        # Wait for initial load
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Dynamic wait for content stabilization
        print("⏳ Waiting for content stabilization...")
        previous_content_length = 0
        stable_checks = 0
        max_wait = 30
        start_wait = time.time()
        
        while time.time() - start_wait < max_wait:
            try:
                current_content_length = driver.execute_script("""
                    var content = document.body.innerText || '';
                    document.querySelectorAll('[contenteditable="true"]').forEach(el => {
                        content += el.innerText || '';
                    });
                    return content.length;
                """)
                
                if current_content_length > previous_content_length:
                    previous_content_length = current_content_length
                    stable_checks = 0
                    time.sleep(2)
                elif current_content_length == previous_content_length and current_content_length > 100:
                    stable_checks += 1
                    if stable_checks >= 3:
                        print(f"✅ Content stabilized at {current_content_length} chars")
                        break
                    time.sleep(1)
                else:
                    time.sleep(2)
            except:
                time.sleep(2)
        
        # Comprehensive extraction using all methods
        print("\n🔍 PERFORMING COMPREHENSIVE EXTRACTION...")
        extraction_result = driver.execute_script("""
            var result = {
                text_content: '',
                anchors: [],
                plain_text_urls: [],
                all_urls: new Set(),
                diagnostics: {
                    contenteditable_count: 0,
                    anchor_count: 0,
                    iframe_count: 0,
                    mutation_count: 0
                }
            };
            
            // Method 1: Extract text from all possible sources
            var textSources = [
                // Contenteditable areas
                function() {
                    var text = '';
                    var elements = document.querySelectorAll('[contenteditable="true"]');
                    result.diagnostics.contenteditable_count = elements.length;
                    elements.forEach(el => {
                        text += (el.innerText || el.textContent || '') + '\\n';
                    });
                    return text;
                },
                // Document/textbox roles
                function() {
                    var text = '';
                    document.querySelectorAll('[role="document"], [role="textbox"]').forEach(el => {
                        text += (el.innerText || el.textContent || '') + '\\n';
                    });
                    return text;
                },
                // Google Docs specific areas
                function() {
                    var text = '';
                    document.querySelectorAll('[class*="kix"]').forEach(el => {
                        var elText = el.innerText || el.textContent || '';
                        if (elText.length > 50) {
                            text += elText + '\\n';
                        }
                    });
                    return text;
                },
                // Tables and lists
                function() {
                    var text = '';
                    document.querySelectorAll('table, ul, ol').forEach(el => {
                        text += (el.innerText || el.textContent || '') + '\\n';
                    });
                    return text;
                },
                // Body fallback
                function() {
                    return document.body.innerText || document.body.textContent || '';
                }
            ];
            
            // Collect text from all sources
            var allText = '';
            textSources.forEach(function(source) {
                var text = source();
                if (text && allText.indexOf(text.substring(0, 50)) === -1) {
                    allText += text + '\\n';
                }
            });
            result.text_content = allText;
            
            // Method 2: Extract ALL anchors using multiple techniques
            var anchorMethods = [
                // Standard querySelectorAll
                function() { return document.querySelectorAll('a[href]'); },
                // Walk all <a> elements
                function() { 
                    var anchors = [];
                    var allAs = document.getElementsByTagName('a');
                    for (var i = 0; i < allAs.length; i++) {
                        if (allAs[i].href) anchors.push(allAs[i]);
                    }
                    return anchors;
                },
                // Check specific containers
                function() {
                    var anchors = [];
                    var containers = document.querySelectorAll('[contenteditable="true"], [role="document"], [class*="kix"]');
                    containers.forEach(container => {
                        container.querySelectorAll('a[href]').forEach(a => anchors.push(a));
                    });
                    return anchors;
                }
            ];
            
            // Collect all unique anchors
            var processedHrefs = new Set();
            anchorMethods.forEach(method => {
                var anchors = method();
                for (var i = 0; i < anchors.length; i++) {
                    var anchor = anchors[i];
                    var href = anchor.href || anchor.getAttribute('href') || '';
                    
                    if (href && !processedHrefs.has(href)) {
                        processedHrefs.add(href);
                        result.all_urls.add(href);
                        
                        result.anchors.push({
                            href: href,
                            text: anchor.textContent || anchor.innerText || '',
                            outerHTML: anchor.outerHTML.substring(0, 200),
                            parent: anchor.parentElement ? anchor.parentElement.tagName : ''
                        });
                    }
                }
            });
            result.diagnostics.anchor_count = result.anchors.length;
            
            // Method 3: Extract plain text URLs using regex
            var urlRegex = /https?:\\/\\/[^\\s<>"{}\\\\|\\^\\[\\]`]+[^\\s<>"{}\\\\|\\^\\[\\]`.,;:!?\\)\\]]/gi;
            var textUrls = allText.match(urlRegex) || [];
            
            textUrls.forEach(url => {
                result.plain_text_urls.push(url);
                result.all_urls.add(url);
            });
            
            // Method 4: Look for partial URLs and playlist IDs
            var playlistPatterns = [
                /playlist\\?list=[a-zA-Z0-9_-]+/gi,
                /list=([a-zA-Z0-9_-]{10,})/gi,
                /PL[a-zA-Z0-9_-]{10,}/gi,
                /youtube\\.com\\/playlist/gi
            ];
            
            playlistPatterns.forEach(pattern => {
                var matches = allText.match(pattern) || [];
                matches.forEach(match => {
                    result.all_urls.add(match);
                });
            });
            
            // Method 5: Check iframes
            result.diagnostics.iframe_count = document.querySelectorAll('iframe').length;
            
            // Method 6: Check HTML source for hidden URLs
            var htmlSource = document.documentElement.innerHTML;
            var hiddenUrls = htmlSource.match(urlRegex) || [];
            hiddenUrls.forEach(url => {
                if ((url.includes('youtube') || url.includes('playlist')) && !result.all_urls.has(url)) {
                    result.all_urls.add(url);
                }
            });
            
            // Convert Set to Array
            result.all_urls = Array.from(result.all_urls);
            
            console.log('Unified extraction complete:', result.diagnostics);
            return result;
        """)
        
        # Process extraction results
        text_content = extraction_result.get('text_content', '')
        anchors = extraction_result.get('anchors', [])
        plain_text_urls = extraction_result.get('plain_text_urls', [])
        all_urls = extraction_result.get('all_urls', [])
        diagnostics = extraction_result.get('diagnostics', {})
        
        print(f"\n📊 EXTRACTION DIAGNOSTICS:")
        print(f"   Text content: {len(text_content)} characters")
        print(f"   Contenteditable areas: {diagnostics.get('contenteditable_count', 0)}")
        print(f"   Anchors found: {diagnostics.get('anchor_count', 0)}")
        print(f"   Plain text URLs: {len(plain_text_urls)}")
        print(f"   Total unique URLs: {len(all_urls)}")
        print(f"   Iframes: {diagnostics.get('iframe_count', 0)}")
        
        # Apply URL reconstruction
        print("\n🔧 APPLYING URL RECONSTRUCTION...")
        reconstructor = URLReconstructor()
        
        # Combine all text for reconstruction
        all_text_for_reconstruction = text_content + " " + " ".join(all_urls)
        
        # Find playlist IDs
        playlist_ids = reconstructor.find_playlist_ids(all_text_for_reconstruction)
        print(f"   Found playlist IDs: {playlist_ids}")
        
        # Reconstruct URLs
        reconstructed = reconstructor.reconstruct_urls(all_text_for_reconstruction)
        print(f"   Reconstructed URLs: {len(reconstructed)}")
        
        for item in reconstructed:
            print(f"     {item['method']}: {item['reconstructed']}")
        
        # Extract all YouTube URLs
        youtube_results = reconstructor.extract_all_youtube_urls(all_text_for_reconstruction)
        
        # Compile final results
        final_results = {
            'url': url,
            'extraction_time': time.time() - start_time,
            'text_length': len(text_content),
            'diagnostics': diagnostics,
            'raw_data': {
                'anchors': len(anchors),
                'plain_text_urls': len(plain_text_urls),
                'all_urls': len(all_urls)
            },
            'youtube_urls': {
                'all': youtube_results['all_urls'],
                'playlists': youtube_results['complete_playlist_urls'],
                'videos': youtube_results['video_urls']
            },
            'playlist_ids': playlist_ids,
            'reconstructed_urls': [item['reconstructed'] for item in reconstructed],
            'success_metrics': {
                'found_playlists': len(youtube_results['complete_playlist_urls']),
                'expected_playlists': 2,
                'success': len(youtube_results['complete_playlist_urls']) >= 2
            }
        }
        
        return final_results
        
    except Exception as e:
        print(f"❌ Unified extraction failed: {e}")
        import traceback
        traceback.print_exc()
        return None
    finally:
        driver.quit()

def main():
    """Test unified extraction on James Kirton's document"""
    test_url = "https://docs.google.com/document/d/106UzVKTBceNnihO711snD-AeJOK9-fki9EINggkLQU8/edit?tab=t.0"
    
    print("🧪 UNIFIED EXTRACTION TEST - JAMES KIRTON (ROW 497)")
    print("Expected: 2 complete YouTube playlist URLs")
    print("  1. https://youtube.com/playlist?list=PLp0u93QMy5vCYzE4OQydGEMEJjRizGDA")
    print("  2. https://youtube.com/playlist?list=PLu9i8x5U9PHhmD9K-5WY4EB12vyhL")
    print("=" * 80)
    
    results = unified_extraction(test_url)
    
    if results:
        # Display results
        print("\n📈 FINAL RESULTS:")
        print(f"   Extraction time: {results['extraction_time']:.2f} seconds")
        print(f"   Text extracted: {results['text_length']} characters")
        
        print(f"\n🎯 YOUTUBE CONTENT:")
        print(f"   Total YouTube URLs: {len(results['youtube_urls']['all'])}")
        print(f"   Complete playlists: {len(results['youtube_urls']['playlists'])}")
        print(f"   Video URLs: {len(results['youtube_urls']['videos'])}")
        
        if results['youtube_urls']['playlists']:
            print(f"\n   ✅ FOUND PLAYLIST URLS:")
            for i, playlist_url in enumerate(results['youtube_urls']['playlists'], 1):
                print(f"      {i}. {playlist_url}")
                
                # Check against expected
                if "PLp0u93QMy5vCYzE4OQydGEMEJjRizGDA" in playlist_url:
                    print(f"         → Matched expected playlist 1 ✓")
                elif "PLu9i8x5U9PHhmD9K-5WY4EB12vyhL" in playlist_url:
                    print(f"         → Matched expected playlist 2 ✓")
        
        # Success evaluation
        success_metrics = results['success_metrics']
        print(f"\n📊 SUCCESS EVALUATION:")
        print(f"   Expected playlists: {success_metrics['expected_playlists']}")
        print(f"   Found playlists: {success_metrics['found_playlists']}")
        print(f"   Result: {'✅ SUCCESS' if success_metrics['success'] else '❌ FAILED'}")
        
        # Save complete results
        with open('unified_extraction_results.json', 'w') as f:
            # Convert sets to lists for JSON serialization
            json_safe_results = json.dumps(results, indent=2, default=str)
            f.write(json_safe_results)
        
        print(f"\n💾 Complete results saved to unified_extraction_results.json")
        
        # Recommendations
        print(f"\n💡 RECOMMENDATIONS:")
        if success_metrics['success']:
            print("   ✅ Extraction successful! Integrate these methods into simple_workflow.py:")
            print("      1. Dynamic content stabilization wait")
            print("      2. Comprehensive anchor tag extraction")
            print("      3. URL reconstruction for incomplete playlist URLs")
            print("      4. Multiple text extraction methods")
        else:
            print("   ❌ Extraction incomplete. Additional methods needed:")
            print("      1. Check if document requires authentication")
            print("      2. Investigate browser console for JavaScript errors")
            print("      3. Consider using Google Docs API for direct access")
            print("      4. Manual inspection of document structure")
    
    return results

if __name__ == "__main__":
    results = main()
    
    # If extraction wasn't fully successful, provide debugging info
    if results and not results['success_metrics']['success']:
        print("\n🔍 DEBUGGING INFORMATION:")
        print("   Consider running with visible browser (remove --headless)")
        print("   Check if playlist URLs are in:")
        print("     - Document comments")
        print("     - Hidden/collapsed sections")
        print("     - Special formatting that requires interaction")
        print("     - Protected/restricted content areas")