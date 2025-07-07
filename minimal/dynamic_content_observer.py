#!/usr/bin/env python3
"""
DYNAMIC CONTENT OBSERVER
Uses DOM Mutation Observer to detect dynamically loaded playlist IDs
Captures content that appears after initial page load
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
import json
import re

def get_selenium_driver():
    """Get Selenium driver with extended capabilities"""
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

def observe_dynamic_content(url, observation_time=15):
    """
    Observe DOM mutations to catch dynamically loaded content
    Especially useful for playlist IDs that might load after initial render
    """
    driver = get_selenium_driver()
    if not driver:
        return {}
    
    try:
        print(f"🔄 DYNAMIC CONTENT OBSERVER: {url}")
        print(f"   Observation time: {observation_time} seconds")
        print("=" * 80)
        
        # Load the page
        print("📥 Loading document...")
        driver.get(url)
        
        # Wait for initial page load
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Set up comprehensive mutation observer
        print("🔍 Setting up mutation observer...")
        observer_script = """
        window.observerResults = {
            initial_state: {
                anchors: [],
                text_content: '',
                timestamp: Date.now()
            },
            mutations: [],
            new_content: {
                anchors: [],
                text_segments: [],
                playlist_urls: []
            },
            final_state: {
                anchors: [],
                text_content: '',
                timestamp: null
            }
        };
        
        // Capture initial state
        function captureState() {
            var state = {
                anchors: [],
                text_content: document.body.innerText || ''
            };
            
            document.querySelectorAll('a[href]').forEach(a => {
                state.anchors.push({
                    href: a.href,
                    text: a.textContent || '',
                    timestamp: Date.now()
                });
            });
            
            return state;
        }
        
        // Capture initial state
        window.observerResults.initial_state = captureState();
        window.observerResults.initial_state.timestamp = Date.now();
        
        // Create mutation observer
        var observer = new MutationObserver(function(mutations) {
            mutations.forEach(function(mutation) {
                var mutationRecord = {
                    type: mutation.type,
                    timestamp: Date.now(),
                    target: mutation.target.tagName || 'unknown',
                    targetClass: mutation.target.className || '',
                    addedNodes: mutation.addedNodes.length,
                    removedNodes: mutation.removedNodes.length
                };
                
                // Check added nodes for new content
                mutation.addedNodes.forEach(function(node) {
                    if (node.nodeType === 1) { // Element node
                        // Check for new anchors
                        var newAnchors = [];
                        if (node.tagName === 'A' && node.href) {
                            newAnchors.push(node);
                        }
                        if (node.querySelectorAll) {
                            node.querySelectorAll('a[href]').forEach(a => newAnchors.push(a));
                        }
                        
                        newAnchors.forEach(function(anchor) {
                            var anchorData = {
                                href: anchor.href,
                                text: anchor.textContent || '',
                                timestamp: Date.now(),
                                parent: anchor.parentElement ? anchor.parentElement.tagName : ''
                            };
                            
                            // Check if it's a YouTube playlist
                            if (anchor.href.includes('youtube') || anchor.href.includes('playlist')) {
                                window.observerResults.new_content.playlist_urls.push(anchorData);
                                mutationRecord.found_playlist = true;
                            }
                            
                            window.observerResults.new_content.anchors.push(anchorData);
                        });
                        
                        // Check for new text content
                        var text = node.innerText || node.textContent || '';
                        if (text.length > 20 && (text.includes('youtube') || text.includes('playlist'))) {
                            window.observerResults.new_content.text_segments.push({
                                text: text.substring(0, 200),
                                timestamp: Date.now(),
                                element: node.tagName
                            });
                        }
                    }
                    
                    // Check text nodes for URLs
                    if (node.nodeType === 3 && node.nodeValue) { // Text node
                        var urlPattern = /https?:\\/\\/[^\\s]+/g;
                        var matches = node.nodeValue.match(urlPattern);
                        if (matches) {
                            matches.forEach(function(url) {
                                if (url.includes('youtube') || url.includes('playlist')) {
                                    window.observerResults.new_content.playlist_urls.push({
                                        href: url,
                                        text: 'Plain text URL',
                                        timestamp: Date.now(),
                                        parent: node.parentElement ? node.parentElement.tagName : ''
                                    });
                                    mutationRecord.found_text_url = true;
                                }
                            });
                        }
                    }
                });
                
                // Record the mutation
                window.observerResults.mutations.push(mutationRecord);
            });
        });
        
        // Observe with comprehensive options
        observer.observe(document.body, {
            childList: true,
            subtree: true,
            attributes: true,
            attributeOldValue: true,
            characterData: true,
            characterDataOldValue: true
        });
        
        // Function to check for specific content patterns
        window.checkForPlaylistPatterns = function() {
            var patterns = [
                /playlist\\?list=[a-zA-Z0-9_-]+/gi,
                /PLp0u93QMy5vCYzE4OQydGEMEJjRizGDA/gi,
                /PLu9i8x5U9PHhmD9K-5WY4EB12vyhL/gi
            ];
            
            var html = document.body.innerHTML;
            var found = [];
            
            patterns.forEach(function(pattern) {
                var matches = html.match(pattern);
                if (matches) {
                    matches.forEach(function(match) {
                        found.push({
                            pattern: pattern.toString(),
                            match: match,
                            timestamp: Date.now()
                        });
                    });
                }
            });
            
            return found;
        };
        
        // Store observer for later cleanup
        window.mutationObserver = observer;
        
        console.log('Mutation observer started at', new Date().toISOString());
        """
        
        driver.execute_script(observer_script)
        
        # Simulate user interactions that might trigger content loading
        print("👆 Simulating user interactions...")
        interaction_script = """
        // Scroll through the document
        var scrollPositions = [0, 0.25, 0.5, 0.75, 1];
        var currentScroll = 0;
        
        function scrollNext() {
            if (currentScroll < scrollPositions.length) {
                var position = scrollPositions[currentScroll] * document.body.scrollHeight;
                window.scrollTo(0, position);
                console.log('Scrolled to', position);
                currentScroll++;
            }
        }
        
        // Trigger hover events on links
        document.querySelectorAll('a').forEach(function(link, index) {
            setTimeout(function() {
                link.dispatchEvent(new MouseEvent('mouseenter', {bubbles: true}));
                setTimeout(function() {
                    link.dispatchEvent(new MouseEvent('mouseleave', {bubbles: true}));
                }, 100);
            }, index * 50);
        });
        
        // Click on contenteditable areas to trigger focus
        document.querySelectorAll('[contenteditable="true"]').forEach(function(el, index) {
            setTimeout(function() {
                el.focus();
                console.log('Focused contenteditable', index);
            }, 2000 + (index * 500));
        });
        
        // Scroll periodically
        var scrollInterval = setInterval(scrollNext, 2000);
        setTimeout(function() {
            clearInterval(scrollInterval);
        }, 10000);
        """
        
        driver.execute_script(interaction_script)
        
        # Monitor for changes periodically
        print("📊 Monitoring for dynamic content...")
        start_time = time.time()
        check_interval = 1  # Check every second
        pattern_checks = []
        
        while time.time() - start_time < observation_time:
            elapsed = time.time() - start_time
            
            # Check for playlist patterns
            pattern_results = driver.execute_script("return window.checkForPlaylistPatterns();")
            if pattern_results:
                for result in pattern_results:
                    if result not in pattern_checks:
                        pattern_checks.append(result)
                        print(f"   [{elapsed:.1f}s] Found pattern: {result['match']}")
            
            # Get current mutation count
            mutation_count = driver.execute_script("return window.observerResults.mutations.length;")
            new_anchor_count = driver.execute_script("return window.observerResults.new_content.anchors.length;")
            playlist_url_count = driver.execute_script("return window.observerResults.new_content.playlist_urls.length;")
            
            if elapsed % 3 < check_interval:  # Log every 3 seconds
                print(f"   [{elapsed:.1f}s] Mutations: {mutation_count}, New anchors: {new_anchor_count}, Playlist URLs: {playlist_url_count}")
            
            time.sleep(check_interval)
        
        # Stop observer and capture final state
        print("\n🏁 Stopping observer and capturing final state...")
        final_results = driver.execute_script("""
        // Stop the observer
        if (window.mutationObserver) {
            window.mutationObserver.disconnect();
        }
        
        // Capture final state
        window.observerResults.final_state = captureState();
        window.observerResults.final_state.timestamp = Date.now();
        
        // Analyze differences
        var initialAnchors = window.observerResults.initial_state.anchors.map(a => a.href);
        var finalAnchors = window.observerResults.final_state.anchors.map(a => a.href);
        
        var newAnchors = finalAnchors.filter(href => !initialAnchors.includes(href));
        
        // Compile comprehensive results
        return {
            observation_duration: window.observerResults.final_state.timestamp - window.observerResults.initial_state.timestamp,
            total_mutations: window.observerResults.mutations.length,
            initial_anchor_count: window.observerResults.initial_state.anchors.length,
            final_anchor_count: window.observerResults.final_state.anchors.length,
            new_anchors_found: newAnchors,
            new_content: window.observerResults.new_content,
            mutations_with_playlists: window.observerResults.mutations.filter(m => m.found_playlist || m.found_text_url),
            all_playlist_urls: window.observerResults.new_content.playlist_urls
        };
        """)
        
        # Extract all YouTube/playlist URLs found
        all_urls = set()
        
        # From initial state
        initial_state = driver.execute_script("return window.observerResults.initial_state;")
        for anchor in initial_state.get('anchors', []):
            if 'youtube' in anchor['href'] or 'playlist' in anchor['href']:
                all_urls.add(anchor['href'])
        
        # From new content
        for url_data in final_results.get('all_playlist_urls', []):
            all_urls.add(url_data['href'])
        
        # From pattern checks
        for pattern_result in pattern_checks:
            if pattern_result['match'].startswith('http'):
                all_urls.add(pattern_result['match'])
            else:
                # Reconstruct URL if it's just the playlist part
                if 'playlist?list=' in pattern_result['match']:
                    all_urls.add(f"https://www.youtube.com/{pattern_result['match']}")
        
        results = {
            'url': url,
            'observation_time': observation_time,
            'final_results': final_results,
            'pattern_checks': pattern_checks,
            'all_youtube_urls': list(all_urls),
            'complete_playlist_urls': [url for url in all_urls if re.search(r'playlist\?list=[a-zA-Z0-9_-]{10,}', url)]
        }
        
        return results
        
    except Exception as e:
        print(f"❌ Dynamic observation failed: {e}")
        import traceback
        traceback.print_exc()
        return {}
    finally:
        driver.quit()

def test_dynamic_observation():
    """Test dynamic content observation on James Kirton's document"""
    test_url = "https://docs.google.com/document/d/106UzVKTBceNnihO711snD-AeJOK9-fki9EINggkLQU8/edit?tab=t.0"
    
    print("🧪 TESTING DYNAMIC CONTENT OBSERVATION")
    print("Document: James Kirton (Row 497)")
    print("Expected: 2 complete YouTube playlist URLs")
    print("=" * 80)
    
    results = observe_dynamic_content(test_url, observation_time=20)
    
    # Analyze results
    print("\n📊 OBSERVATION RESULTS:")
    
    if results.get('final_results'):
        fr = results['final_results']
        print(f"   Observation duration: {fr.get('observation_duration', 0) / 1000:.1f} seconds")
        print(f"   Total mutations detected: {fr.get('total_mutations', 0)}")
        print(f"   Initial anchors: {fr.get('initial_anchor_count', 0)}")
        print(f"   Final anchors: {fr.get('final_anchor_count', 0)}")
        print(f"   New anchors found: {len(fr.get('new_anchors_found', []))}")
        print(f"   Playlist-related mutations: {len(fr.get('mutations_with_playlists', []))}")
    
    print(f"\n🎯 YOUTUBE/PLAYLIST URLS FOUND:")
    all_urls = results.get('all_youtube_urls', [])
    complete_playlists = results.get('complete_playlist_urls', [])
    
    print(f"   Total YouTube URLs: {len(all_urls)}")
    print(f"   Complete playlist URLs: {len(complete_playlists)}")
    
    if complete_playlists:
        print("\n   ✅ COMPLETE PLAYLIST URLS:")
        for url in complete_playlists:
            print(f"      {url}")
            # Check for expected playlist IDs
            if "PLp0u93QMy5vCYzE4OQydGEMEJjRizGDA" in url:
                print(f"        → Found expected playlist 1")
            if "PLu9i8x5U9PHhmD9K-5WY4EB12vyhL" in url:
                print(f"        → Found expected playlist 2")
    
    # Save results
    with open('dynamic_observation_results.json', 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"\n💾 Results saved to dynamic_observation_results.json")
    
    # Success evaluation
    expected_count = 2
    found_count = len(complete_playlists)
    success = found_count >= expected_count
    
    print(f"\n📈 SUCCESS EVALUATION:")
    print(f"   Expected playlists: {expected_count}")
    print(f"   Found playlists: {found_count}")
    print(f"   Result: {'✅ SUCCESS' if success else '❌ NEEDS IMPROVEMENT'}")
    
    return results

if __name__ == "__main__":
    test_dynamic_observation()