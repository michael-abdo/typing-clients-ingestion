#!/usr/bin/env python3
"""
GOOGLE DOCS DOM STRUCTURE INSPECTOR
Research current Google Docs DOM structure and identify active CSS selectors
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
import json
import re

def get_inspector_driver():
    """Get Selenium driver for DOM inspection"""
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

def analyze_dom_structure(url):
    """Analyze Google Docs DOM structure to identify content selectors"""
    print(f"🔍 ANALYZING DOM STRUCTURE: {url}")
    print("=" * 80)
    
    driver = get_inspector_driver()
    if not driver:
        return None
    
    try:
        # Load the document
        print("📥 Loading document...")
        driver.get(url)
        
        # Wait for page to load
        WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.TAG_NAME, "body"))
        )
        
        # Wait for content to render
        time.sleep(10)  # Longer wait to ensure full loading
        
        print("🏗️  Analyzing DOM structure...")
        
        # Get page source
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        
        # 1. Find all divs and analyze their characteristics
        print("\n📊 DIV ELEMENT ANALYSIS:")
        divs = soup.find_all('div')
        print(f"   Total div elements: {len(divs)}")
        
        # Analyze divs by class patterns
        class_patterns = {}
        content_candidates = []
        
        for div in divs:
            if div.get('class'):
                classes = ' '.join(div.get('class'))
                
                # Look for Google Docs patterns
                if any(pattern in classes for pattern in ['kix', 'docs', 'content', 'page', 'document']):
                    text_content = div.get_text(strip=True)
                    if len(text_content) > 50:  # Substantial content
                        content_candidates.append({
                            'classes': classes,
                            'text_length': len(text_content),
                            'text_sample': text_content[:200] + "..." if len(text_content) > 200 else text_content,
                            'has_links': len(div.find_all('a')) > 0,
                            'link_count': len(div.find_all('a'))
                        })
                
                # Count class pattern frequency
                for class_name in div.get('class'):
                    if class_name.startswith(('kix', 'docs', 'content', 'page', 'document')):
                        class_patterns[class_name] = class_patterns.get(class_name, 0) + 1
        
        print(f"   Content candidates found: {len(content_candidates)}")
        print(f"   Relevant class patterns:")
        for pattern, count in sorted(class_patterns.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"     {pattern}: {count} occurrences")
        
        # 2. Sort content candidates by text length
        content_candidates.sort(key=lambda x: x['text_length'], reverse=True)
        
        print(f"\n📝 TOP CONTENT CANDIDATES:")
        for i, candidate in enumerate(content_candidates[:5]):
            print(f"   {i+1}. Classes: {candidate['classes']}")
            print(f"      Text length: {candidate['text_length']} chars")
            print(f"      Links: {candidate['link_count']}")
            print(f"      Sample: {candidate['text_sample']}")
            print()
        
        # 3. Look for specific Google Docs content areas
        print(f"\n🎯 GOOGLE DOCS SPECIFIC ANALYSIS:")
        
        # Test various known and potential selectors
        test_selectors = [
            # Current selectors (known to fail)
            '.kix-pagesettings-protected-text', '.kix-page', '.doc-content', '.google-docs-content',
            # Potential new selectors based on Google Docs evolution
            '.kix-canvas-content', '.kix-page-content', '.kix-page-content-wrap',
            '.kix-pagesettings-content', '.kix-canvas-tile-content', '.kix-canvas',
            '.kix-pagesettings-page', '.kix-wordhtmlgenerator-word-wrap',
            # Generic content selectors
            '[role="document"]', '[role="textbox"]', '[contenteditable="true"]',
            '.docs-texteventtarget-iframe', '.docs-content',
            # Check for iframes (Google Docs often uses iframes)
            'iframe'
        ]
        
        selector_results = {}
        for selector in test_selectors:
            try:
                elements = soup.select(selector)
                if elements:
                    total_text = ' '.join([elem.get_text(strip=True) for elem in elements])
                    if len(total_text) > 20:  # Meaningful content
                        selector_results[selector] = {
                            'element_count': len(elements),
                            'text_length': len(total_text),
                            'sample_text': total_text[:200] + "..." if len(total_text) > 200 else total_text,
                            'has_links': any(elem.find_all('a') for elem in elements),
                            'link_count': sum(len(elem.find_all('a')) for elem in elements)
                        }
            except Exception:
                continue
        
        print(f"   Working selectors found: {len(selector_results)}")
        for selector, info in sorted(selector_results.items(), key=lambda x: x[1]['text_length'], reverse=True):
            print(f"     📍 {selector}:")
            print(f"       Elements: {info['element_count']}, Text: {info['text_length']} chars, Links: {info['link_count']}")
            print(f"       Sample: {info['sample_text']}")
            print()
        
        # 4. Check for iframes (Google Docs editing interface)
        print(f"\n🖼️  IFRAME ANALYSIS:")
        iframes = soup.find_all('iframe')
        print(f"   Found {len(iframes)} iframes")
        
        for i, iframe in enumerate(iframes):
            src = iframe.get('src', '')
            title = iframe.get('title', '')
            print(f"     {i+1}. Src: {src[:100]}...")
            print(f"        Title: {title}")
            
            # Check if this might be the editing iframe
            if 'editing' in src or 'editor' in src or 'kix' in src:
                print(f"        ⭐ POTENTIAL EDITING IFRAME")
        
        # 5. JavaScript-based content detection
        print(f"\n🔍 JAVASCRIPT CONTENT DETECTION:")
        try:
            # Try to get text content using JavaScript
            js_text = driver.execute_script("""
                // Try different methods to get document content
                var content = '';
                
                // Method 1: Try to find contenteditable areas
                var editables = document.querySelectorAll('[contenteditable="true"]');
                for (var i = 0; i < editables.length; i++) {
                    content += editables[i].innerText + ' ';
                }
                
                // Method 2: Try to find document text areas
                var docElements = document.querySelectorAll('[role="document"], [role="textbox"]');
                for (var i = 0; i < docElements.length; i++) {
                    content += docElements[i].innerText + ' ';
                }
                
                // Method 3: Look in iframes (if accessible)
                var iframes = document.querySelectorAll('iframe');
                for (var i = 0; i < iframes.length; i++) {
                    try {
                        var iframeDoc = iframes[i].contentDocument || iframes[i].contentWindow.document;
                        if (iframeDoc) {
                            content += iframeDoc.body.innerText + ' ';
                        }
                    } catch (e) {
                        // Cross-origin iframe, skip
                    }
                }
                
                // Method 4: Try Google Docs specific classes
                var kixElements = document.querySelectorAll('[class*="kix"]');
                for (var i = 0; i < kixElements.length; i++) {
                    if (kixElements[i].innerText && kixElements[i].innerText.length > 50) {
                        content += kixElements[i].innerText + ' ';
                    }
                }
                
                return content.trim();
            """)
            
            print(f"   JavaScript-extracted content: {len(js_text)} characters")
            if js_text:
                print(f"   Sample: {js_text[:300]}...")
                
                # Look for links in JavaScript-extracted content
                youtube_pattern = r'https?://(?:www\.)?(?:youtube\.com|youtu\.be)/[^\s<>"{}\\|^`\[\]]+'
                drive_pattern = r'https?://drive\.google\.com/[^\s<>"{}\\|^`\[\]]+'
                
                youtube_matches = re.findall(youtube_pattern, js_text)
                drive_matches = re.findall(drive_pattern, js_text)
                
                print(f"   YouTube links in JS content: {len(youtube_matches)}")
                for yt in youtube_matches:
                    print(f"     🎥 {yt}")
                
                print(f"   Drive links in JS content: {len(drive_matches)}")
                for drive in drive_matches:
                    print(f"     💾 {drive}")
            
        except Exception as e:
            print(f"   JavaScript execution failed: {e}")
        
        # 6. Generate recommendations
        print(f"\n💡 RECOMMENDATIONS:")
        
        if selector_results:
            best_selector = max(selector_results.items(), key=lambda x: x[1]['text_length'])
            print(f"   ✅ Best working selector: {best_selector[0]}")
            print(f"      ({best_selector[1]['text_length']} chars, {best_selector[1]['link_count']} links)")
        else:
            print(f"   ❌ No working CSS selectors found")
        
        if js_text and len(js_text) > 100:
            print(f"   ✅ JavaScript extraction working ({len(js_text)} chars)")
            print(f"   💡 Recommend switching to JavaScript-based extraction")
        else:
            print(f"   ❌ JavaScript extraction also limited")
        
        if iframes:
            print(f"   💡 Document may be in iframe - consider iframe access methods")
        
        return {
            'url': url,
            'total_divs': len(divs),
            'class_patterns': class_patterns,
            'content_candidates': content_candidates[:10],  # Top 10
            'working_selectors': selector_results,
            'iframes_found': len(iframes),
            'js_content_length': len(js_text) if 'js_text' in locals() else 0,
            'js_content_sample': js_text[:500] if 'js_text' in locals() and js_text else "",
            'recommendations': {
                'best_css_selector': best_selector[0] if selector_results else None,
                'use_javascript': len(js_text) > 100 if 'js_text' in locals() else False,
                'has_iframes': len(iframes) > 0,
                'needs_iframe_access': len(iframes) > 0 and not selector_results
            }
        }
        
    except Exception as e:
        print(f"❌ DOM analysis failed: {e}")
        return None
    finally:
        driver.quit()

def main():
    """Analyze DOM structure of problematic documents"""
    test_urls = [
        # Caroline Chiu - has Drive link but missing YouTube content  
        "https://docs.google.com/document/d/1Ael_iSce9tO3SECHp5X5N0yYgahDnnCY837aYsC21PE/edit?tab=t.0",
        # James Kirton - has incomplete YouTube playlist URL
        "https://docs.google.com/document/d/106UzVKTBceNnihO711snD-AeJOK9-fki9EINggkLQU8/edit?tab=t.0"
    ]
    
    results = []
    
    for i, url in enumerate(test_urls, 1):
        print(f"\n{'='*100}")
        print(f"DOM ANALYSIS {i}/{len(test_urls)}")
        print(f"{'='*100}")
        
        result = analyze_dom_structure(url)
        if result:
            results.append(result)
            
            # Save individual results
            with open(f'dom_analysis_{i}.json', 'w') as f:
                json.dump(result, f, indent=2)
        
        if i < len(test_urls):
            print("\nWaiting 5 seconds before next analysis...")
            time.sleep(5)
    
    # Save combined results
    with open('dom_structure_analysis.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    print(f"\n🎉 DOM STRUCTURE ANALYSIS COMPLETE")
    print(f"Results saved to dom_structure_analysis.json")
    print(f"Individual results: dom_analysis_1.json, dom_analysis_2.json")

if __name__ == "__main__":
    main()