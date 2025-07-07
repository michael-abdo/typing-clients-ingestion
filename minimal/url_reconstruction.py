#!/usr/bin/env python3
"""
URL RECONSTRUCTION MODULE
Creates regex patterns that handle partial URLs and reconstruct complete playlist links
Handles cases where playlist IDs are separated from base URLs
"""

import re
import json

class URLReconstructor:
    """Reconstructs complete URLs from partial or malformed URL fragments"""
    
    def __init__(self):
        # Known playlist ID patterns
        self.playlist_id_patterns = [
            r'PLp0u93QMy5v[a-zA-Z0-9_-]+',  # Playlist IDs starting with PLp0u93QMy5v
            r'PLu9i8x5U9P[a-zA-Z0-9_-]+',   # Playlist IDs starting with PLu9i8x5U9P
            r'PL[a-zA-Z0-9_-]{16,}',        # General playlist ID pattern (PL + 16+ chars)
            r'PL[a-zA-Z0-9_-]{10,}',        # Shorter playlist IDs (PL + 10+ chars)
            r'list=([a-zA-Z0-9_-]+)',        # List parameter with ID
            r'list%3D([a-zA-Z0-9_-]+)',      # URL-encoded list parameter
        ]
        
        # Base URL patterns for YouTube
        self.youtube_base_patterns = [
            'https://www.youtube.com/',
            'https://youtube.com/',
            'http://www.youtube.com/',
            'http://youtube.com/',
            'https://youtu.be/',
            'youtube.com/',
            'youtu.be/'
        ]
        
        # Patterns for finding incomplete URLs
        self.incomplete_patterns = [
            # Partial playlist URLs
            (r'(https?://(?:www\.)?youtube\.com/playlist\?list)(?:$|\s|["\'])', r'\1='),
            (r'(playlist\?list)(?:$|\s|["\'])', r'https://www.youtube.com/\1='),
            (r'youtube\.com/playlist\?list(?:$|\s)', 'https://www.youtube.com/playlist?list='),
            
            # Separated components
            (r'playlist\s*\?\s*list\s*=?\s*([a-zA-Z0-9_-]+)', r'https://www.youtube.com/playlist?list=\1'),
            (r'list\s*=\s*([a-zA-Z0-9_-]{10,})', r'https://www.youtube.com/playlist?list=\1'),
            
            # URL-encoded versions
            (r'playlist%3Flist%3D([a-zA-Z0-9_-]+)', r'https://www.youtube.com/playlist?list=\1'),
            (r'list%3D([a-zA-Z0-9_-]+)', r'https://www.youtube.com/playlist?list=\1'),
        ]
    
    def find_playlist_ids(self, text):
        """Find all playlist IDs in text, even if not part of complete URLs"""
        playlist_ids = set()
        
        for pattern in self.playlist_id_patterns:
            matches = re.finditer(pattern, text, re.IGNORECASE)
            for match in matches:
                if match.group(1) if match.groups() else match.group(0):
                    playlist_id = match.group(1) if match.groups() else match.group(0)
                    # Clean up the ID
                    if playlist_id.startswith('list='):
                        playlist_id = playlist_id[5:]
                    elif playlist_id.startswith('list%3D'):
                        playlist_id = playlist_id[7:]
                    
                    # Validate it looks like a playlist ID
                    if len(playlist_id) >= 10 and re.match(r'^[a-zA-Z0-9_-]+$', playlist_id):
                        playlist_ids.add(playlist_id)
        
        return list(playlist_ids)
    
    def reconstruct_urls(self, text):
        """Reconstruct complete URLs from partial fragments"""
        reconstructed_urls = []
        
        # Method 1: Apply incomplete patterns
        working_text = text
        for pattern, replacement in self.incomplete_patterns:
            matches = re.finditer(pattern, working_text, re.IGNORECASE)
            for match in matches:
                if isinstance(replacement, str) and '\\1' in replacement:
                    # Pattern with capture group
                    reconstructed = re.sub(pattern, replacement, match.group(0))
                else:
                    # Simple replacement
                    reconstructed = replacement
                
                # Validate the reconstructed URL
                if self.is_valid_playlist_url(reconstructed):
                    reconstructed_urls.append({
                        'original': match.group(0),
                        'reconstructed': reconstructed,
                        'method': 'pattern_replacement'
                    })
        
        # Method 2: Find orphaned playlist IDs and build URLs
        playlist_ids = self.find_playlist_ids(text)
        for playlist_id in playlist_ids:
            # Check if this ID is already part of a complete URL
            complete_url_pattern = f'https?://[^\\s]*playlist\\?list={playlist_id}'
            if not re.search(complete_url_pattern, text):
                # This ID is orphaned, create a URL for it
                reconstructed_url = f'https://www.youtube.com/playlist?list={playlist_id}'
                reconstructed_urls.append({
                    'original': playlist_id,
                    'reconstructed': reconstructed_url,
                    'method': 'orphaned_id'
                })
        
        # Method 3: Fix truncated URLs
        # Look for URLs that end with 'playlist?list' or 'playlist?list='
        truncated_pattern = r'(https?://[^\\s]*youtube[^\\s]*playlist\?list=?)(?:$|\\s|["\'])'
        matches = re.finditer(truncated_pattern, text, re.IGNORECASE)
        
        for match in matches:
            url_fragment = match.group(1)
            # Look ahead in the text for a potential playlist ID
            remaining_text = text[match.end():]
            id_match = re.match(r'^[=\\s]*([a-zA-Z0-9_-]{10,})', remaining_text)
            
            if id_match:
                playlist_id = id_match.group(1)
                reconstructed_url = f'{url_fragment}{playlist_id}'
                if '=' not in reconstructed_url:
                    reconstructed_url = reconstructed_url.replace('list', 'list=')
                
                reconstructed_urls.append({
                    'original': f'{url_fragment} ... {playlist_id}',
                    'reconstructed': reconstructed_url,
                    'method': 'proximity_matching'
                })
        
        # Remove duplicates
        unique_urls = {}
        for item in reconstructed_urls:
            url = item['reconstructed']
            if url not in unique_urls:
                unique_urls[url] = item
        
        return list(unique_urls.values())
    
    def is_valid_playlist_url(self, url):
        """Check if a URL is a valid YouTube playlist URL"""
        if not url:
            return False
        
        # Must contain youtube and playlist
        if 'youtube' not in url.lower() or 'playlist' not in url.lower():
            return False
        
        # Must have a list parameter with an ID
        list_match = re.search(r'list=([a-zA-Z0-9_-]+)', url)
        if not list_match:
            return False
        
        # The ID should be at least 10 characters
        playlist_id = list_match.group(1)
        if len(playlist_id) < 10:
            return False
        
        return True
    
    def extract_all_youtube_urls(self, text):
        """Extract all YouTube URLs, both complete and reconstructed"""
        all_urls = set()
        
        # Find complete URLs
        complete_url_pattern = r'https?://(?:www\.)?(?:youtube\.com|youtu\.be)/[^\s<>"\'{}\\|^`\[\]]+[^\s<>"\'{}\\|^`\[\].,;:!?\)\]]'
        complete_matches = re.finditer(complete_url_pattern, text, re.IGNORECASE)
        
        for match in complete_matches:
            url = match.group(0)
            # Clean up common suffixes
            url = re.sub(r'[.,;:!?\)\]]+$', '', url)
            all_urls.add(url)
        
        # Add reconstructed URLs
        reconstructed = self.reconstruct_urls(text)
        for item in reconstructed:
            all_urls.add(item['reconstructed'])
        
        # Categorize URLs
        results = {
            'all_urls': list(all_urls),
            'complete_playlist_urls': [],
            'video_urls': [],
            'other_urls': [],
            'reconstructed': reconstructed
        }
        
        for url in all_urls:
            if self.is_valid_playlist_url(url):
                results['complete_playlist_urls'].append(url)
            elif re.search(r'watch\?v=([a-zA-Z0-9_-]{11})', url) or re.search(r'youtu\.be/([a-zA-Z0-9_-]{11})', url):
                results['video_urls'].append(url)
            else:
                results['other_urls'].append(url)
        
        return results

def test_url_reconstruction():
    """Test URL reconstruction with various edge cases"""
    
    # Test cases including real examples
    test_cases = [
        {
            'name': 'Truncated playlist URL',
            'text': 'Check out this playlist: https://youtube.com/playlist?list',
            'expected_ids': []
        },
        {
            'name': 'Separated playlist ID',
            'text': 'The playlist URL is https://youtube.com/playlist?list and the ID is PLp0u93QMy5vCYzE4OQydGEMEJjRizGDA',
            'expected_ids': ['PLp0u93QMy5vCYzE4OQydGEMEJjRizGDA']
        },
        {
            'name': 'Multiple playlist IDs',
            'text': '''Here are my playlists:
            https://youtube.com/playlist?list=PLp0u93QMy5vCYzE4OQydGEMEJjRizGDA
            https://youtube.com/playlist?list=PLu9i8x5U9PHhmD9K-5WY4EB12vyhL''',
            'expected_ids': ['PLp0u93QMy5vCYzE4OQydGEMEJjRizGDA', 'PLu9i8x5U9PHhmD9K-5WY4EB12vyhL']
        },
        {
            'name': 'URL-encoded playlist',
            'text': 'Link: youtube.com%2Fplaylist%3Flist%3DPLp0u93QMy5vCYzE4OQydGEMEJjRizGDA',
            'expected_ids': ['PLp0u93QMy5vCYzE4OQydGEMEJjRizGDA']
        },
        {
            'name': 'Orphaned playlist IDs',
            'text': 'My playlists: PLp0u93QMy5vCYzE4OQydGEMEJjRizGDA and PLu9i8x5U9PHhmD9K-5WY4EB12vyhL',
            'expected_ids': ['PLp0u93QMy5vCYzE4OQydGEMEJjRizGDA', 'PLu9i8x5U9PHhmD9K-5WY4EB12vyhL']
        },
        {
            'name': 'Mixed content with partial URLs',
            'text': '''Here's some content with playlist?list=PLp0u93QMy5vCYzE4OQydGEMEJjRizGDA 
            and another one: youtube.com/playlist?list without the ID but here it is: PLu9i8x5U9PHhmD9K-5WY4EB12vyhL''',
            'expected_ids': ['PLp0u93QMy5vCYzE4OQydGEMEJjRizGDA', 'PLu9i8x5U9PHhmD9K-5WY4EB12vyhL']
        }
    ]
    
    reconstructor = URLReconstructor()
    results = []
    
    print("🧪 TESTING URL RECONSTRUCTION")
    print("=" * 80)
    
    for test_case in test_cases:
        print(f"\n📝 Test: {test_case['name']}")
        print(f"   Input text: {test_case['text'][:100]}...")
        
        # Find playlist IDs
        found_ids = reconstructor.find_playlist_ids(test_case['text'])
        print(f"   Found IDs: {found_ids}")
        
        # Reconstruct URLs
        reconstructed = reconstructor.reconstruct_urls(test_case['text'])
        print(f"   Reconstructed: {len(reconstructed)} URLs")
        
        for item in reconstructed:
            print(f"     Original: {item['original']}")
            print(f"     Reconstructed: {item['reconstructed']}")
            print(f"     Method: {item['method']}")
        
        # Extract all URLs
        all_results = reconstructor.extract_all_youtube_urls(test_case['text'])
        
        # Check against expected
        success = True
        for expected_id in test_case['expected_ids']:
            found = any(expected_id in url for url in all_results['complete_playlist_urls'])
            if not found:
                success = False
                print(f"   ❌ Missing expected ID: {expected_id}")
            else:
                print(f"   ✅ Found expected ID: {expected_id}")
        
        test_result = {
            'test_name': test_case['name'],
            'success': success,
            'found_ids': found_ids,
            'reconstructed_count': len(reconstructed),
            'complete_playlist_urls': all_results['complete_playlist_urls']
        }
        
        results.append(test_result)
    
    # Save results
    with open('url_reconstruction_test_results.json', 'w') as f:
        json.dump(results, f, indent=2)
    
    # Summary
    print("\n📊 TEST SUMMARY:")
    successful = sum(1 for r in results if r['success'])
    print(f"   Total tests: {len(results)}")
    print(f"   Successful: {successful}")
    print(f"   Failed: {len(results) - successful}")
    print(f"   Success rate: {(successful / len(results)) * 100:.1f}%")
    
    print(f"\n💾 Results saved to url_reconstruction_test_results.json")
    
    return results

def create_enhanced_extract_links_function():
    """Generate the enhanced extract_links function for simple_workflow.py"""
    
    code = '''def step4_extract_links_enhanced(doc_content, doc_text=""):
    """Enhanced link extraction with URL reconstruction for incomplete playlist URLs"""
    print("Step 4: Enhanced link extraction with URL reconstruction...")
    
    # Import URL reconstructor inline to avoid dependencies
    import re
    
    # Initialize URL reconstructor patterns
    incomplete_patterns = [
        (r'(https?://(?:www\\.)?youtube\\.com/playlist\\?list)(?:$|\\s|["\\\'])', r'\\1='),
        (r'(playlist\\?list)(?:$|\\s|["\\\'])', r'https://www.youtube.com/\\1='),
        (r'playlist\\s*\\?\\s*list\\s*=?\\s*([a-zA-Z0-9_-]+)', r'https://www.youtube.com/playlist?list=\\1'),
        (r'list\\s*=\\s*([a-zA-Z0-9_-]{10,})', r'https://www.youtube.com/playlist?list=\\1'),
    ]
    
    # Combine HTML content and plain text for comprehensive link extraction
    combined_content = doc_content + " " + doc_text
    print(f"  Processing {len(combined_content)} characters of content")
    
    links = {
        'youtube': [],
        'drive_files': [],
        'drive_folders': [],
        'all_links': []
    }
    
    # First, find and reconstruct any incomplete URLs
    print("  🔧 Reconstructing incomplete URLs...")
    reconstructed_urls = []
    
    for pattern, replacement in incomplete_patterns:
        matches = re.finditer(pattern, combined_content, re.IGNORECASE)
        for match in matches:
            if isinstance(replacement, str) and '\\\\1' in replacement:
                reconstructed = re.sub(pattern, replacement, match.group(0))
            else:
                reconstructed = replacement
            
            if 'playlist?list=' in reconstructed and len(reconstructed.split('list=')[-1]) >= 10:
                reconstructed_urls.append(reconstructed)
                print(f"    Reconstructed: {reconstructed}")
    
    # Find orphaned playlist IDs
    playlist_id_pattern = r'(?:PL[a-zA-Z0-9_-]{10,})'
    orphaned_ids = re.findall(playlist_id_pattern, combined_content)
    
    for playlist_id in orphaned_ids:
        # Check if this ID is already part of a complete URL
        if not re.search(f'playlist\\?list={playlist_id}', combined_content):
            reconstructed_url = f'https://www.youtube.com/playlist?list={playlist_id}'
            reconstructed_urls.append(reconstructed_url)
            print(f"    Reconstructed from orphaned ID: {reconstructed_url}")
    
    # Add reconstructed URLs to the content for extraction
    enhanced_content = combined_content + " " + " ".join(reconstructed_urls)
    
    # Now perform standard extraction on enhanced content
    # [Rest of the standard extraction code continues...]
'''
    
    print("📝 Enhanced extract_links function code generated")
    print("This code can be integrated into simple_workflow.py")
    
    return code

if __name__ == "__main__":
    # Run tests
    test_url_reconstruction()
    
    # Generate enhanced function
    print("\n" + "=" * 80)
    enhanced_code = create_enhanced_extract_links_function()
    
    # Save the enhanced function
    with open('enhanced_extract_links_function.py', 'w') as f:
        f.write(enhanced_code)
    
    print("💾 Enhanced function saved to enhanced_extract_links_function.py")