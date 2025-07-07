#!/usr/bin/env python3
"""
PLAYLIST ID VALIDATION
Check which playlist IDs actually exist on YouTube
"""

import requests
import time

def validate_playlist_id(playlist_id):
    """Check if a playlist ID exists on YouTube by making a simple HTTP request"""
    
    # Construct the YouTube playlist URL
    url = f"https://www.youtube.com/playlist?list={playlist_id}"
    
    try:
        # Make a head request to check if the playlist exists
        response = requests.head(url, timeout=10, allow_redirects=True)
        
        # If we get a 200 status, the playlist likely exists
        if response.status_code == 200:
            return True, "Playlist exists"
        elif response.status_code == 404:
            return False, "Playlist not found"
        else:
            return False, f"HTTP {response.status_code}"
    
    except requests.RequestException as e:
        return False, f"Request failed: {e}"

def main():
    """Validate all playlist IDs in question"""
    
    print("🔍 PLAYLIST ID VALIDATION")
    print("=" * 80)
    
    # Expected playlist IDs (from manual analysis)
    expected_ids = [
        "PLp0u93QMy5vCYzE4OQydGEMEJjRizGDA",
        "PLu9i8x5U9PHhmD9K-5WY4EB12vyhL"
    ]
    
    # Found playlist IDs (from automated extraction)
    found_ids = [
        "PLpOu93QMy5fVCzxYe4OOydGEMEjlRziGD",
        "PLJu98jx5U9PHhImDP9K-5WrV4EB12wyhL"
    ]
    
    print("\n📋 EXPECTED PLAYLIST IDs (from manual analysis):")
    for i, playlist_id in enumerate(expected_ids):
        print(f"   {i+1}. {playlist_id}")
        is_valid, message = validate_playlist_id(playlist_id)
        status = "✅ VALID" if is_valid else "❌ INVALID"
        print(f"      Status: {status} - {message}")
        time.sleep(1)  # Be respectful to YouTube servers
    
    print("\n📋 FOUND PLAYLIST IDs (from automated extraction):")
    for i, playlist_id in enumerate(found_ids):
        print(f"   {i+1}. {playlist_id}")
        is_valid, message = validate_playlist_id(playlist_id)
        status = "✅ VALID" if is_valid else "❌ INVALID"
        print(f"      Status: {status} - {message}")
        time.sleep(1)  # Be respectful to YouTube servers
    
    # Additional analysis
    print("\n🔍 ADDITIONAL ANALYSIS:")
    print("   Checking for common character substitution patterns...")
    
    # Test some common substitutions
    substitution_tests = [
        # Test if 0 vs O makes a difference
        ("PLp0u93QMy5vCYzE4OQydGEMEJjRizGDA", "PLpOu93QMy5vCYzE4OQydGEMEJjRizGDA"),
        # Test partial matches
        ("PLp0u93QMy5vCYzE4OQydGEMEJjRizGD", "PLpOu93QMy5fVCzxYe4OOydGEMEjlRziG"),
    ]
    
    for original, modified in substitution_tests:
        print(f"   Testing: {original} vs {modified}")
        is_valid_orig, msg_orig = validate_playlist_id(original)
        is_valid_mod, msg_mod = validate_playlist_id(modified)
        print(f"     Original: {'✅' if is_valid_orig else '❌'} {msg_orig}")
        print(f"     Modified: {'✅' if is_valid_mod else '❌'} {msg_mod}")
        time.sleep(1)
    
    print("\n📊 CONCLUSIONS:")
    print("   This validation will help determine:")
    print("   1. Which playlist IDs actually exist on YouTube")
    print("   2. Whether the discrepancy is due to extraction errors")
    print("   3. If manual analysis or automated extraction is correct")
    print("   4. Common patterns in character misrecognition")

if __name__ == "__main__":
    main()