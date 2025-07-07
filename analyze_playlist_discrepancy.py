#!/usr/bin/env python3
"""
PLAYLIST ID DISCREPANCY ANALYSIS
Compare the expected vs found playlist IDs for James Kirton to understand the differences
"""

def analyze_playlist_differences():
    """Analyze the character-by-character differences between expected and found playlist IDs"""
    
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
    
    print("🔍 PLAYLIST ID DISCREPANCY ANALYSIS")
    print("=" * 80)
    
    for i, (expected, found) in enumerate(zip(expected_ids, found_ids)):
        print(f"\n📋 Playlist {i+1}:")
        print(f"   Expected: {expected}")
        print(f"   Found:    {found}")
        
        # Character-by-character comparison
        print(f"   Length:   Expected={len(expected)}, Found={len(found)}")
        
        # Find differences
        differences = []
        min_len = min(len(expected), len(found))
        
        for j in range(min_len):
            if expected[j] != found[j]:
                differences.append({
                    'position': j,
                    'expected_char': expected[j],
                    'found_char': found[j]
                })
        
        # Check for length differences
        if len(expected) != len(found):
            differences.append({
                'position': min_len,
                'expected_char': expected[min_len:] if len(expected) > min_len else '<END>',
                'found_char': found[min_len:] if len(found) > min_len else '<END>'
            })
        
        if differences:
            print(f"   Differences ({len(differences)} found):")
            for diff in differences:
                print(f"     Position {diff['position']}: '{diff['expected_char']}' vs '{diff['found_char']}'")
        else:
            print(f"   ✅ No differences found")
    
    # Pattern analysis
    print(f"\n🔍 PATTERN ANALYSIS:")
    
    # Common prefix analysis
    for i, (expected, found) in enumerate(zip(expected_ids, found_ids)):
        common_prefix = ""
        for j in range(min(len(expected), len(found))):
            if expected[j] == found[j]:
                common_prefix += expected[j]
            else:
                break
        
        print(f"   Playlist {i+1} common prefix: '{common_prefix}' (length: {len(common_prefix)})")
        
        # Show the divergence point
        if len(common_prefix) < max(len(expected), len(found)):
            divergence_pos = len(common_prefix)
            expected_suffix = expected[divergence_pos:]
            found_suffix = found[divergence_pos:]
            print(f"     Divergence at position {divergence_pos}:")
            print(f"       Expected suffix: '{expected_suffix}'")
            print(f"       Found suffix:    '{found_suffix}'")
    
    # Look for common patterns
    print(f"\n🔍 COMMON PATTERNS:")
    
    # Check if these could be similar-looking characters
    similar_chars = {
        'o': '0',  # lowercase o vs zero
        '0': 'o',  # zero vs lowercase o
        'I': 'l',  # uppercase i vs lowercase L
        'l': 'I',  # lowercase L vs uppercase i
        '1': 'l',  # one vs lowercase L
        'l': '1',  # lowercase L vs one
        'J': 'I',  # uppercase J vs uppercase I
        'I': 'J',  # uppercase I vs uppercase J
        'v': 'u',  # v vs u
        'u': 'v',  # u vs v
        'f': 't',  # f vs t
        't': 'f',  # t vs f
        'Y': 'V',  # Y vs V
        'V': 'Y',  # V vs Y
        'h': 'b',  # h vs b
        'b': 'h',  # b vs h
        'j': 'i',  # j vs i
        'i': 'j',  # i vs j
        'M': 'N',  # M vs N
        'N': 'M',  # N vs M
        'E': 'F',  # E vs F
        'F': 'E',  # F vs E
        'R': 'P',  # R vs P
        'P': 'R',  # P vs R
        'z': 'r',  # z vs r
        'r': 'z',  # r vs z
        'G': 'C',  # G vs C
        'C': 'G',  # C vs G
        'D': 'O',  # D vs O
        'O': 'D',  # O vs D
        'A': 'R',  # A vs R
        'R': 'A',  # R vs A
        'w': 'v',  # w vs v
        'v': 'w',  # v vs w
        'H': 'N',  # H vs N
        'N': 'H',  # N vs H
        'K': 'R',  # K vs R
        'R': 'K',  # R vs K
        'W': 'V',  # W vs V
        'V': 'W',  # V vs W
        'y': 'v',  # y vs v
        'v': 'y',  # v vs y
        'L': 'I',  # L vs I
        'I': 'L',  # I vs L
    }
    
    for i, (expected, found) in enumerate(zip(expected_ids, found_ids)):
        print(f"   Playlist {i+1} character substitution analysis:")
        
        # Check if found could be a result of character misrecognition
        reconstructed = ""
        substitutions = []
        
        for j in range(min(len(expected), len(found))):
            expected_char = expected[j]
            found_char = found[j]
            
            if expected_char != found_char:
                # Check if it's a common substitution
                if expected_char in similar_chars and similar_chars[expected_char] == found_char:
                    substitutions.append(f"{expected_char}->{found_char} (similar chars)")
                elif found_char in similar_chars and similar_chars[found_char] == expected_char:
                    substitutions.append(f"{expected_char}->{found_char} (similar chars)")
                else:
                    substitutions.append(f"{expected_char}->{found_char}")
        
        if substitutions:
            print(f"     Substitutions: {', '.join(substitutions)}")
        else:
            print(f"     No character substitutions found")
    
    print(f"\n📊 SUMMARY:")
    print(f"   This appears to be a case of character misrecognition during extraction")
    print(f"   The playlist IDs are very similar but have subtle character differences")
    print(f"   Common causes:")
    print(f"   - OCR-like misrecognition of similar characters")
    print(f"   - Font rendering differences in the document")
    print(f"   - Different character encodings")
    print(f"   - Copy-paste errors with similar-looking characters")
    

if __name__ == "__main__":
    analyze_playlist_differences()