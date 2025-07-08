#!/usr/bin/env python3
import pandas as pd

df = pd.read_csv('outputs/output.csv')

print("FILTERING RESULTS ANALYSIS:")
print("=" * 50)

key_people = {
    'Carlos Arthur': {'youtube_expected': 1, 'drive_expected': 0},
    'Caroline Chiu': {'youtube_expected': 0, 'drive_expected': 0}, 
    'James Kirton': {'youtube_expected': 2, 'drive_expected': 0},
    'Florence': {'youtube_expected': 0, 'drive_expected': 0},
    'John Williams': {'youtube_expected': 4, 'drive_expected': 0},
    'Kiko': {'youtube_expected': 0, 'drive_expected': 1}
}

perfect_matches = 0
total = 0

for name, expected in key_people.items():
    person = df[df['name'].str.contains(name.split()[0], na=False)]
    if not person.empty:
        total += 1
        row = person.iloc[0]
        youtube_links = str(row.get('youtube_playlist', '')).split('|') if pd.notna(row.get('youtube_playlist')) else []
        drive_links = str(row.get('google_drive', '')).split('|') if pd.notna(row.get('google_drive')) else []
        youtube_links = [l for l in youtube_links if l and l != 'nan']
        drive_links = [l for l in drive_links if l and l != 'nan']
        
        print(f"\n{name} (Row {row.get('row_id', '?')}):")
        print(f"  YouTube: {len(youtube_links)} found vs {expected['youtube_expected']} expected")
        print(f"  Drive: {len(drive_links)} found vs {expected['drive_expected']} expected")
        
        if len(youtube_links) == expected['youtube_expected'] and len(drive_links) == expected['drive_expected']:
            print(f"  ✅ PERFECT MATCH")
            perfect_matches += 1
        else:
            print(f"  ⚠️  Difference detected")
            if youtube_links:
                print(f"    YouTube links: {youtube_links}")
            if drive_links:
                print(f"    Drive links: {drive_links}")

print(f"\n📊 SUMMARY:")
print(f"Perfect matches: {perfect_matches}/{total} ({perfect_matches/total*100:.1f}%)")