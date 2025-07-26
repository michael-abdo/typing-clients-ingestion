#!/usr/bin/env python3
"""Analyze remaining 79 orphaned files for additional recovery opportunities"""

import boto3
import pandas as pd
import json
import re
from collections import defaultdict

def get_remaining_orphaned_files():
    """Get current inventory of remaining orphaned files"""
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    
    response = s3.list_objects_v2(Bucket=bucket, Prefix="files/")
    files = []
    
    if 'Contents' in response:
        for obj in response['Contents']:
            if not obj['Key'].endswith('/'):
                filename = obj['Key'].split('/')[-1]
                uuid = filename.split('.')[0]
                extension = filename.split('.')[-1] if '.' in filename else 'no_ext'
                
                files.append({
                    'uuid': uuid,
                    'filename': filename,
                    'extension': extension,
                    'size_bytes': obj['Size'],
                    'size_mb': obj['Size'] / (1024 * 1024),
                    'last_modified': obj['LastModified'],
                    's3_key': obj['Key']
                })
    
    return sorted(files, key=lambda x: x['size_mb'], reverse=True)

def analyze_large_media_files(files, size_threshold_mb=100):
    """Analyze large media files for potential client identification"""
    print(f"🎥 ANALYZING LARGE MEDIA FILES (>{size_threshold_mb}MB)")
    print("=" * 50)
    
    large_files = [f for f in files 
                  if f['extension'] in ['mp4', 'mp3', 'webm', 'm4a'] 
                  and f['size_mb'] > size_threshold_mb]
    
    if not large_files:
        print(f"   No large media files found above {size_threshold_mb}MB")
        return []
    
    print(f"   Found {len(large_files)} large media files:")
    for file_info in large_files:
        print(f"   - {file_info['filename']} ({file_info['size_mb']:.1f} MB)")
    
    # These large files are most likely to contain valuable client content
    # Return them for potential manual review or advanced analysis
    return large_files

def group_files_by_patterns(files):
    """Group remaining files by various patterns to find batch uploads"""
    print(f"\n📊 GROUPING FILES BY PATTERNS")
    print("=" * 40)
    
    patterns = {
        'by_extension': defaultdict(list),
        'by_size_range': defaultdict(list),
        'by_hour': defaultdict(list),
        'by_minute': defaultdict(list)
    }
    
    for file_info in files:
        # Group by extension
        patterns['by_extension'][file_info['extension']].append(file_info)
        
        # Group by size range
        size_mb = file_info['size_mb']
        if size_mb < 1:
            size_range = "< 1MB"
        elif size_mb < 10:
            size_range = "1-10MB"
        elif size_mb < 50:
            size_range = "10-50MB"
        elif size_mb < 200:
            size_range = "50-200MB"
        else:
            size_range = "> 200MB"
        
        patterns['by_size_range'][size_range].append(file_info)
        
        # Group by upload hour (might indicate batch uploads)
        hour_key = file_info['last_modified'].strftime('%Y-%m-%d %H:00')
        patterns['by_hour'][hour_key].append(file_info)
        
        # Group by upload minute (very tight batch uploads)
        minute_key = file_info['last_modified'].strftime('%Y-%m-%d %H:%M')
        patterns['by_minute'][minute_key].append(file_info)
    
    print(f"📋 File Type Distribution:")
    for ext, files_list in sorted(patterns['by_extension'].items()):
        total_size = sum(f['size_mb'] for f in files_list)
        print(f"   {ext}: {len(files_list)} files ({total_size:.1f} MB)")
    
    print(f"\n⏰ Batch Upload Analysis:")
    
    # Find tight batch uploads (same minute)
    tight_batches = {k: v for k, v in patterns['by_minute'].items() if len(v) >= 3}
    if tight_batches:
        print(f"   Tight batches (same minute): {len(tight_batches)} groups")
        for minute, files_list in sorted(tight_batches.items())[:5]:  # Show top 5
            print(f"   - {minute}: {len(files_list)} files")
    
    # Find hourly batches
    hourly_batches = {k: v for k, v in patterns['by_hour'].items() if len(v) >= 5}
    if hourly_batches:
        print(f"   Hourly batches: {len(hourly_batches)} groups")
        for hour, files_list in sorted(hourly_batches.items())[:3]:  # Show top 3
            print(f"   - {hour}: {len(files_list)} files")
    
    return patterns

def analyze_filename_similarity(files):
    """Look for files with similar names or patterns that might indicate same client"""
    print(f"\n🔍 ANALYZING FILENAME SIMILARITY")
    print("=" * 40)
    
    # Group by file name patterns
    name_patterns = defaultdict(list)
    
    for file_info in files:
        uuid = file_info['uuid']
        
        # Extract potential patterns from UUID structure
        # UUIDs are random, but if generated in sequence, first parts might be similar
        uuid_prefix = uuid[:8]  # First 8 characters
        name_patterns[uuid_prefix].append(file_info)
    
    # Find groups with multiple files (might indicate batch generation)
    significant_groups = {k: v for k, v in name_patterns.items() if len(v) >= 2}
    
    if significant_groups:
        print(f"   Found {len(significant_groups)} UUID prefix groups with multiple files:")
        for prefix, files_list in sorted(significant_groups.items(), key=lambda x: len(x[1]), reverse=True)[:5]:
            print(f"   - {prefix}*: {len(files_list)} files")
            for file_info in files_list[:3]:  # Show first 3
                print(f"     * {file_info['filename']} ({file_info['size_mb']:.1f} MB)")
    else:
        print(f"   No significant UUID prefix groupings found")
    
    return significant_groups

def check_for_missing_dan_jane_files():
    """Check if there's a Dan Jane mentioned in logs that might map to remaining files"""
    print(f"\n🔍 CHECKING FOR SPECIFIC CLIENT CLUES")
    print("=" * 40)
    
    # From our earlier analysis, we saw 'Dan Jane' in one of the JSON files
    # Let's see if we can find more files that might belong to this client
    
    # Load current CSV to check if Dan Jane exists
    df = pd.read_csv('outputs/output.csv')
    
    # Search for Dan Jane variations
    dan_jane_variations = ['dan jane', 'dan', 'jane', 'daniel jane', 'danielle jane']
    
    potential_clients = []
    for _, row in df.iterrows():
        name_lower = str(row['name']).lower()
        for variation in dan_jane_variations:
            if variation in name_lower:
                potential_clients.append({
                    'row_id': row['row_id'],
                    'name': row['name'],
                    'email': row.get('email', ''),
                    'match_variation': variation
                })
                break
    
    if potential_clients:
        print(f"   Found potential Dan Jane matches in CSV:")
        for client in potential_clients:
            print(f"   - Row {client['row_id']}: {client['name']} (matched: '{client['match_variation']}')")
    else:
        print(f"   No Dan Jane matches found in current CSV")
    
    return potential_clients

def suggest_manual_review_candidates(files):
    """Suggest files that are good candidates for manual review"""
    print(f"\n🎯 MANUAL REVIEW CANDIDATES")
    print("=" * 40)
    
    # Prioritize large media files for manual review
    large_media = [f for f in files 
                  if f['extension'] in ['mp4', 'mp3', 'webm', 'm4a'] 
                  and f['size_mb'] > 50]
    
    # Prioritize by size (larger files more likely to contain valuable content)
    manual_candidates = sorted(large_media, key=lambda x: x['size_mb'], reverse=True)[:10]
    
    if manual_candidates:
        print(f"   Top {len(manual_candidates)} candidates for manual review:")
        for i, file_info in enumerate(manual_candidates, 1):
            print(f"   {i}. {file_info['filename']} ({file_info['size_mb']:.1f} MB)")
            print(f"      UUID: {file_info['uuid']}")
            print(f"      S3 Path: {file_info['s3_key']}")
    else:
        print(f"   No high-priority candidates for manual review")
    
    return manual_candidates

def generate_recovery_recommendations(files, patterns, large_files, manual_candidates):
    """Generate final recommendations for recovering remaining files"""
    print(f"\n📋 RECOVERY RECOMMENDATIONS")
    print("=" * 40)
    
    total_files = len(files)
    total_size_mb = sum(f['size_mb'] for f in files)
    
    print(f"📊 Remaining File Summary:")
    print(f"   Files remaining: {total_files}")
    print(f"   Total size: {total_size_mb:.1f} MB")
    print(f"   Average size: {total_size_mb/total_files:.1f} MB per file")
    
    # Calculate recovery potential
    large_file_count = len(large_files)
    large_file_size = sum(f['size_mb'] for f in large_files)
    
    print(f"\n🎯 High-Value Recovery Targets:")
    print(f"   Large files (>100MB): {large_file_count} files ({large_file_size:.1f} MB)")
    print(f"   Manual review candidates: {len(manual_candidates)} files")
    
    # Recommendations
    recommendations = []
    
    if large_file_count > 0:
        recommendations.append(f"Priority 1: Review {large_file_count} large media files - likely contain valuable client content")
    
    if len(manual_candidates) > 0:
        recommendations.append(f"Priority 2: Manual content review of top {len(manual_candidates)} files for client identification")
    
    json_files = len(patterns['by_extension'].get('json', []))
    if json_files > 0:
        recommendations.append(f"Priority 3: Deep analysis of remaining {json_files} JSON files for missed metadata")
    
    batch_opportunities = sum(1 for files_list in patterns['by_minute'].values() if len(files_list) >= 3)
    if batch_opportunities > 0:
        recommendations.append(f"Priority 4: Investigate {batch_opportunities} batch upload groups for common ownership")
    
    print(f"\n🚀 Recommended Next Steps:")
    for i, rec in enumerate(recommendations, 1):
        print(f"   {i}. {rec}")
    
    if not recommendations:
        print(f"   No immediate high-confidence recovery opportunities identified")
        print(f"   Consider archiving remaining files as 'unidentified' with preservation for future reference")
    
    return recommendations

def main():
    """Main analysis function"""
    print("🔍 ANALYZING REMAINING 79 ORPHANED FILES")
    print("=" * 60)
    
    # Get current file inventory
    files = get_remaining_orphaned_files()
    print(f"📊 Current Status: {len(files)} orphaned files remaining")
    
    if len(files) == 0:
        print("🎉 No orphaned files remaining - recovery complete!")
        return
    
    # Analyze large media files
    large_files = analyze_large_media_files(files)
    
    # Group by patterns
    patterns = group_files_by_patterns(files)
    
    # Analyze filename similarity
    filename_groups = analyze_filename_similarity(files)
    
    # Check for specific client clues
    potential_clients = check_for_missing_dan_jane_files()
    
    # Suggest manual review candidates
    manual_candidates = suggest_manual_review_candidates(files)
    
    # Generate final recommendations
    recommendations = generate_recovery_recommendations(files, patterns, large_files, manual_candidates)
    
    print(f"\n✅ Analysis complete!")
    return {
        'files': files,
        'large_files': large_files,
        'patterns': patterns,
        'manual_candidates': manual_candidates,
        'recommendations': recommendations
    }

if __name__ == "__main__":
    results = main()