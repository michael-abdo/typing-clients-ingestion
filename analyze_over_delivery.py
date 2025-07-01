#!/usr/bin/env python3
"""
Analyze 31 over-delivery rows to understand discovery mechanisms beyond formal tracking
"""

import json
import pandas as pd
from datetime import datetime
from utils.clean_file_mapper import CleanFileMapper
import os
from collections import defaultdict

def analyze_over_delivery():
    print('=== ANALYZING OVER-DELIVERY ROWS ===')

    # Load current mappings and CSV
    clean_mapper = CleanFileMapper()
    clean_mapper.map_all_files()
    df = pd.read_csv('outputs/output.csv')

    # Identify expected vs actual rows with files
    expected_rows = set()
    actual_rows = set(clean_mapper.row_to_files.keys())

    for idx, row in df.iterrows():
        row_id = str(row['row_id'])
        has_youtube = (pd.notna(row.get('youtube_playlist')) and 
                       'youtube.com' in str(row.get('youtube_playlist', '')) and
                       row.get('youtube_status') == 'completed')
        has_drive = (pd.notna(row.get('drive_playlist')) and 
                     'drive.google.com' in str(row.get('drive_playlist', '')) and
                     row.get('drive_status') == 'completed')
        if has_youtube or has_drive:
            expected_rows.add(row_id)

    over_delivery_rows = actual_rows - expected_rows
    
    print(f'Expected rows with files: {len(expected_rows)}')
    print(f'Actual rows with files: {len(actual_rows)}')
    print(f'Over-delivery rows: {len(over_delivery_rows)}')

    # Analyze over-delivery patterns
    over_delivery_analysis = {
        'analysis_timestamp': datetime.now().isoformat(),
        'summary': {
            'expected_rows': len(expected_rows),
            'actual_rows': len(actual_rows),
            'over_delivery_count': len(over_delivery_rows),
            'over_delivery_percentage': (len(over_delivery_rows) / len(expected_rows)) * 100
        },
        'discovery_mechanisms': {},
        'over_delivery_details': [],
        'patterns_discovered': {}
    }

    # Analyze each over-delivery row
    discovery_mechanisms = defaultdict(int)
    file_source_patterns = defaultdict(int)
    personality_type_patterns = defaultdict(int)
    file_count_distribution = defaultdict(int)

    for row_id in over_delivery_rows:
        # Get row information
        row_data = df[df['row_id'] == row_id]
        if row_data.empty:
            continue
            
        row = row_data.iloc[0]
        files = clean_mapper.get_row_files(row_id)
        
        # Analyze why this row has files
        discovery_reason = 'unknown'
        
        # Check status fields for clues
        youtube_status = row.get('youtube_status', 'nan')
        drive_status = row.get('drive_status', 'nan')
        youtube_playlist = str(row.get('youtube_playlist', 'nan'))
        drive_playlist = str(row.get('drive_playlist', 'nan'))
        
        if youtube_status in ['failed', 'pending'] and len(files) > 0:
            discovery_reason = 'files_despite_failed_status'
        elif pd.isna(row.get('youtube_playlist')) and pd.isna(row.get('drive_playlist')):
            discovery_reason = 'no_playlist_but_files_found'
        elif 'youtube.com' not in youtube_playlist and 'drive.google.com' not in drive_playlist:
            discovery_reason = 'invalid_playlist_urls_but_files_found'
        elif youtube_status != 'completed' and drive_status != 'completed':
            discovery_reason = 'incomplete_status_but_files_found'
        
        discovery_mechanisms[discovery_reason] += 1
        
        # File source analysis
        file_sources = set()
        for file_path in files:
            if 'youtube_downloads' in file_path:
                file_sources.add('youtube')
            elif 'drive_downloads' in file_path:
                file_sources.add('drive')
        
        for source in file_sources:
            file_source_patterns[source] += 1
        
        # Track patterns
        personality_type_patterns[row['type']] += 1
        file_count_distribution[len(files)] += 1
        
        # Detailed record
        over_delivery_analysis['over_delivery_details'].append({
            'row_id': row_id,
            'person_name': row['name'],
            'personality_type': row['type'],
            'email': row.get('email', 'unknown'),
            'file_count': len(files),
            'file_sources': list(file_sources),
            'files': [os.path.basename(f) for f in files],
            'youtube_status': youtube_status,
            'drive_status': drive_status,
            'youtube_playlist': youtube_playlist,
            'drive_playlist': drive_playlist,
            'discovery_reason': discovery_reason
        })

    # Store patterns
    over_delivery_analysis['discovery_mechanisms'] = dict(discovery_mechanisms)
    over_delivery_analysis['patterns_discovered'] = {
        'file_source_patterns': dict(file_source_patterns),
        'personality_type_patterns': dict(personality_type_patterns),
        'file_count_distribution': dict(file_count_distribution)
    }

    # Key insights
    insights = []
    
    # Top discovery mechanism
    top_mechanism = max(discovery_mechanisms.items(), key=lambda x: x[1]) if discovery_mechanisms else ('none', 0)
    insights.append(f"Primary discovery mechanism: {top_mechanism[0]} ({top_mechanism[1]} rows)")
    
    # File source bias
    if 'youtube' in file_source_patterns and 'drive' in file_source_patterns:
        youtube_count = file_source_patterns['youtube']
        drive_count = file_source_patterns['drive']
        source_bias = 'youtube' if youtube_count > drive_count else 'drive'
        insights.append(f"File source bias: {source_bias} ({max(youtube_count, drive_count)} vs {min(youtube_count, drive_count)})")
    
    # High-file-count outliers
    max_files = max(file_count_distribution.keys()) if file_count_distribution else 0
    if max_files > 5:
        high_count_rows = [detail for detail in over_delivery_analysis['over_delivery_details'] if detail['file_count'] >= 5]
        insights.append(f"High file count outliers: {len(high_count_rows)} rows with 5+ files")
    
    over_delivery_analysis['key_insights'] = insights

    # Save analysis
    analysis_dir = 'analysis'
    os.makedirs(analysis_dir, exist_ok=True)
    
    analysis_file = os.path.join(analysis_dir, f'over_delivery_analysis_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
    with open(analysis_file, 'w') as f:
        json.dump(over_delivery_analysis, f, indent=2)

    # CSV for detailed analysis
    if over_delivery_analysis['over_delivery_details']:
        details_df = pd.DataFrame(over_delivery_analysis['over_delivery_details'])
        csv_file = os.path.join(analysis_dir, f'over_delivery_details_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
        details_df.to_csv(csv_file, index=False)
    else:
        csv_file = None

    # Print results
    print(f'\\n✅ Over-delivery analysis completed:')
    print(f'   Analysis saved to: {analysis_file}')
    if csv_file:
        print(f'   Details CSV: {csv_file}')
    
    print(f'\\n   Discovery mechanisms:')
    for mechanism, count in sorted(discovery_mechanisms.items(), key=lambda x: x[1], reverse=True):
        print(f'     {mechanism}: {count} rows')
    
    print(f'\\n   Key insights:')
    for insight in insights:
        print(f'     - {insight}')

    # Verification
    issues = []
    if len(over_delivery_rows) != 31:
        issues.append(f'Expected 31 over-delivery rows, got {len(over_delivery_rows)}')
    if not discovery_mechanisms:
        issues.append('No discovery mechanisms identified')

    if issues:
        print(f'\\n⚠️  Verification issues: {issues}')
    else:
        print(f'\\n✅ Verification passed: Over-delivery analysis complete')
    
    return analysis_file, len(over_delivery_rows), dict(discovery_mechanisms)

if __name__ == "__main__":
    analyze_over_delivery()