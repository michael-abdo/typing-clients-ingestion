#!/usr/bin/env python3
"""
Document patterns in over-delivery rows (focus on Connor Lewis with 11 files)
"""

import json
import pandas as pd
from datetime import datetime
from utils.clean_file_mapper import CleanFileMapper
import os
from collections import defaultdict

def document_discovery_patterns():
    print('=== DOCUMENTING DISCOVERY PATTERNS ===')

    # Load analysis and mappings
    clean_mapper = CleanFileMapper()
    clean_mapper.map_all_files()
    df = pd.read_csv('outputs/output.csv')

    # Find Connor Lewis specifically
    connor_row = df[df['name'].str.contains('Connor Lewis', case=False, na=False)]
    if not connor_row.empty:
        connor_row_id = str(connor_row.iloc[0]['row_id'])
        connor_files = clean_mapper.get_row_files(connor_row_id)
        print(f'Connor Lewis (Row {connor_row_id}): {len(connor_files)} files found')
    else:
        connor_row_id = None
        connor_files = []
        print('Connor Lewis not found in CSV')

    # Pattern documentation structure
    pattern_doc = {
        'documentation_timestamp': datetime.now().isoformat(),
        'purpose': 'Document successful discovery patterns for replication',
        'connor_lewis_case_study': {},
        'successful_discovery_patterns': {},
        'failure_mode_analysis': {},
        'replication_strategies': {},
        'metadata_quality_indicators': {}
    }

    # Connor Lewis detailed case study
    if connor_files and connor_row_id:
        connor_data = connor_row.iloc[0]
        
        # Analyze Connor's files
        file_analysis = []
        file_types = defaultdict(int)
        file_sizes = []
        
        for file_path in connor_files:
            file_info = {
                'filename': os.path.basename(file_path),
                'full_path': file_path,
                'file_type': os.path.splitext(file_path)[1].lower(),
                'file_size': os.path.getsize(file_path) if os.path.exists(file_path) else 0,
                'directory': os.path.dirname(file_path)
            }
            file_analysis.append(file_info)
            file_types[file_info['file_type']] += 1
            if file_info['file_size'] > 0:
                file_sizes.append(file_info['file_size'])
        
        pattern_doc['connor_lewis_case_study'] = {
            'row_id': connor_row_id,
            'name': connor_data['name'],
            'personality_type': connor_data['type'],
            'email': connor_data.get('email', 'unknown'),
            'youtube_status': connor_data.get('youtube_status', 'nan'),
            'drive_status': connor_data.get('drive_status', 'nan'),
            'youtube_playlist': str(connor_data.get('youtube_playlist', 'nan')),
            'drive_playlist': str(connor_data.get('drive_playlist', 'nan')),
            'file_count': len(connor_files),
            'file_types': dict(file_types),
            'average_file_size': sum(file_sizes) / len(file_sizes) if file_sizes else 0,
            'total_content_size': sum(file_sizes),
            'file_details': file_analysis,
            'discovery_success_factors': []
        }
        
        # Analyze why Connor's discovery was successful
        success_factors = []
        if len(connor_files) > 5:
            success_factors.append('high_file_count_indicates_bulk_download')
        if '.mp4' in file_types:
            success_factors.append('video_content_successfully_preserved')
        if sum(file_sizes) > 100 * 1024 * 1024:  # > 100MB
            success_factors.append('substantial_content_size_preserved')
        if len(set(os.path.dirname(f) for f in connor_files)) == 1:
            success_factors.append('files_in_single_directory_suggest_batch_operation')
        
        pattern_doc['connor_lewis_case_study']['discovery_success_factors'] = success_factors

    # Analyze patterns across all over-delivery rows
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

    # Pattern analysis
    patterns = {
        'status_override_pattern': {'count': 0, 'description': 'Files found despite failed/pending status'},
        'directory_preservation_pattern': {'count': 0, 'description': 'Files preserved in download directories'},
        'metadata_mismatch_pattern': {'count': 0, 'description': 'Metadata exists but status not updated'},
        'bulk_download_pattern': {'count': 0, 'description': 'Multiple files suggest successful bulk operation'},
        'mixed_source_pattern': {'count': 0, 'description': 'Files from both YouTube and Drive sources'}
    }

    high_value_discoveries = []
    
    for row_id in over_delivery_rows:
        row_data = df[df['row_id'] == row_id]
        if row_data.empty:
            continue
            
        row = row_data.iloc[0]
        files = clean_mapper.get_row_files(row_id)
        
        # Pattern detection
        youtube_status = row.get('youtube_status', 'nan')
        drive_status = row.get('drive_status', 'nan')
        
        if youtube_status in ['failed', 'pending'] or drive_status in ['failed', 'pending']:
            patterns['status_override_pattern']['count'] += 1
        
        if len(files) > 1:
            patterns['bulk_download_pattern']['count'] += 1
        
        file_dirs = set(os.path.dirname(f) for f in files)
        if len(file_dirs) == 1:
            patterns['directory_preservation_pattern']['count'] += 1
        
        youtube_files = [f for f in files if 'youtube_downloads' in f]
        drive_files = [f for f in files if 'drive_downloads' in f]
        if youtube_files and drive_files:
            patterns['mixed_source_pattern']['count'] += 1
        
        # High-value discovery criteria
        total_size = sum(os.path.getsize(f) if os.path.exists(f) else 0 for f in files)
        if len(files) >= 5 or total_size > 50 * 1024 * 1024:  # 5+ files or 50MB+
            high_value_discoveries.append({
                'row_id': row_id,
                'name': row['name'],
                'file_count': len(files),
                'total_size_mb': total_size / (1024 * 1024),
                'personality_type': row['type']
            })

    pattern_doc['successful_discovery_patterns'] = patterns
    pattern_doc['high_value_discoveries'] = high_value_discoveries

    # Replication strategies
    replication_strategies = {
        'metadata_scanning_strategy': {
            'description': 'Scan for definitive metadata files even when status shows failed',
            'implementation': 'Use filename-based metadata matching instead of status-based filtering',
            'success_rate': f"{patterns['status_override_pattern']['count']}/{len(over_delivery_rows)} cases"
        },
        'directory_enumeration_strategy': {
            'description': 'Enumerate download directories to find preserved files',
            'implementation': 'Scan download directories for content files regardless of tracking status',
            'success_rate': f"{patterns['directory_preservation_pattern']['count']}/{len(over_delivery_rows)} cases"
        },
        'bulk_operation_detection': {
            'description': 'Identify patterns suggesting successful bulk downloads',
            'implementation': 'Look for multiple files with similar timestamps in same directory',
            'success_rate': f"{patterns['bulk_download_pattern']['count']}/{len(over_delivery_rows)} cases"
        }
    }
    
    pattern_doc['replication_strategies'] = replication_strategies

    # Metadata quality indicators
    quality_indicators = {
        'definitive_metadata_presence': 'Strong indicator of successful mapping',
        'file_count_consistency': 'Multiple files suggest successful bulk operation',
        'directory_clustering': 'Files in same directory indicate batch processing',
        'file_size_distribution': 'Substantial file sizes indicate real content vs placeholders',
        'extension_variety': 'Multiple file types suggest comprehensive download'
    }
    
    pattern_doc['metadata_quality_indicators'] = quality_indicators

    # Save documentation
    docs_dir = 'documentation'
    os.makedirs(docs_dir, exist_ok=True)
    
    pattern_file = os.path.join(docs_dir, f'discovery_patterns_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
    with open(pattern_file, 'w') as f:
        json.dump(pattern_doc, f, indent=2)

    # Create summary report
    summary_data = []
    for pattern_name, pattern_info in patterns.items():
        summary_data.append({
            'pattern_name': pattern_name,
            'occurrences': pattern_info['count'],
            'description': pattern_info['description'],
            'success_rate': f"{pattern_info['count']}/{len(over_delivery_rows)}"
        })
    
    summary_df = pd.DataFrame(summary_data)
    summary_file = os.path.join(docs_dir, f'pattern_summary_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
    summary_df.to_csv(summary_file, index=False)

    # Print results
    print(f'\\n✅ Discovery patterns documented:')
    print(f'   Documentation saved to: {pattern_file}')
    print(f'   Summary CSV: {summary_file}')
    
    if connor_files:
        print(f'\\n   Connor Lewis case study:')
        print(f'     - {len(connor_files)} files successfully discovered')
        print(f'     - Total content: {sum(os.path.getsize(f) if os.path.exists(f) else 0 for f in connor_files) / (1024*1024):.1f} MB')
        print(f'     - Success factors: {len(pattern_doc["connor_lewis_case_study"]["discovery_success_factors"])}')
    
    print(f'\\n   Key patterns discovered:')
    for pattern_name, pattern_info in patterns.items():
        if pattern_info['count'] > 0:
            print(f'     - {pattern_name}: {pattern_info["count"]} cases')
    
    print(f'\\n   High-value discoveries: {len(high_value_discoveries)} rows with 5+ files or 50MB+ content')

    # Verification
    issues = []
    if not connor_files and connor_row_id:
        issues.append('Connor Lewis found but no files mapped')
    if sum(p['count'] for p in patterns.values()) == 0:
        issues.append('No discovery patterns identified')

    if issues:
        print(f'\\n⚠️  Verification issues: {issues}')
    else:
        print(f'\\n✅ Verification passed: Discovery patterns documented successfully')
    
    return pattern_file, len(connor_files), dict(patterns)

if __name__ == "__main__":
    document_discovery_patterns()