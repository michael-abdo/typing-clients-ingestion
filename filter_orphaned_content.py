#!/usr/bin/env python3
"""
Filter 111 orphaned files to exclude metadata/temp files, isolate content files for classification
"""

import json
import pandas as pd
from datetime import datetime
from utils.clean_file_mapper import CleanFileMapper
import os
from collections import defaultdict

def filter_orphaned_content():
    print('=== FILTERING ORPHANED CONTENT FILES ===')

    # Load current mappings
    clean_mapper = CleanFileMapper()
    clean_mapper.map_all_files()

    orphaned_files = clean_mapper.unmapped_files
    print(f'Total orphaned files to filter: {len(orphaned_files)}')

    # Define file categories
    CONTENT_EXTENSIONS = {
        'video': ['.mp4', '.mov', '.avi', '.mkv', '.webm', '.m4v'],
        'audio': ['.mp3', '.m4a', '.wav', '.aac', '.ogg', '.flac'],
        'document': ['.pdf', '.docx', '.doc', '.txt', '.rtf', '.odt'],
        'subtitle': ['.srt', '.vtt', '.sub', '.ass', '.ssa'],
        'web': ['.html', '.htm', '.mhtml']
    }
    
    METADATA_EXTENSIONS = ['.json', '.xml', '.yaml', '.yml', '.info']
    TEMP_EXTENSIONS = ['.part', '.tmp', '.temp', '.ytdl', '.crdownload', '.lock']
    SYSTEM_EXTENSIONS = ['.log', '.db', '.cache', '.idx']

    # Classification results
    filter_results = {
        'filter_timestamp': datetime.now().isoformat(),
        'total_orphaned_files': len(orphaned_files),
        'content_files': [],
        'metadata_files': [],
        'temporary_files': [],
        'system_files': [],
        'unknown_files': [],
        'content_categories': {},
        'filter_summary': {}
    }

    # Content category tracking
    content_categories = defaultdict(list)
    
    # Filter files
    for file_path in orphaned_files:
        if not os.path.exists(file_path):
            continue
            
        filename = os.path.basename(file_path)
        file_ext = os.path.splitext(filename)[1].lower()
        file_size = os.path.getsize(file_path)
        
        file_info = {
            'file_path': file_path,
            'filename': filename,
            'extension': file_ext,
            'size_bytes': file_size,
            'size_mb': file_size / (1024 * 1024),
            'directory': os.path.dirname(file_path),
            'is_content_candidate': False,
            'content_category': None
        }
        
        # Categorize by extension
        if file_ext in METADATA_EXTENSIONS:
            filter_results['metadata_files'].append(file_info)
        elif file_ext in TEMP_EXTENSIONS:
            filter_results['temporary_files'].append(file_info)
        elif file_ext in SYSTEM_EXTENSIONS:
            filter_results['system_files'].append(file_info)
        else:
            # Check if it's content
            is_content = False
            content_category = None
            
            for category, extensions in CONTENT_EXTENSIONS.items():
                if file_ext in extensions:
                    is_content = True
                    content_category = category
                    break
            
            if is_content:
                file_info['is_content_candidate'] = True
                file_info['content_category'] = content_category
                filter_results['content_files'].append(file_info)
                content_categories[content_category].append(file_info)
            else:
                filter_results['unknown_files'].append(file_info)

    # Store content categories
    filter_results['content_categories'] = {
        category: len(files) for category, files in content_categories.items()
    }

    # Additional content filtering (size and quality checks)
    filtered_content = []
    size_filtered = []
    
    for file_info in filter_results['content_files']:
        # Size filtering (exclude very small files that might be placeholders)
        min_size_kb = 1  # 1KB minimum
        if file_info['size_bytes'] < min_size_kb * 1024:
            size_filtered.append(file_info)
            continue
            
        # Directory filtering (exclude organized_by_type directories)
        if 'organized_by_type' in file_info['file_path']:
            continue
            
        # Quality indicators
        quality_score = 0
        
        # Size quality (larger files likely more valuable)
        if file_info['size_bytes'] > 1024 * 1024:  # > 1MB
            quality_score += 2
        elif file_info['size_bytes'] > 100 * 1024:  # > 100KB
            quality_score += 1
            
        # File type quality
        if file_info['content_category'] in ['video', 'audio']:
            quality_score += 2
        elif file_info['content_category'] in ['document', 'subtitle']:
            quality_score += 1
            
        file_info['quality_score'] = quality_score
        filtered_content.append(file_info)

    # Sort by quality score (highest first)
    filtered_content.sort(key=lambda x: x['quality_score'], reverse=True)

    # Generate summary
    filter_results['filter_summary'] = {
        'total_orphaned': len(orphaned_files),
        'content_files': len(filter_results['content_files']),
        'metadata_files': len(filter_results['metadata_files']),
        'temporary_files': len(filter_results['temporary_files']),
        'system_files': len(filter_results['system_files']),
        'unknown_files': len(filter_results['unknown_files']),
        'quality_filtered_content': len(filtered_content),
        'size_filtered_out': len(size_filtered),
        'content_categories': filter_results['content_categories']
    }

    # Save filtering results
    filter_dir = 'filtered_content'
    os.makedirs(filter_dir, exist_ok=True)
    
    # Complete filter results
    filter_file = os.path.join(filter_dir, f'orphaned_file_filter_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
    with open(filter_file, 'w') as f:
        json.dump(filter_results, f, indent=2)

    # Content candidates CSV for classification
    if filtered_content:
        content_df = pd.DataFrame(filtered_content)
        content_csv = os.path.join(filter_dir, f'content_candidates_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
        content_df.to_csv(content_csv, index=False)
    else:
        content_csv = None

    # High-quality content subset (for priority classification)
    high_quality = [f for f in filtered_content if f['quality_score'] >= 3]
    if high_quality:
        hq_df = pd.DataFrame(high_quality)
        hq_csv = os.path.join(filter_dir, f'high_quality_candidates_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
        hq_df.to_csv(hq_csv, index=False)
    else:
        hq_csv = None

    # Print results
    print(f'\\n✅ Orphaned file filtering completed:')
    print(f'   Results saved to: {filter_file}')
    if content_csv:
        print(f'   Content candidates: {content_csv}')
    if hq_csv:
        print(f'   High-quality candidates: {hq_csv}')
    
    print(f'\\n   Filtering summary:')
    print(f'     Total orphaned files: {len(orphaned_files)}')
    print(f'     Content files identified: {len(filter_results["content_files"])}')
    print(f'     Quality-filtered content: {len(filtered_content)}')
    print(f'     High-quality candidates: {len(high_quality)}')
    
    print(f'\\n   Content categories:')
    for category, count in filter_results['content_categories'].items():
        print(f'     {category}: {count} files')
    
    print(f'\\n   Excluded files:')
    print(f'     Metadata files: {len(filter_results["metadata_files"])}')
    print(f'     Temporary files: {len(filter_results["temporary_files"])}')
    print(f'     System files: {len(filter_results["system_files"])}')
    print(f'     Unknown files: {len(filter_results["unknown_files"])}')

    # Verification
    total_categorized = (len(filter_results['content_files']) + 
                        len(filter_results['metadata_files']) + 
                        len(filter_results['temporary_files']) + 
                        len(filter_results['system_files']) + 
                        len(filter_results['unknown_files']))
    
    issues = []
    if len(filtered_content) == 0:
        issues.append('No content files suitable for classification')
    if total_categorized != len(orphaned_files):
        issues.append(f'File count mismatch: {total_categorized} categorized vs {len(orphaned_files)} orphaned')
    if len(filter_results['content_files']) < 50:  # Expecting around 104 based on earlier analysis
        issues.append(f'Lower content file count than expected: {len(filter_results["content_files"])}')

    if issues:
        print(f'\\n⚠️  Verification issues: {issues}')
    else:
        print(f'\\n✅ Verification passed: Content filtering successful')
    
    return filter_file, len(filtered_content), len(high_quality)

if __name__ == "__main__":
    filter_orphaned_content()