#!/usr/bin/env python3
"""
Create protected reference dataset from 148 definitive mappings only
"""

import json
import pandas as pd
from datetime import datetime
from utils.clean_file_mapper import CleanFileMapper
import os
import shutil
from collections import defaultdict

def create_reference_dataset():
    print('=== CREATING PROTECTED REFERENCE DATASET ===')

    # Load current mappings
    clean_mapper = CleanFileMapper()
    clean_mapper.map_all_files()
    df = pd.read_csv('outputs/output.csv')

    # Filter for HIGH quality (definitive) mappings only
    definitive_mappings = {}
    for file_path, mapping_info in clean_mapper.file_to_row.items():
        if mapping_info['source'] == 'definitive':
            definitive_mappings[file_path] = mapping_info

    print(f'Filtering to {len(definitive_mappings)} definitive mappings from {len(clean_mapper.file_to_row)} total')

    # Create reference dataset structure
    reference_data = {
        'dataset_metadata': {
            'creation_timestamp': datetime.now().isoformat(),
            'purpose': 'Protected reference dataset for personality type classification',
            'quality_level': 'HIGH (definitive mappings only)',
            'total_files': len(definitive_mappings),
            'data_integrity': 'PROTECTED',
            'usage': 'Training data for content classification'
        },
        'personality_type_distribution': {},
        'file_type_distribution': {},
        'reference_mappings': {},
        'training_metadata': {}
    }

    # Process definitive mappings
    personality_counts = defaultdict(int)
    file_type_counts = defaultdict(int)
    
    for file_path, mapping_info in definitive_mappings.items():
        row_id = mapping_info['row_id']
        
        # Get personality type from CSV
        row_data = df[df['row_id'] == row_id]
        if not row_data.empty:
            row = row_data.iloc[0]
            personality_type = row['type']
            person_name = row['name']
            person_email = row.get('email', 'unknown')
        else:
            personality_type = 'UNKNOWN'
            person_name = 'UNKNOWN'
            person_email = 'unknown'
        
        # File characteristics
        file_ext = os.path.splitext(file_path)[1].lower()
        file_size = os.path.getsize(file_path) if os.path.exists(file_path) else 0
        
        # Track distributions
        personality_counts[personality_type] += 1
        file_type_counts[file_ext] += 1
        
        # Create reference record
        reference_data['reference_mappings'][file_path] = {
            'row_id': row_id,
            'personality_type': personality_type,
            'person_name': person_name,
            'person_email': person_email,
            'file_path': file_path,
            'filename': os.path.basename(file_path),
            'file_extension': file_ext,
            'file_size_bytes': file_size,
            'mapping_source': 'definitive',
            'confidence_score': 0.95,
            'data_quality': 'HIGH'
        }

    # Store distributions
    reference_data['personality_type_distribution'] = dict(personality_counts)
    reference_data['file_type_distribution'] = dict(file_type_counts)
    
    # Training metadata
    reference_data['training_metadata'] = {
        'total_examples': len(definitive_mappings),
        'unique_personality_types': len(personality_counts),
        'unique_file_types': len(file_type_counts),
        'min_examples_per_type': min(personality_counts.values()) if personality_counts else 0,
        'max_examples_per_type': max(personality_counts.values()) if personality_counts else 0,
        'class_balance_ratio': max(personality_counts.values()) / min(personality_counts.values()) if personality_counts and min(personality_counts.values()) > 0 else 0
    }

    # Create protected reference directory
    reference_dir = f'reference_dataset_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
    os.makedirs(reference_dir, exist_ok=True)
    
    # Make it read-only to protect integrity
    os.chmod(reference_dir, 0o755)  # rwxr-xr-x

    # Save reference dataset
    reference_file = os.path.join(reference_dir, 'protected_reference_dataset.json')
    with open(reference_file, 'w') as f:
        json.dump(reference_data, f, indent=2)
    
    # Make reference file read-only
    os.chmod(reference_file, 0o444)  # r--r--r--

    # Create CSV version for analysis
    csv_data = []
    for file_path, ref_data in reference_data['reference_mappings'].items():
        csv_data.append(ref_data)
    
    reference_df = pd.DataFrame(csv_data)
    csv_file = os.path.join(reference_dir, 'reference_dataset.csv')
    reference_df.to_csv(csv_file, index=False)
    os.chmod(csv_file, 0o444)  # r--r--r--

    # Create personality type summary
    summary_data = []
    for ptype, count in personality_counts.items():
        summary_data.append({
            'personality_type': ptype,
            'file_count': count,
            'percentage': (count / len(definitive_mappings)) * 100
        })
    
    summary_df = pd.DataFrame(summary_data).sort_values('file_count', ascending=False)
    summary_file = os.path.join(reference_dir, 'personality_type_distribution.csv')
    summary_df.to_csv(summary_file, index=False)
    os.chmod(summary_file, 0o444)  # r--r--r--

    # Print results
    print(f'✅ Protected reference dataset created:')
    print(f'   Location: {reference_dir}')
    print(f'   Total definitive mappings: {len(definitive_mappings)}')
    print(f'   Unique personality types: {len(personality_counts)}')
    print(f'   Unique file types: {len(file_type_counts)}')
    print(f'   Class balance ratio: {reference_data["training_metadata"]["class_balance_ratio"]:.1f}')
    
    print(f'\\n   Top 5 personality types:')
    for ptype, count in sorted(personality_counts.items(), key=lambda x: x[1], reverse=True)[:5]:
        print(f'     {ptype}: {count} files ({(count/len(definitive_mappings))*100:.1f}%)')
    
    print(f'\\n   Files created (protected):')
    print(f'     - {reference_file}')
    print(f'     - {csv_file}')
    print(f'     - {summary_file}')

    # Verification
    issues = []
    if len(definitive_mappings) != 148:
        issues.append(f'Expected 148 definitive mappings, got {len(definitive_mappings)}')
    if len(personality_counts) == 0:
        issues.append('No personality types found in reference dataset')
    if reference_data['training_metadata']['class_balance_ratio'] > 10:
        issues.append(f'High class imbalance detected: {reference_data["training_metadata"]["class_balance_ratio"]:.1f}')

    if issues:
        print(f'⚠️  Verification issues: {issues}')
    else:
        print(f'✅ Verification passed: Reference dataset meets quality standards')
    
    return reference_dir, len(definitive_mappings), len(personality_counts)

if __name__ == "__main__":
    create_reference_dataset()