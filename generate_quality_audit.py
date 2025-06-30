#!/usr/bin/env python3
"""
Generate comprehensive mapping quality audit
"""

import json
import pandas as pd
from datetime import datetime
from utils.clean_file_mapper import CleanFileMapper
import os
from collections import defaultdict

def generate_quality_audit():
    print('=== GENERATING MAPPING QUALITY AUDIT ===')

    # Load current mappings
    clean_mapper = CleanFileMapper()
    clean_mapper.map_all_files()
    df = pd.read_csv('outputs/output.csv')

    # Quality tier definitions
    QUALITY_TIERS = {
        'definitive': {'tier': 'HIGH', 'confidence': 0.95, 'description': 'File-specific metadata match'},
        'csv': {'tier': 'MEDIUM', 'confidence': 0.80, 'description': 'CSV authoritative listing'},
        'original_metadata': {'tier': 'LOW', 'confidence': 0.60, 'description': 'Original metadata inference'}
    }

    # Audit structure
    audit_data = {
        'audit_timestamp': datetime.now().isoformat(),
        'total_mappings_audited': len(clean_mapper.file_to_row),
        'quality_distribution': {},
        'tier_breakdown': {
            'HIGH': {'files': [], 'count': 0, 'percentage': 0},
            'MEDIUM': {'files': [], 'count': 0, 'percentage': 0}, 
            'LOW': {'files': [], 'count': 0, 'percentage': 0}
        },
        'detailed_audit': []
    }

    # Process each mapping
    for file_path, mapping_info in clean_mapper.file_to_row.items():
        source = mapping_info['source']
        row_id = mapping_info['row_id']
        
        # Get tier information
        tier_info = QUALITY_TIERS.get(source, {'tier': 'UNKNOWN', 'confidence': 0.0})
        tier = tier_info['tier']
        
        # Get row information
        row_data = df[df['row_id'] == row_id]
        if not row_data.empty:
            person_name = row_data.iloc[0]['name']
            personality_type = row_data.iloc[0]['type']
        else:
            person_name = 'UNKNOWN'
            personality_type = 'UNKNOWN'
        
        # Create detailed audit record
        audit_record = {
            'file_path': file_path,
            'filename': os.path.basename(file_path),
            'row_id': row_id,
            'person_name': person_name,
            'personality_type': personality_type,
            'mapping_source': source,
            'quality_tier': tier,
            'confidence_score': tier_info['confidence'],
            'source_description': tier_info['description'],
            'file_size_bytes': os.path.getsize(file_path) if os.path.exists(file_path) else 0,
            'file_extension': os.path.splitext(file_path)[1].lower()
        }
        
        audit_data['detailed_audit'].append(audit_record)
        
        # Track by tier
        if tier in audit_data['tier_breakdown']:
            audit_data['tier_breakdown'][tier]['files'].append(file_path)
            audit_data['tier_breakdown'][tier]['count'] += 1

    # Calculate percentages
    total_files = len(clean_mapper.file_to_row)
    for tier in audit_data['tier_breakdown']:
        count = audit_data['tier_breakdown'][tier]['count']
        audit_data['tier_breakdown'][tier]['percentage'] = (count / total_files) * 100

    # Track source distribution
    source_counts = defaultdict(int)
    for record in audit_data['detailed_audit']:
        source_counts[record['mapping_source']] += 1
    audit_data['quality_distribution'] = dict(source_counts)

    # Save audit results
    audit_dir = 'audit'
    os.makedirs(audit_dir, exist_ok=True)
    
    # JSON audit report
    audit_file = os.path.join(audit_dir, f'mapping_quality_audit_{datetime.now().strftime("%Y%m%d_%H%M%S")}.json')
    with open(audit_file, 'w') as f:
        json.dump(audit_data, f, indent=2)

    # CSV audit report for analysis
    audit_df = pd.DataFrame(audit_data['detailed_audit'])
    csv_file = os.path.join(audit_dir, f'mapping_quality_audit_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
    audit_df.to_csv(csv_file, index=False)

    # HIGH quality subset (reference dataset candidates)
    high_quality_df = audit_df[audit_df['quality_tier'] == 'HIGH']
    high_quality_file = os.path.join(audit_dir, f'high_quality_mappings_{datetime.now().strftime("%Y%m%d_%H%M%S")}.csv')
    high_quality_df.to_csv(high_quality_file, index=False)

    # Print audit summary
    print(f'✅ Quality audit completed:')
    print(f'   Total mappings audited: {total_files}')
    
    for tier, info in audit_data['tier_breakdown'].items():
        if info['count'] > 0:
            print(f'   {tier} quality: {info["count"]} files ({info["percentage"]:.1f}%)')
    
    print(f'   Audit files saved:')
    print(f'     - Complete audit: {audit_file}')
    print(f'     - CSV analysis: {csv_file}')
    print(f'     - HIGH quality subset: {high_quality_file}')

    # Verification
    high_count = audit_data['tier_breakdown']['HIGH']['count']
    medium_count = audit_data['tier_breakdown']['MEDIUM']['count']
    low_count = audit_data['tier_breakdown']['LOW']['count']
    
    issues = []
    if high_count != 148:
        issues.append(f'Expected 148 HIGH quality mappings, got {high_count}')
    if low_count != 1:
        issues.append(f'Expected 1 LOW quality mapping, got {low_count}')
    if medium_count != 0:
        issues.append(f'Expected 0 MEDIUM quality mappings, got {medium_count}')

    if issues:
        print(f'⚠️  Verification issues: {issues}')
    else:
        print(f'✅ Verification passed: Quality distribution matches expectations')
    
    return audit_file, high_quality_file, high_count

if __name__ == "__main__":
    generate_quality_audit()