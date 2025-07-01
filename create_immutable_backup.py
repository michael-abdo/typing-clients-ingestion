#!/usr/bin/env python3
"""
Create immutable backup of high-quality mappings
"""

import json
import pandas as pd
from datetime import datetime
from utils.clean_file_mapper import CleanFileMapper
import os

def create_immutable_backup():
    print('=== CREATING IMMUTABLE BACKUP OF HIGH-QUALITY MAPPINGS ===')

    # Load current mappings
    clean_mapper = CleanFileMapper()
    clean_mapper.map_all_files()

    # Create backup structure
    backup_data = {
        'timestamp': datetime.now().isoformat(),
        'total_mappings': len(clean_mapper.file_to_row),
        'mapping_sources': {},
        'gold_standard_mappings': {},
        'metadata': {
            'purpose': 'Immutable backup of high-quality file-to-row mappings',
            'quality_threshold': 'definitive source only',
            'total_files_mapped': len(clean_mapper.file_to_row),
            'backup_type': 'complete_system_state'
        }
    }

    # Categorize by source quality
    for file_path, mapping_info in clean_mapper.file_to_row.items():
        source = mapping_info['source']
        row_id = mapping_info['row_id']
        
        if source not in backup_data['mapping_sources']:
            backup_data['mapping_sources'][source] = []
        
        backup_data['mapping_sources'][source].append({
            'file_path': file_path,
            'row_id': row_id,
            'source': source
        })
        
        # Store all mappings in gold standard for preservation
        backup_data['gold_standard_mappings'][file_path] = mapping_info

    # Create backup directory with timestamp
    backup_dir = f'backup/gold_standard_{datetime.now().strftime("%Y%m%d_%H%M%S")}'
    os.makedirs(backup_dir, exist_ok=True)

    # Save immutable backup
    backup_file = os.path.join(backup_dir, 'immutable_mapping_backup.json')
    with open(backup_file, 'w') as f:
        json.dump(backup_data, f, indent=2)

    # Create CSV version for easy access
    csv_data = []
    for file_path, mapping_info in backup_data['gold_standard_mappings'].items():
        csv_data.append({
            'file_path': file_path,
            'filename': os.path.basename(file_path),
            'row_id': mapping_info['row_id'],
            'source': mapping_info['source'],
            'quality_tier': 'HIGH' if mapping_info['source'] == 'definitive' else 'LOW'
        })

    csv_df = pd.DataFrame(csv_data)
    csv_file = os.path.join(backup_dir, 'gold_standard_mappings.csv')
    csv_df.to_csv(csv_file, index=False)

    # Verification
    total_backed_up = len(backup_data['gold_standard_mappings'])
    definitive_count = len(backup_data['mapping_sources'].get('definitive', []))
    other_count = total_backed_up - definitive_count

    print(f'✅ Backup created successfully:')
    print(f'   Location: {backup_dir}')
    print(f'   Total mappings backed up: {total_backed_up}')
    print(f'   HIGH quality (definitive): {definitive_count}')
    print(f'   Other quality: {other_count}')
    print(f'   Files: {backup_file}, {csv_file}')

    # Issue logging
    issues = []
    if total_backed_up != 149:
        issues.append(f'Expected 149 mappings, got {total_backed_up}')
    if definitive_count != 148:
        issues.append(f'Expected 148 definitive mappings, got {definitive_count}')

    if issues:
        print(f'⚠️  Issues detected: {issues}')
    else:
        print(f'✅ Verification passed: All expected mappings backed up')
    
    return backup_dir, total_backed_up, definitive_count

if __name__ == "__main__":
    create_immutable_backup()