#!/usr/bin/env python3
"""Extract Google Drive IDs from CSV google_drive columns"""

import pandas as pd
import re
from typing import List, Dict, Set
from orphaned_file_recovery_tracker import get_tracker

def extract_drive_ids_from_csv():
    """Extract Google Drive file IDs from all clients in CSV"""
    tracker = get_tracker()
    tracker.start_phase('drive_ids')
    
    try:
        # Load CSV
        df = pd.read_csv('outputs/output.csv')
        tracker.log("INFO", "Loaded CSV for Drive ID extraction", {
            'total_rows': len(df),
            'columns': list(df.columns)
        })
        
        # Pattern to match Google Drive file IDs
        # Drive IDs are typically 28-44 characters, alphanumeric with underscores and hyphens
        drive_id_patterns = [
            r'/file/d/([a-zA-Z0-9_-]{28,44})',  # /file/d/ID pattern
            r'id=([a-zA-Z0-9_-]{28,44})',       # id=ID pattern
            r'folders/([a-zA-Z0-9_-]{28,44})',  # folders/ID pattern
            r'([a-zA-Z0-9_-]{28,44})'           # Just the ID itself
        ]
        
        client_drive_mappings = []
        total_drive_links = 0
        total_extracted_ids = 0
        
        for _, row in df.iterrows():
            row_id = row['row_id']
            name = row['name']
            google_drive_data = str(row.get('google_drive', ''))
            
            # Skip if no Drive data
            if pd.isna(google_drive_data) or google_drive_data == 'nan' or not google_drive_data:
                continue
            
            total_drive_links += 1
            
            # Extract Drive IDs using patterns
            extracted_ids = set()
            
            for pattern in drive_id_patterns:
                matches = re.findall(pattern, google_drive_data, re.IGNORECASE)
                for match in matches:
                    # Validate ID length and format
                    if 28 <= len(match) <= 44 and re.match(r'^[a-zA-Z0-9_-]+$', match):
                        extracted_ids.add(match)
            
            if extracted_ids:
                client_mapping = {
                    'row_id': row_id,
                    'name': name,
                    'email': row.get('email', ''),
                    'type': row.get('type', ''),
                    'google_drive_raw': google_drive_data,
                    'extracted_drive_ids': list(extracted_ids),
                    'drive_id_count': len(extracted_ids)
                }
                
                client_drive_mappings.append(client_mapping)
                total_extracted_ids += len(extracted_ids)
                
                tracker.log("DEBUG", f"Extracted Drive IDs for client {row_id}", {
                    'client_name': name,
                    'drive_ids': list(extracted_ids),
                    'raw_data': google_drive_data[:100] + '...' if len(google_drive_data) > 100 else google_drive_data
                })
        
        # Store results
        tracker.session_data['client_drive_mappings'] = client_drive_mappings
        
        # Summary statistics
        tracker.log("SUCCESS", "Drive ID extraction completed", {
            'clients_with_drive_data': total_drive_links,
            'clients_with_extracted_ids': len(client_drive_mappings),
            'total_drive_ids_extracted': total_extracted_ids,
            'extraction_success_rate': f"{(len(client_drive_mappings)/total_drive_links)*100:.1f}%" if total_drive_links > 0 else "0%"
        })
        
        # Display results
        print(f"\n📊 GOOGLE DRIVE ID EXTRACTION RESULTS:")
        print(f"   Total clients: {len(df)}")
        print(f"   Clients with Drive data: {total_drive_links}")
        print(f"   Clients with extracted IDs: {len(client_drive_mappings)}")
        print(f"   Total Drive IDs extracted: {total_extracted_ids}")
        print(f"   Extraction success rate: {(len(client_drive_mappings)/total_drive_links)*100:.1f}%" if total_drive_links > 0 else "0%")
        
        if client_drive_mappings:
            print(f"\n📋 SAMPLE EXTRACTED DRIVE IDS:")
            for i, mapping in enumerate(client_drive_mappings[:5], 1):
                print(f"   {i}. {mapping['name']} (Row {mapping['row_id']})")
                print(f"      Drive IDs: {mapping['drive_id_count']}")
                for drive_id in mapping['extracted_drive_ids'][:3]:
                    print(f"        - {drive_id}")
                if len(mapping['extracted_drive_ids']) > 3:
                    print(f"        ... and {len(mapping['extracted_drive_ids']) - 3} more")
        
        return client_drive_mappings
        
    except Exception as e:
        tracker.add_error('drive_ids', {
            'error_type': 'extraction_error',
            'error_message': str(e),
            'context': {'operation': 'csv_drive_id_extraction'}
        })
        raise

def get_all_unique_drive_ids(client_mappings: List[Dict]) -> Set[str]:
    """Get all unique Drive IDs from client mappings"""
    all_ids = set()
    for mapping in client_mappings:
        all_ids.update(mapping['extracted_drive_ids'])
    return all_ids

if __name__ == "__main__":
    print("🔍 EXTRACTING GOOGLE DRIVE IDS FROM CSV...")
    print("=" * 60)
    
    mappings = extract_drive_ids_from_csv()
    
    if mappings:
        all_drive_ids = get_all_unique_drive_ids(mappings)
        print(f"\n✅ Extraction complete: {len(all_drive_ids)} unique Drive IDs found")
        
        # Show some example IDs
        print(f"\n📋 Example Drive IDs:")
        for i, drive_id in enumerate(list(all_drive_ids)[:5], 1):
            print(f"   {i}. {drive_id}")
    else:
        print(f"\n❌ No Drive IDs extracted")