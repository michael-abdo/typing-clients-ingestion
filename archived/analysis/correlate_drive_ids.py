#!/usr/bin/env python3
"""Create Drive ID correlation matrix matching JSON to CSV clients"""

import json
from typing import List, Dict, Set
from orphaned_file_recovery_tracker import get_tracker

def correlate_drive_ids():
    """Correlate Drive IDs between CSV clients and JSON files"""
    tracker = get_tracker()
    
    try:
        # Load previous results from tracker session data
        client_drive_mappings = tracker.session_data.get('client_drive_mappings', [])
        json_drive_mappings = tracker.session_data.get('json_drive_mappings', [])
        
        if not client_drive_mappings:
            tracker.log("WARNING", "No client Drive ID mappings found - running extraction")
            # Re-run CSV extraction if needed
            from extract_drive_ids_csv import extract_drive_ids_from_csv
            client_drive_mappings = extract_drive_ids_from_csv()
            
        if not json_drive_mappings:
            tracker.log("WARNING", "No JSON Drive ID mappings found - running extraction")
            # Re-run JSON extraction if needed  
            from extract_drive_ids_json import extract_drive_ids_from_json
            json_drive_mappings = extract_drive_ids_from_json()
        
        tracker.log("INFO", "Starting Drive ID correlation", {
            'client_mappings': len(client_drive_mappings),
            'json_mappings': len(json_drive_mappings)
        })
        
        # Create Drive ID lookup from CSV clients
        csv_drive_to_client = {}
        csv_drive_ids = set()
        
        for client in client_drive_mappings:
            for drive_id in client['extracted_drive_ids']:
                csv_drive_ids.add(drive_id)
                csv_drive_to_client[drive_id] = {
                    'row_id': client['row_id'],
                    'name': client['name'],
                    'email': client['email'],
                    'type': client['type']
                }
        
        # Create Drive ID lookup from JSON files
        json_drive_to_uuid = {}
        json_drive_ids = set()
        
        for json_mapping in json_drive_mappings:
            for drive_id in json_mapping['drive_ids']:
                json_drive_ids.add(drive_id)
                if drive_id not in json_drive_to_uuid:
                    json_drive_to_uuid[drive_id] = []
                json_drive_to_uuid[drive_id].append({
                    'uuid': json_mapping['uuid'],
                    's3_key': json_mapping['s3_key'],
                    'file_size': json_mapping['file_size'],
                    'last_modified': json_mapping['last_modified']
                })
        
        # Find correlations (Drive IDs that appear in both CSV and JSON)
        matching_drive_ids = csv_drive_ids.intersection(json_drive_ids)
        
        drive_correlations = []
        
        for drive_id in matching_drive_ids:
            client_info = csv_drive_to_client[drive_id]
            json_files = json_drive_to_uuid[drive_id]
            
            correlation = {
                'drive_id': drive_id,
                'client_info': client_info,
                'json_files': json_files,
                'match_confidence': 'high',  # Exact Drive ID match
                'correlation_type': 'exact_drive_id_match'
            }
            
            drive_correlations.append(correlation)
            
            tracker.log("SUCCESS", f"Drive ID correlation found", {
                'drive_id': drive_id,
                'client': f"{client_info['name']} (Row {client_info['row_id']})",
                'json_files': len(json_files)
            })
        
        # Store results
        tracker.session_data['drive_correlations'] = drive_correlations
        
        # Summary
        tracker.log("INFO", "Drive ID correlation completed", {
            'csv_drive_ids': len(csv_drive_ids),
            'json_drive_ids': len(json_drive_ids),
            'matching_drive_ids': len(matching_drive_ids),
            'correlations_found': len(drive_correlations)
        })
        
        # Display results
        print(f"\n📊 DRIVE ID CORRELATION RESULTS:")
        print(f"   CSV Drive IDs: {len(csv_drive_ids)}")
        print(f"   JSON Drive IDs: {len(json_drive_ids)}")
        print(f"   Matching Drive IDs: {len(matching_drive_ids)}")
        print(f"   Correlations found: {len(drive_correlations)}")
        
        if matching_drive_ids:
            print(f"\n✅ SUCCESSFUL CORRELATIONS:")
            for correlation in drive_correlations:
                client = correlation['client_info']
                drive_id = correlation['drive_id']
                json_files = correlation['json_files']
                
                print(f"\n   🎯 Match Found!")
                print(f"      Drive ID: {drive_id}")
                print(f"      Client: {client['name']} (Row {client['row_id']})")
                print(f"      Type: {client['type']}")
                print(f"      JSON files: {len(json_files)}")
                
                for json_file in json_files:
                    size_mb = json_file['file_size'] / (1024 * 1024)
                    print(f"        - UUID: {json_file['uuid']}")
                    print(f"          S3 Key: {json_file['s3_key']}")
                    print(f"          Size: {size_mb:.2f} MB")
        else:
            print(f"\n❌ NO DRIVE ID MATCHES FOUND")
            print(f"\n📋 CSV Drive IDs:")
            for drive_id in sorted(csv_drive_ids):
                client = csv_drive_to_client[drive_id]
                print(f"      {drive_id} - {client['name']} (Row {client['row_id']})")
            
            print(f"\n📋 JSON Drive IDs (first 10):")
            for drive_id in sorted(list(json_drive_ids))[:10]:
                json_files = json_drive_to_uuid[drive_id]
                print(f"      {drive_id} - {len(json_files)} JSON file(s)")
        
        return drive_correlations
        
    except Exception as e:
        tracker.add_error('drive_ids', {
            'error_type': 'correlation_error',
            'error_message': str(e),
            'context': {'operation': 'drive_id_correlation'}
        })
        raise

if __name__ == "__main__":
    print("🔗 CORRELATING DRIVE IDS BETWEEN CSV AND JSON...")
    print("=" * 60)
    
    correlations = correlate_drive_ids()
    
    if correlations:
        print(f"\n🎉 Success! Found {len(correlations)} Drive ID correlations")
        print("These orphaned files can be mapped to their owners!")
    else:
        print(f"\n😞 No Drive ID correlations found")
        print("Will need to use alternative mapping methods")