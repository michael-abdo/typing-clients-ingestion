#!/usr/bin/env python3
"""Extract Google Drive IDs from JSON metadata files in S3"""

import boto3
import json
import re
from typing import List, Dict, Set
from orphaned_file_recovery_tracker import get_tracker

def extract_drive_ids_from_json():
    """Extract Google Drive IDs from orphaned JSON metadata files"""
    tracker = get_tracker()
    
    try:
        # Initialize S3 client
        s3 = boto3.client('s3')
        bucket = 'typing-clients-uuid-system'
        
        tracker.log("INFO", "Starting Drive ID extraction from JSON files", {
            'bucket': bucket
        })
        
        # Get all JSON files from orphaned files directory
        response = s3.list_objects_v2(Bucket=bucket, Prefix="files/")
        json_files = []
        
        if 'Contents' in response:
            for obj in response['Contents']:
                if obj['Key'].endswith('.json'):
                    json_files.append({
                        'key': obj['Key'],
                        'uuid': obj['Key'].split('/')[-1].replace('.json', ''),
                        'size': obj['Size'],
                        'last_modified': obj['LastModified']
                    })
        
        tracker.log("INFO", f"Found JSON files to process", {
            'json_file_count': len(json_files)
        })
        
        json_drive_mappings = []
        total_processed = 0
        total_with_drive_ids = 0
        
        # Drive ID pattern
        drive_id_pattern = r'[a-zA-Z0-9_-]{28,44}'
        
        for json_file in json_files:
            total_processed += 1
            
            try:
                # Download and parse JSON file
                response = s3.get_object(Bucket=bucket, Key=json_file['key'])
                content = response['Body'].read().decode('utf-8')
                
                if not content.strip():
                    tracker.log("DEBUG", f"Empty JSON file: {json_file['key']}")
                    continue
                
                data = json.loads(content)
                
                # Look for Drive ID in various fields
                drive_ids = set()
                
                # Common fields that might contain Drive IDs
                drive_fields = ['id', 'file_id', 'drive_id', 'webpage_url', 'url', 'original_url']
                
                def extract_ids_from_value(value):
                    """Extract Drive IDs from a value"""
                    if isinstance(value, str):
                        # Look for Drive file IDs in URLs or direct values
                        if 'drive.google.com' in value or 'docs.google.com' in value:
                            matches = re.findall(r'/file/d/([a-zA-Z0-9_-]{28,44})', value)
                            matches.extend(re.findall(r'id=([a-zA-Z0-9_-]{28,44})', value))
                            return matches
                        elif re.match(drive_id_pattern, value) and 28 <= len(value) <= 44:
                            return [value]
                    return []
                
                # Search through all data recursively
                def search_data(obj, path=""):
                    found_ids = []
                    if isinstance(obj, dict):
                        for key, value in obj.items():
                            current_path = f"{path}.{key}" if path else key
                            
                            # Check this field
                            if key.lower() in ['id', 'file_id', 'drive_id'] or 'drive' in key.lower():
                                found_ids.extend(extract_ids_from_value(value))
                            
                            # Recurse into nested objects
                            found_ids.extend(search_data(value, current_path))
                    
                    elif isinstance(obj, list):
                        for i, item in enumerate(obj):
                            found_ids.extend(search_data(item, f"{path}[{i}]"))
                    
                    elif isinstance(obj, str):
                        found_ids.extend(extract_ids_from_value(obj))
                    
                    return found_ids
                
                found_drive_ids = search_data(data)
                
                if found_drive_ids:
                    drive_ids.update(found_drive_ids)
                    total_with_drive_ids += 1
                    
                    json_mapping = {
                        'uuid': json_file['uuid'],
                        's3_key': json_file['key'],
                        'drive_ids': list(drive_ids),
                        'drive_id_count': len(drive_ids),
                        'file_size': json_file['size'],
                        'last_modified': json_file['last_modified'].isoformat(),
                        'json_keys': list(data.keys()) if isinstance(data, dict) else []
                    }
                    
                    json_drive_mappings.append(json_mapping)
                    
                    tracker.log("DEBUG", f"Extracted Drive IDs from JSON", {
                        'uuid': json_file['uuid'],
                        'drive_ids': list(drive_ids),
                        's3_key': json_file['key']
                    })
                    
                    print(f"✅ {json_file['uuid']}.json")
                    print(f"   Drive IDs: {list(drive_ids)}")
                
            except json.JSONDecodeError as e:
                tracker.log("WARNING", f"Invalid JSON in {json_file['key']}", {'error': str(e)})
                print(f"⚠️  {json_file['uuid']}.json - Invalid JSON")
                
            except Exception as e:
                tracker.add_error('drive_ids', {
                    'error_type': 'json_processing_error',
                    'error_message': str(e),
                    'context': {'json_file': json_file['key']}
                })
                print(f"❌ {json_file['uuid']}.json - Error: {str(e)}")
        
        # Store results
        tracker.session_data['json_drive_mappings'] = json_drive_mappings
        
        # Summary
        tracker.log("SUCCESS", "JSON Drive ID extraction completed", {
            'total_json_files': total_processed,
            'files_with_drive_ids': total_with_drive_ids,
            'total_drive_ids_found': sum(len(m['drive_ids']) for m in json_drive_mappings),
            'success_rate': f"{(total_with_drive_ids/total_processed)*100:.1f}%" if total_processed > 0 else "0%"
        })
        
        print(f"\n📊 JSON DRIVE ID EXTRACTION RESULTS:")
        print(f"   Total JSON files: {total_processed}")
        print(f"   Files with Drive IDs: {total_with_drive_ids}")
        print(f"   Total Drive IDs found: {sum(len(m['drive_ids']) for m in json_drive_mappings)}")
        print(f"   Success rate: {(total_with_drive_ids/total_processed)*100:.1f}%" if total_processed > 0 else "0%")
        
        return json_drive_mappings
        
    except Exception as e:
        tracker.add_error('drive_ids', {
            'error_type': 'json_extraction_error',
            'error_message': str(e),
            'context': {'operation': 'json_drive_id_extraction'}
        })
        raise

def get_all_json_drive_ids(json_mappings: List[Dict]) -> Set[str]:
    """Get all unique Drive IDs from JSON mappings"""
    all_ids = set()
    for mapping in json_mappings:
        all_ids.update(mapping['drive_ids'])
    return all_ids

if __name__ == "__main__":
    print("🔍 EXTRACTING GOOGLE DRIVE IDS FROM JSON FILES...")
    print("=" * 60)
    
    mappings = extract_drive_ids_from_json()
    
    if mappings:
        all_drive_ids = get_all_json_drive_ids(mappings)
        print(f"\n✅ Extraction complete: {len(all_drive_ids)} unique Drive IDs found in JSON files")
        
        # Show some example IDs
        if all_drive_ids:
            print(f"\n📋 Example Drive IDs from JSON:")
            for i, drive_id in enumerate(list(all_drive_ids)[:5], 1):
                print(f"   {i}. {drive_id}")
    else:
        print(f"\n❌ No Drive IDs found in JSON files")