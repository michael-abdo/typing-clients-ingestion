#!/usr/bin/env python3
"""Investigate all JSON files for structured row_id data like Dan Jane's file"""

import boto3
import pandas as pd
import json
import re
from orphaned_file_recovery_tracker import get_tracker

def investigate_all_json_for_row_ids():
    """Systematically check all remaining JSON files for structured row_id data"""
    print("🎯 INVESTIGATING ALL JSON FILES FOR ROW_ID MAPPINGS")
    print("=" * 60)
    
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    
    # Get all JSON files in the orphaned directory
    response = s3.list_objects_v2(Bucket=bucket, Prefix="files/")
    json_files = []
    
    if 'Contents' in response:
        for obj in response['Contents']:
            if obj['Key'].endswith('.json'):
                uuid = obj['Key'].split('/')[-1].replace('.json', '')
                json_files.append({
                    'uuid': uuid,
                    's3_key': obj['Key'],
                    'size_mb': obj['Size'] / (1024 * 1024)
                })
    
    print(f"📄 Found {len(json_files)} JSON files to investigate")
    
    # Load client data for validation
    df = pd.read_csv('outputs/output.csv')
    max_row_id = df['row_id'].max()
    print(f"📊 CSV contains rows 1-{max_row_id}")
    
    structured_mappings = []
    
    for json_file in json_files:
        uuid = json_file['uuid']
        
        try:
            print(f"\n🔍 Analyzing {uuid}.json ({json_file['size_mb']:.3f} MB)...")
            
            response = s3.get_object(Bucket=bucket, Key=json_file['s3_key'])
            content = response['Body'].read().decode('utf-8')
            
            if not content.strip():
                print(f"   📭 Empty file")
                continue
            
            try:
                data = json.loads(content)
            except json.JSONDecodeError:
                print(f"   ❌ Invalid JSON format")
                continue
            
            # Search for row_id in the data structure
            row_ids_found = []
            
            def find_row_ids(obj, path=""):
                nonlocal row_ids_found
                
                if isinstance(obj, dict):
                    for key, value in obj.items():
                        current_path = f"{path}.{key}" if path else key
                        
                        # Look for row_id specifically
                        if key.lower() == 'row_id' and isinstance(value, (int, str)):
                            try:
                                row_id_val = int(value)
                                row_ids_found.append({
                                    'row_id': row_id_val,
                                    'path': current_path,
                                    'context': obj  # Store surrounding context
                                })
                            except (ValueError, TypeError):
                                pass
                        
                        # Also look for other row-related fields
                        if any(term in key.lower() for term in ['row', 'id', 'index']) and isinstance(value, (int, str)):
                            try:
                                potential_id = int(value)
                                if 1 <= potential_id <= max_row_id:  # Valid row range
                                    row_ids_found.append({
                                        'row_id': potential_id,
                                        'path': current_path,
                                        'context': obj,
                                        'field_name': key
                                    })
                            except (ValueError, TypeError):
                                pass
                        
                        if isinstance(value, (dict, list)):
                            find_row_ids(value, current_path)
                
                elif isinstance(obj, list):
                    for i, item in enumerate(obj):
                        find_row_ids(item, f"{path}[{i}]")
            
            find_row_ids(data)
            
            # Remove duplicates based on row_id value
            unique_row_ids = {}
            for found in row_ids_found:
                row_id = found['row_id']
                if row_id not in unique_row_ids:
                    unique_row_ids[row_id] = found
            
            if unique_row_ids:
                print(f"   ✅ Found {len(unique_row_ids)} potential row_id mappings:")
                
                for row_id, found_data in unique_row_ids.items():
                    # Validate against CSV
                    client_row = df[df['row_id'] == row_id]
                    
                    if not client_row.empty:
                        client_name = client_row.iloc[0]['name']
                        print(f"      📍 row_id {row_id} → {client_name}")
                        print(f"         Path: {found_data['path']}")
                        
                        # Extract additional context for verification
                        context = found_data.get('context', {})
                        person_name = None
                        
                        # Look for person/name fields in the same context
                        for key, value in context.items():
                            if key.lower() in ['person', 'name', 'user', 'client', 'owner'] and isinstance(value, str):
                                person_name = value
                                print(f"         Person: {person_name}")
                                break
                        
                        # Calculate confidence score
                        confidence = 0.7  # Base confidence for row_id match
                        
                        if person_name:
                            # Check if the person name matches the client name
                            person_lower = person_name.lower()
                            client_lower = client_name.lower()
                            
                            # Simple name matching
                            client_parts = client_lower.split()
                            matches = sum(1 for part in client_parts if len(part) > 2 and part in person_lower)
                            
                            if matches >= 2:  # Full name match
                                confidence = 0.95
                                print(f"         🎯 HIGH CONFIDENCE: Person name matches client name!")
                            elif matches == 1:  # Partial match
                                confidence = 0.85
                                print(f"         ⚠️  MEDIUM CONFIDENCE: Partial name match")
                            else:
                                confidence = 0.6
                                print(f"         ⚠️  LOW CONFIDENCE: Person name doesn't match client")
                        
                        structured_mappings.append({
                            'uuid': uuid,
                            'row_id': row_id,
                            'client_name': client_name,
                            'person_in_json': person_name,
                            'confidence': confidence,
                            'method': 'structured_row_id',
                            'evidence': f"row_id {row_id} found at {found_data['path']}",
                            'full_context': context
                        })
                    else:
                        print(f"      ❌ row_id {row_id} not found in CSV (out of range)")
            else:
                print(f"   📊 No row_id mappings found")
        
        except Exception as e:
            print(f"   ❌ Error analyzing {uuid}: {str(e)}")
            continue
    
    # Summary of findings
    print(f"\n📊 STRUCTURED ROW_ID INVESTIGATION RESULTS")
    print("=" * 50)
    print(f"   JSON files analyzed: {len(json_files)}")
    print(f"   Files with row_id mappings: {len(structured_mappings)}")
    
    if structured_mappings:
        print(f"\n🎯 CONFIRMED MAPPINGS READY FOR EXECUTION:")
        
        high_confidence = [m for m in structured_mappings if m['confidence'] >= 0.8]
        medium_confidence = [m for m in structured_mappings if 0.6 <= m['confidence'] < 0.8]
        
        print(f"   High confidence (≥80%): {len(high_confidence)}")
        print(f"   Medium confidence (60-80%): {len(medium_confidence)}")
        
        for mapping in sorted(structured_mappings, key=lambda x: x['confidence'], reverse=True):
            print(f"\n   📦 {mapping['uuid']} → {mapping['client_name']} (Row {mapping['row_id']})")
            print(f"      Confidence: {mapping['confidence']:.1%}")
            print(f"      Evidence: {mapping['evidence']}")
            if mapping['person_in_json']:
                print(f"      Person in JSON: {mapping['person_in_json']}")
    
    return structured_mappings

def verify_mappings_with_companion_files(mappings):
    """Check if the mapped JSON files have companion media files"""
    print(f"\n🔍 CHECKING FOR COMPANION FILES")
    print("=" * 40)
    
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    
    # Get all orphaned files to check for companions
    response = s3.list_objects_v2(Bucket=bucket, Prefix="files/")
    all_files = {}
    
    if 'Contents' in response:
        for obj in response['Contents']:
            if not obj['Key'].endswith('/'):
                filename = obj['Key'].split('/')[-1]
                uuid = filename.split('.')[0]
                extension = filename.split('.')[-1] if '.' in filename else 'no_ext'
                
                if uuid not in all_files:
                    all_files[uuid] = []
                
                all_files[uuid].append({
                    'filename': filename,
                    'extension': extension,
                    's3_key': obj['Key'],
                    'size_mb': obj['Size'] / (1024 * 1024)
                })
    
    # Check each mapping for companion files
    enhanced_mappings = []
    
    for mapping in mappings:
        uuid = mapping['uuid']
        
        if uuid in all_files:
            files = all_files[uuid]
            
            # Separate JSON and media files
            json_files = [f for f in files if f['extension'] == 'json']
            media_files = [f for f in files if f['extension'] in ['mp4', 'mp3', 'webm', 'm4a', 'avi', 'mov']]
            other_files = [f for f in files if f['extension'] not in ['json', 'mp4', 'mp3', 'webm', 'm4a', 'avi', 'mov']]
            
            total_files = len(files)
            total_size_mb = sum(f['size_mb'] for f in files)
            
            print(f"   📦 {uuid} → {mapping['client_name']}")
            print(f"      Total files: {total_files} ({total_size_mb:.1f} MB)")
            print(f"      JSON: {len(json_files)}, Media: {len(media_files)}, Other: {len(other_files)}")
            
            if media_files:
                print(f"      Media files:")
                for media in media_files:
                    print(f"        - {media['filename']} ({media['size_mb']:.1f} MB)")
            
            enhanced_mapping = mapping.copy()
            enhanced_mapping.update({
                'total_files': total_files,
                'total_size_mb': total_size_mb,
                'media_files': media_files,
                'json_files': json_files,
                'other_files': other_files,
                'all_files': files
            })
            
            enhanced_mappings.append(enhanced_mapping)
    
    return enhanced_mappings

def main():
    """Main investigation function"""
    tracker = get_tracker()
    
    print("🚀 STRUCTURED ROW_ID MAPPING INVESTIGATION")
    print("=" * 60)
    
    # Step 1: Investigate all JSON files for row_id data
    structured_mappings = investigate_all_json_for_row_ids()
    
    if not structured_mappings:
        print("\n📊 No structured row_id mappings found")
        return []
    
    # Step 2: Verify mappings and find companion files
    enhanced_mappings = verify_mappings_with_companion_files(structured_mappings)
    
    # Step 3: Save findings for execution
    tracker.session_data['structured_row_id_mappings'] = {
        'total_mappings': len(enhanced_mappings),
        'high_confidence': len([m for m in enhanced_mappings if m['confidence'] >= 0.8]),
        'total_files': sum(m.get('total_files', 1) for m in enhanced_mappings),
        'total_size_mb': sum(m.get('total_size_mb', 0) for m in enhanced_mappings),
        'mappings': enhanced_mappings
    }
    
    print(f"\n🎊 INVESTIGATION COMPLETE")
    print(f"   Structured mappings found: {len(enhanced_mappings)}")
    print(f"   Total files ready for recovery: {sum(m.get('total_files', 1) for m in enhanced_mappings)}")
    print(f"   Total size: {sum(m.get('total_size_mb', 0) for m in enhanced_mappings):.1f} MB")
    
    return enhanced_mappings

if __name__ == "__main__":
    mappings = main()
    
    if mappings:
        high_conf = [m for m in mappings if m['confidence'] >= 0.8]
        print(f"\n✅ SUCCESS: Found {len(high_conf)} high-confidence structured mappings ready for execution!")
    else:
        print(f"\n📊 No structured row_id mappings discovered")