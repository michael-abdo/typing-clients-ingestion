#!/usr/bin/env python3
"""Advanced orphaned file recovery using multiple deep analysis methods"""

import boto3
import json
import pandas as pd
import re
import os
import glob
from datetime import datetime
from typing import List, Dict, Set, Tuple, Optional
from orphaned_file_recovery_tracker import get_tracker

def analyze_file_content_for_clues():
    """Deep analysis of file content for mapping clues"""
    tracker = get_tracker()
    
    try:
        s3 = boto3.client('s3')
        bucket = 'typing-clients-uuid-system'
        
        tracker.log("INFO", "Starting deep file content analysis", {
            'method': 'content_analysis'
        })
        
        # Get all orphaned files
        response = s3.list_objects_v2(Bucket=bucket, Prefix="files/")
        orphaned_files = []
        
        if 'Contents' in response:
            for obj in response['Contents']:
                if not obj['Key'].endswith('/'):
                    orphaned_files.append({
                        'key': obj['Key'],
                        'uuid': obj['Key'].split('/')[-1].split('.')[0],
                        'extension': obj['Key'].split('.')[-1] if '.' in obj['Key'] else 'no_ext',
                        'size': obj['Size'],
                        'last_modified': obj['LastModified']
                    })
        
        print(f"🔍 Analyzing {len(orphaned_files)} orphaned files for content clues...")
        
        content_clues = []
        
        for file_info in orphaned_files:
            try:
                # Analyze JSON files for metadata
                if file_info['extension'] == 'json':
                    response = s3.get_object(Bucket=bucket, Key=file_info['key'])
                    content = response['Body'].read().decode('utf-8')
                    
                    if content.strip():
                        data = json.loads(content)
                        
                        # Look for identifying information
                        clues = {
                            'uuid': file_info['uuid'],
                            'file_type': 'json',
                            'size': file_info['size'],
                            'clues': {}
                        }
                        
                        # Extract potential identifying fields
                        identifying_fields = [
                            'title', 'name', 'filename', 'original_filename',
                            'uploader', 'creator', 'owner', 'channel',
                            'webpage_url', 'url', 'id', 'video_id',
                            'description', 'playlist_title'
                        ]
                        
                        def extract_clues(obj, path=""):
                            found_clues = {}
                            if isinstance(obj, dict):
                                for key, value in obj.items():
                                    current_path = f"{path}.{key}" if path else key
                                    
                                    if key.lower() in identifying_fields:
                                        if isinstance(value, str) and value.strip():
                                            found_clues[key] = value
                                    
                                    if isinstance(value, (dict, list)):
                                        nested_clues = extract_clues(value, current_path)
                                        found_clues.update(nested_clues)
                            
                            elif isinstance(obj, list):
                                for i, item in enumerate(obj):
                                    nested_clues = extract_clues(item, f"{path}[{i}]")
                                    found_clues.update(nested_clues)
                            
                            return found_clues
                        
                        clues['clues'] = extract_clues(data)
                        
                        if clues['clues']:
                            content_clues.append(clues)
                            print(f"✅ {file_info['uuid']}.json - Found clues: {list(clues['clues'].keys())}")
                
                # For media files, analyze first few KB for metadata
                elif file_info['extension'] in ['mp3', 'mp4', 'm4a', 'webm']:
                    # Get first 8KB to look for metadata
                    response = s3.get_object(
                        Bucket=bucket, 
                        Key=file_info['key'],
                        Range='bytes=0-8191'
                    )
                    content = response['Body'].read()
                    
                    # Look for readable strings that might be names/titles
                    text_content = content.decode('utf-8', errors='ignore')
                    
                    # Extract potential metadata strings
                    potential_names = re.findall(r'[A-Za-z]{2,}(?:\s+[A-Za-z]{2,})+', text_content)
                    potential_names = [name.strip() for name in potential_names if 3 <= len(name) <= 50]
                    
                    if potential_names:
                        clues = {
                            'uuid': file_info['uuid'],
                            'file_type': file_info['extension'],
                            'size': file_info['size'],
                            'clues': {'potential_names': potential_names[:10]}  # Limit to first 10
                        }
                        content_clues.append(clues)
                        print(f"✅ {file_info['uuid']}.{file_info['extension']} - Found metadata strings")
                
            except Exception as e:
                print(f"⚠️  Error analyzing {file_info['uuid']}: {str(e)}")
                continue
        
        tracker.session_data['content_clues'] = content_clues
        
        print(f"\n📊 Content Analysis Results:")
        print(f"   Files with clues: {len(content_clues)}")
        print(f"   JSON files analyzed: {len([c for c in content_clues if c['file_type'] == 'json'])}")
        print(f"   Media files analyzed: {len([c for c in content_clues if c['file_type'] != 'json'])}")
        
        return content_clues
        
    except Exception as e:
        tracker.add_error('content_analysis', {
            'error_type': 'content_analysis_error',
            'error_message': str(e)
        })
        raise

def analyze_filename_and_timestamp_patterns():
    """Analyze UUID patterns and timestamps for grouping clues"""
    tracker = get_tracker()
    
    try:
        s3 = boto3.client('s3')
        bucket = 'typing-clients-uuid-system'
        
        # Get all files (orphaned + client files)
        all_files = {}
        
        # Get orphaned files
        response = s3.list_objects_v2(Bucket=bucket, Prefix="files/")
        if 'Contents' in response:
            for obj in response['Contents']:
                if not obj['Key'].endswith('/'):
                    all_files[obj['Key']] = {
                        'location': 'orphaned',
                        'size': obj['Size'],
                        'last_modified': obj['LastModified'],
                        'uuid': obj['Key'].split('/')[-1].split('.')[0]
                    }
        
        # Get client files
        for client_id in ['502', '503', '504', '505', '506']:
            try:
                response = s3.list_objects_v2(Bucket=bucket, Prefix=f"clients/{client_id}/")
                if 'Contents' in response:
                    for obj in response['Contents']:
                        if not obj['Key'].endswith('/'):
                            all_files[obj['Key']] = {
                                'location': f'client_{client_id}',
                                'size': obj['Size'],
                                'last_modified': obj['LastModified'],
                                'uuid': obj['Key'].split('/')[-1].split('.')[0]
                            }
            except:
                continue
        
        print(f"🔍 Analyzing patterns in {len(all_files)} total files...")
        
        # Group by timestamp (same day uploads might be related)
        timestamp_groups = {}
        for key, info in all_files.items():
            date_key = info['last_modified'].date()
            if date_key not in timestamp_groups:
                timestamp_groups[date_key] = []
            timestamp_groups[date_key].append({
                'key': key,
                'location': info['location'],
                'size': info['size'],
                'uuid': info['uuid']
            })
        
        # Look for patterns
        pattern_analysis = {
            'same_day_uploads': {},
            'size_clusters': {},
            'uuid_patterns': {}
        }
        
        # Same day upload analysis
        for date, files in timestamp_groups.items():
            if len(files) > 1:
                orphaned_count = len([f for f in files if f['location'] == 'orphaned'])
                client_count = len([f for f in files if f['location'] != 'orphaned'])
                
                if orphaned_count > 0 and client_count > 0:
                    pattern_analysis['same_day_uploads'][str(date)] = {
                        'orphaned_files': orphaned_count,
                        'client_files': client_count,
                        'total_files': len(files),
                        'files': files
                    }
        
        # Size clustering analysis
        size_ranges = [
            (0, 1024*1024),           # < 1MB
            (1024*1024, 10*1024*1024), # 1-10MB
            (10*1024*1024, 50*1024*1024), # 10-50MB
            (50*1024*1024, float('inf'))  # > 50MB
        ]
        
        for min_size, max_size in size_ranges:
            range_name = f"{min_size//1024//1024 if min_size > 0 else 0}-{max_size//1024//1024 if max_size != float('inf') else 'inf'}MB"
            
            orphaned_in_range = [f for f in all_files.values() 
                               if f['location'] == 'orphaned' and min_size <= f['size'] < max_size]
            client_in_range = [f for f in all_files.values() 
                             if f['location'] != 'orphaned' and min_size <= f['size'] < max_size]
            
            if orphaned_in_range and client_in_range:
                pattern_analysis['size_clusters'][range_name] = {
                    'orphaned_count': len(orphaned_in_range),
                    'client_count': len(client_in_range),
                    'avg_orphaned_size': sum(f['size'] for f in orphaned_in_range) / len(orphaned_in_range),
                    'avg_client_size': sum(f['size'] for f in client_in_range) / len(client_in_range)
                }
        
        tracker.session_data['pattern_analysis'] = pattern_analysis
        
        print(f"\n📊 Pattern Analysis Results:")
        print(f"   Same-day upload groups: {len(pattern_analysis['same_day_uploads'])}")
        print(f"   Size clusters with matches: {len(pattern_analysis['size_clusters'])}")
        
        return pattern_analysis
        
    except Exception as e:
        tracker.add_error('pattern_analysis', {
            'error_type': 'pattern_analysis_error',
            'error_message': str(e)
        })
        raise

def search_for_historical_references():
    """Search for any historical logs or files that might reference these UUIDs"""
    tracker = get_tracker()
    
    try:
        print("🔍 Searching for historical references to orphaned UUIDs...")
        
        # Get list of orphaned UUIDs
        s3 = boto3.client('s3')
        bucket = 'typing-clients-uuid-system'
        
        response = s3.list_objects_v2(Bucket=bucket, Prefix="files/")
        orphaned_uuids = set()
        
        if 'Contents' in response:
            for obj in response['Contents']:
                if not obj['Key'].endswith('/'):
                    uuid = obj['Key'].split('/')[-1].split('.')[0]
                    orphaned_uuids.add(uuid)
        
        # Search through local files for references
        
        reference_findings = {}
        
        # Search patterns
        search_locations = [
            "*.json",
            "*.csv", 
            "*.txt",
            "*.log",
            "logs/**/*.log",
            "outputs/**/*",
            "*.py"
        ]
        
        for pattern in search_locations:
            try:
                files = glob.glob(pattern, recursive=True)
                
                for file_path in files:
                    try:
                        # Skip large files
                        if os.path.getsize(file_path) > 50 * 1024 * 1024:  # Skip files > 50MB
                            continue
                            
                        with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                            content = f.read()
                            
                            found_uuids = []
                            for uuid in orphaned_uuids:
                                if uuid in content:
                                    found_uuids.append(uuid)
                            
                            if found_uuids:
                                reference_findings[file_path] = {
                                    'found_uuids': found_uuids,
                                    'file_size': os.path.getsize(file_path),
                                    'file_type': os.path.splitext(file_path)[1]
                                }
                                print(f"✅ Found {len(found_uuids)} UUIDs in {file_path}")
                    
                    except Exception as e:
                        continue
                        
            except Exception as e:
                continue
        
        tracker.session_data['reference_findings'] = reference_findings
        
        print(f"\n📊 Historical Reference Search Results:")
        print(f"   Files with UUID references: {len(reference_findings)}")
        total_found = sum(len(data['found_uuids']) for data in reference_findings.values())
        print(f"   Total UUID references found: {total_found}")
        
        return reference_findings
        
    except Exception as e:
        tracker.add_error('historical_search', {
            'error_type': 'historical_search_error',
            'error_message': str(e)
        })
        raise

def create_smart_mapping_candidates():
    """Create smart mapping candidates based on all available evidence"""
    tracker = get_tracker()
    
    try:
        # Load all analysis results
        content_clues = tracker.session_data.get('content_clues', [])
        pattern_analysis = tracker.session_data.get('pattern_analysis', {})
        reference_findings = tracker.session_data.get('reference_findings', {})
        
        # Load CSV for client information
        df = pd.read_csv('outputs/output.csv')
        
        mapping_candidates = []
        
        print("🧠 Creating smart mapping candidates...")
        
        # Method 1: Content-based mapping
        for clue in content_clues:
            if clue['clues']:
                # Extract potential client names from content
                potential_names = []
                
                for key, value in clue['clues'].items():
                    if isinstance(value, str):
                        # Look for names that might match client names
                        words = re.findall(r'\b[A-Z][a-z]+\b', value)
                        potential_names.extend(words)
                    elif isinstance(value, list):
                        for item in value:
                            if isinstance(item, str):
                                words = re.findall(r'\b[A-Z][a-z]+\b', item)
                                potential_names.extend(words)
                
                # Match against client names
                for _, client_row in df.iterrows():
                    client_name = str(client_row['name']).strip()
                    name_parts = client_name.split()
                    
                    # Check for name matches
                    matches = []
                    for name_part in name_parts:
                        if len(name_part) > 2:  # Skip short words
                            for potential in potential_names:
                                if potential.lower() == name_part.lower():
                                    matches.append(potential)
                    
                    if matches:
                        confidence = len(matches) / len(name_parts)
                        mapping_candidates.append({
                            'uuid': clue['uuid'],
                            'client_row_id': client_row['row_id'],
                            'client_name': client_name,
                            'method': 'content_name_match',
                            'confidence': confidence,
                            'evidence': {
                                'matched_names': matches,
                                'content_clues': clue['clues']
                            }
                        })
        
        # Method 2: Size and timestamp correlation
        same_day_uploads = pattern_analysis.get('same_day_uploads', {})
        
        for date, upload_group in same_day_uploads.items():
            orphaned_files = [f for f in upload_group['files'] if f['location'] == 'orphaned']
            client_files = [f for f in upload_group['files'] if f['location'] != 'orphaned']
            
            # Try to match orphaned files to clients based on size similarity
            for orphaned_file in orphaned_files:
                for client_file in client_files:
                    size_diff = abs(orphaned_file['size'] - client_file['size'])
                    size_ratio = min(orphaned_file['size'], client_file['size']) / max(orphaned_file['size'], client_file['size'])
                    
                    if size_ratio > 0.8:  # Similar sizes
                        client_id = client_file['location'].replace('client_', '')
                        client_row = df[df['row_id'] == int(client_id)].iloc[0] if not df[df['row_id'] == int(client_id)].empty else None
                        
                        if client_row is not None:
                            mapping_candidates.append({
                                'uuid': orphaned_file['uuid'],
                                'client_row_id': int(client_id),
                                'client_name': client_row['name'],
                                'method': 'size_timestamp_correlation',
                                'confidence': size_ratio * 0.7,  # Lower confidence for size-based matching
                                'evidence': {
                                    'upload_date': date,
                                    'size_similarity': size_ratio,
                                    'size_diff_bytes': size_diff
                                }
                            })
        
        # Method 3: Historical reference mapping
        for file_path, ref_data in reference_findings.items():
            # Try to extract client context from filename/path
            for uuid in ref_data['found_uuids']:
                # Look for client IDs or names in the file path
                path_parts = file_path.lower().split('/')
                filename = os.path.basename(file_path).lower()
                
                for _, client_row in df.iterrows():
                    client_name_parts = str(client_row['name']).lower().split()
                    client_id = str(client_row['row_id'])
                    
                    # Check if client ID or name appears in file path
                    name_in_path = any(part in filename or part in ' '.join(path_parts) 
                                     for part in client_name_parts if len(part) > 2)
                    id_in_path = client_id in filename or client_id in ' '.join(path_parts)
                    
                    if name_in_path or id_in_path:
                        mapping_candidates.append({
                            'uuid': uuid,
                            'client_row_id': client_row['row_id'],
                            'client_name': client_row['name'],
                            'method': 'historical_reference',
                            'confidence': 0.8 if id_in_path else 0.6,
                            'evidence': {
                                'reference_file': file_path,
                                'match_type': 'id_match' if id_in_path else 'name_match'
                            }
                        })
        
        # Remove duplicates and sort by confidence
        unique_candidates = {}
        for candidate in mapping_candidates:
            key = f"{candidate['uuid']}_{candidate['client_row_id']}"
            if key not in unique_candidates or candidate['confidence'] > unique_candidates[key]['confidence']:
                unique_candidates[key] = candidate
        
        final_candidates = sorted(unique_candidates.values(), key=lambda x: x['confidence'], reverse=True)
        
        tracker.session_data['mapping_candidates'] = final_candidates
        
        print(f"\n📊 Smart Mapping Candidates Generated:")
        print(f"   Total candidates: {len(final_candidates)}")
        print(f"   High confidence (>0.8): {len([c for c in final_candidates if c['confidence'] > 0.8])}")
        print(f"   Medium confidence (0.5-0.8): {len([c for c in final_candidates if 0.5 <= c['confidence'] <= 0.8])}")
        print(f"   Low confidence (<0.5): {len([c for c in final_candidates if c['confidence'] < 0.5])}")
        
        return final_candidates
        
    except Exception as e:
        tracker.add_error('smart_mapping', {
            'error_type': 'smart_mapping_error',
            'error_message': str(e)
        })
        raise

if __name__ == "__main__":
    print("🔬 ADVANCED ORPHANED FILE RECOVERY")
    print("=" * 60)
    
    # Run all analysis methods
    print("\n1️⃣ ANALYZING FILE CONTENT FOR CLUES...")
    content_clues = analyze_file_content_for_clues()
    
    print("\n2️⃣ ANALYZING FILENAME AND TIMESTAMP PATTERNS...")
    pattern_analysis = analyze_filename_and_timestamp_patterns()
    
    print("\n3️⃣ SEARCHING FOR HISTORICAL REFERENCES...")
    reference_findings = search_for_historical_references()
    
    print("\n4️⃣ CREATING SMART MAPPING CANDIDATES...")
    mapping_candidates = create_smart_mapping_candidates()
    
    print(f"\n🎯 ADVANCED RECOVERY COMPLETE")
    print(f"   Methods tried: 4")
    print(f"   Mapping candidates found: {len(mapping_candidates)}")
    
    if mapping_candidates:
        print(f"\n✅ Top mapping candidates:")
        for i, candidate in enumerate(mapping_candidates[:10], 1):
            print(f"   {i}. UUID {candidate['uuid']} → {candidate['client_name']} (Row {candidate['client_row_id']})")
            print(f"      Method: {candidate['method']}, Confidence: {candidate['confidence']:.2f}")