#!/usr/bin/env python3
"""Advanced content analysis of remaining 79 orphaned files"""

import boto3
import pandas as pd
import json
import re
from datetime import datetime
from orphaned_file_recovery_tracker import get_tracker

def analyze_large_media_files_deep():
    """Deep analysis of 10 largest media files for client identification"""
    tracker = get_tracker()
    
    print("🎥 DEEP ANALYSIS OF LARGE MEDIA FILES")
    print("=" * 50)
    
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    
    # Large media files identified in previous analysis
    large_files = [
        {'uuid': 'fa7fa69d-79c6-4eac-9b8a-570edbceb431', 'ext': 'mp4', 'size_mb': 282.5},
        {'uuid': '0115f162-17c4-4392-bedb-870a1679da56', 'ext': 'mp4', 'size_mb': 266.2},
        {'uuid': '274a60ab-5acf-491b-94a8-3346ba1f2000', 'ext': 'mp4', 'size_mb': 173.3},
        {'uuid': '1dad364a-0cc3-4ea8-8a4e-fbc5a2867fe9', 'ext': 'mp4', 'size_mb': 156.2},
        {'uuid': '006cecbd-6a7f-4e28-9dee-6a1a2ce13cad', 'ext': 'mp4', 'size_mb': 135.3},
        {'uuid': '94e58533-dc61-4594-b463-438fa7d7e6f8', 'ext': 'mp4', 'size_mb': 125.6},
        {'uuid': '1f8274c6-4dc8-4681-a81a-03cef3b89aea', 'ext': 'mp4', 'size_mb': 120.4},
        {'uuid': 'ce9c48cb-080e-49ee-a9fd-1fafb9ee3b06', 'ext': 'mp4', 'size_mb': 117.9},
        {'uuid': 'e9ee93b0-2169-4c35-854c-3f62130609af', 'ext': 'm4a', 'size_mb': 108.9},
        {'uuid': '24b1278b-bd3b-4339-b876-b759d60bfeda', 'ext': 'mp3', 'size_mb': 107.7}
    ]
    
    content_clues = []
    
    for file_info in large_files:
        uuid = file_info['uuid']
        extension = file_info['ext']
        
        try:
            print(f"\n🔍 Analyzing {uuid}.{extension} ({file_info['size_mb']:.1f} MB)...")
            
            # Get multiple chunks for better metadata analysis
            chunks_to_analyze = [
                ('header', 0, 65535),      # First 64KB - header metadata
                ('middle', 1048576, 32767), # 1MB in, 32KB - might have embedded info
                ('footer', -65536, 65535)   # Last 64KB - footer metadata
            ]
            
            all_text_content = ""
            metadata_strings = set()
            
            for chunk_name, offset, size in chunks_to_analyze:
                try:
                    if offset < 0:
                        # Get file size first for negative offsets
                        head_response = s3.head_object(Bucket=bucket, Key=f"files/{uuid}.{extension}")
                        file_size = head_response['ContentLength']
                        start_byte = max(0, file_size + offset)
                        end_byte = file_size - 1
                        range_header = f"bytes={start_byte}-{end_byte}"
                    else:
                        range_header = f"bytes={offset}-{offset + size - 1}"
                    
                    response = s3.get_object(
                        Bucket=bucket,
                        Key=f"files/{uuid}.{extension}",
                        Range=range_header
                    )
                    
                    content = response['Body'].read()
                    text_content = content.decode('utf-8', errors='ignore')
                    all_text_content += f"\n--- {chunk_name} ---\n" + text_content
                    
                    # Extract potential metadata strings
                    # Look for readable strings that might be names, emails, etc.
                    readable_strings = re.findall(r'[A-Za-z]{2,}(?:\s+[A-Za-z]{2,})*', text_content)
                    
                    # Filter for potential names (2+ words, reasonable length)
                    potential_names = [s for s in readable_strings 
                                     if 5 <= len(s) <= 50 and ' ' in s and not any(
                                         word in s.lower() for word in ['codec', 'audio', 'video', 'stream', 'media', 'data', 'file'])]
                    
                    metadata_strings.update(potential_names)
                    
                except Exception as e:
                    print(f"     ⚠️  Error reading {chunk_name} chunk: {str(e)}")
                    continue
            
            # Extract structured metadata patterns
            extracted_info = {
                'emails': re.findall(r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b', all_text_content),
                'youtube_ids': re.findall(r'\b[a-zA-Z0-9_-]{11}\b', all_text_content),
                'urls': re.findall(r'https?://[^\s<>"]+', all_text_content),
                'potential_names': list(metadata_strings),
                'dates': re.findall(r'\b\d{4}-\d{2}-\d{2}\b', all_text_content)
            }
            
            # Clean and filter extracted info
            filtered_info = {}
            for key, values in extracted_info.items():
                if key == 'potential_names':
                    # Additional filtering for names
                    filtered_names = []
                    for name in values:
                        # Skip very common words/patterns
                        if not any(common in name.lower() for common in [
                            'frame', 'format', 'version', 'player', 'system', 'windows',
                            'ffmpeg', 'encoder', 'decoder', 'bitrate', 'sample'
                        ]):
                            filtered_names.append(name)
                    filtered_info[key] = filtered_names[:10]  # Top 10
                else:
                    filtered_info[key] = list(set(values))[:5]  # Top 5 unique
            
            if any(filtered_info.values()):
                content_clues.append({
                    'uuid': uuid,
                    'extension': extension,
                    'size_mb': file_info['size_mb'],
                    'extracted_info': filtered_info
                })
                
                print(f"     ✅ Found identifying information:")
                for info_type, values in filtered_info.items():
                    if values:
                        print(f"       {info_type}: {values[:3]}")  # Show first 3
            else:
                print(f"     📊 No clear identifying information found")
        
        except Exception as e:
            print(f"     ❌ Error analyzing {uuid}: {str(e)}")
            continue
    
    print(f"\n📊 Large File Analysis Results:")
    print(f"   Files analyzed: {len(large_files)}")
    print(f"   Files with identifying info: {len(content_clues)}")
    
    return content_clues

def analyze_remaining_json_files():
    """Deep analysis of remaining 8 JSON files"""
    print(f"\n📄 ANALYZING REMAINING JSON FILES")
    print("=" * 40)
    
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    
    # Get all remaining JSON files
    response = s3.list_objects_v2(Bucket=bucket, Prefix="files/")
    json_files = []
    
    if 'Contents' in response:
        for obj in response['Contents']:
            if obj['Key'].endswith('.json'):
                uuid = obj['Key'].split('/')[-1].replace('.json', '')
                json_files.append(uuid)
    
    print(f"   Found {len(json_files)} remaining JSON files")
    
    json_analysis = []
    
    for uuid in json_files:
        try:
            print(f"   🔍 Deep analyzing {uuid}.json...")
            
            response = s3.get_object(Bucket=bucket, Key=f"files/{uuid}.json")
            content = response['Body'].read().decode('utf-8')
            
            if not content.strip():
                continue
                
            data = json.loads(content)
            
            # Enhanced extraction - look deeper into nested structures
            clues = {
                'uuid': uuid,
                'drive_ids': [],
                'youtube_ids': [],
                'emails': [],
                'names': [],
                'titles': [],
                'descriptions': [],
                'urls': [],
                'file_names': [],
                'metadata': {}
            }
            
            def deep_extract(obj, path="", depth=0):
                if depth > 10:  # Prevent infinite recursion
                    return
                    
                if isinstance(obj, dict):
                    for key, value in obj.items():
                        current_path = f"{path}.{key}" if path else key
                        
                        # Store important metadata
                        if key.lower() in ['title', 'name', 'description', 'uploader', 'channel', 'creator', 'author']:
                            if isinstance(value, str) and value.strip():
                                clues['metadata'][key] = value
                                
                                if key.lower() in ['title', 'name']:
                                    clues['titles'].append(value)
                                elif key.lower() == 'description':
                                    clues['descriptions'].append(value)
                        
                        if isinstance(value, str) and value.strip():
                            # Extract URLs
                            if 'http' in value:
                                clues['urls'].append(value)
                            
                            # Extract Drive IDs with more patterns
                            drive_patterns = [
                                r'drive\.google\.com/file/d/([a-zA-Z0-9_-]+)',
                                r'docs\.google\.com/.*/d/([a-zA-Z0-9_-]+)',
                                r'/([a-zA-Z0-9_-]{33})[/?]',
                                r'/([a-zA-Z0-9_-]{28})[/?]',
                                r'/([a-zA-Z0-9_-]{25})[/?]',
                                r'id["\']?\s*:\s*["\']([a-zA-Z0-9_-]{20,})["\']'
                            ]
                            
                            for pattern in drive_patterns:
                                matches = re.findall(pattern, value)
                                clues['drive_ids'].extend(matches)
                            
                            # Extract YouTube IDs
                            youtube_patterns = [
                                r'youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})',
                                r'youtu\.be/([a-zA-Z0-9_-]{11})',
                                r'/([a-zA-Z0-9_-]{11})(?:\?|$|&)',
                                r'["\']([a-zA-Z0-9_-]{11})["\']'
                            ]
                            
                            for pattern in youtube_patterns:
                                matches = re.findall(pattern, value)
                                for match in matches:
                                    if re.match(r'^[a-zA-Z0-9_-]{11}$', match):
                                        clues['youtube_ids'].append(match)
                            
                            # Extract emails
                            email_matches = re.findall(r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b', value)
                            clues['emails'].extend(email_matches)
                            
                            # Extract potential names (enhanced patterns)
                            name_patterns = [
                                r'\b[A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?\b',  # Title Case Names
                                r'[Nn]ame["\']?\s*:\s*["\']([A-Za-z\s]+)["\']',        # name: "value"
                                r'[Uu]ser["\']?\s*:\s*["\']([A-Za-z\s]+)["\']',        # user: "value"
                                r'[Oo]wner["\']?\s*:\s*["\']([A-Za-z\s]+)["\']'        # owner: "value"
                            ]
                            
                            for pattern in name_patterns:
                                matches = re.findall(pattern, value)
                                clues['names'].extend(matches)
                            
                            # Extract potential file names
                            if any(ext in value.lower() for ext in ['.mp4', '.mp3', '.webm', '.m4a', '.avi', '.mov']):
                                clues['file_names'].append(value)
                        
                        if isinstance(value, (dict, list)):
                            deep_extract(value, current_path, depth + 1)
                
                elif isinstance(obj, list):
                    for i, item in enumerate(obj):
                        deep_extract(item, f"{path}[{i}]", depth + 1)
            
            deep_extract(data)
            
            # Remove duplicates and clean up
            for key in ['drive_ids', 'youtube_ids', 'emails', 'names', 'titles', 'descriptions', 'urls', 'file_names']:
                clues[key] = list(set([item.strip() for item in clues[key] if isinstance(item, str) and item.strip()]))
            
            # Filter out obvious non-names from names
            filtered_names = []
            for name in clues['names']:
                if (len(name.split()) >= 2 and 
                    not any(word in name.lower() for word in ['google', 'drive', 'youtube', 'video', 'audio', 'file', 'download']) and
                    len(name) <= 50):
                    filtered_names.append(name)
            clues['names'] = filtered_names
            
            if any(clues[key] for key in ['drive_ids', 'youtube_ids', 'emails', 'names']) or clues['metadata']:
                json_analysis.append(clues)
                
                print(f"     ✅ Enhanced extraction results:")
                if clues['names']:
                    print(f"       Names: {clues['names'][:3]}")
                if clues['emails']:
                    print(f"       Emails: {clues['emails'][:2]}")
                if clues['drive_ids']:
                    print(f"       Drive IDs: {len(clues['drive_ids'])} found")
                if clues['youtube_ids']:
                    print(f"       YouTube IDs: {clues['youtube_ids'][:2]}")
                if clues['metadata']:
                    print(f"       Metadata: {list(clues['metadata'].keys())}")
            else:
                print(f"     📊 No additional clues found")
        
        except Exception as e:
            print(f"     ❌ Error analyzing {uuid}: {str(e)}")
            continue
    
    print(f"\n📊 Enhanced JSON Analysis Results:")
    print(f"   Files with new clues: {len(json_analysis)}")
    
    return json_analysis

def analyze_unnamed_large_files():
    """Analyze the 6 unnamed large files (9GB total)"""
    print(f"\n🗂️ ANALYZING UNNAMED LARGE FILES")
    print("=" * 40)
    
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    
    # Get files without extensions (from previous analysis: 6 files, 9046.1 MB)
    response = s3.list_objects_v2(Bucket=bucket, Prefix="files/")
    unnamed_files = []
    
    if 'Contents' in response:
        for obj in response['Contents']:
            key = obj['Key']
            if not key.endswith('/'):
                filename = key.split('/')[-1]
                if '.' not in filename:  # No extension
                    unnamed_files.append({
                        'uuid': filename,
                        'size_mb': obj['Size'] / (1024 * 1024),
                        's3_key': key,
                        'last_modified': obj['LastModified']
                    })
    
    unnamed_files = sorted(unnamed_files, key=lambda x: x['size_mb'], reverse=True)
    
    print(f"   Found {len(unnamed_files)} unnamed files")
    total_size = sum(f['size_mb'] for f in unnamed_files)
    print(f"   Total size: {total_size:.1f} MB")
    
    file_type_analysis = []
    
    for file_info in unnamed_files:
        uuid = file_info['uuid']
        
        try:
            print(f"   🔍 Analyzing {uuid} ({file_info['size_mb']:.1f} MB)...")
            
            # Get first few KB to determine file type
            response = s3.get_object(
                Bucket=bucket,
                Key=file_info['s3_key'],
                Range='bytes=0-8191'
            )
            
            content = response['Body'].read()
            
            # File type detection based on magic bytes and content
            file_type = "unknown"
            readable_content = content.decode('utf-8', errors='ignore')
            
            # Check for common file signatures
            if content.startswith(b'\x00\x00\x00\x18ftypmp4') or content.startswith(b'\x00\x00\x00\x20ftypmp4'):
                file_type = "mp4"
            elif content.startswith(b'ID3') or b'MPEG' in content[:100]:
                file_type = "mp3"
            elif b'ftypisom' in content[:100] or b'ftypM4A' in content[:100]:
                file_type = "m4a"
            elif content.startswith(b'\x1aE\xdf\xa3'):
                file_type = "webm"
            elif content.startswith(b'RIFF') and b'AVI' in content[:100]:
                file_type = "avi"
            elif content.startswith(b'{') or readable_content.strip().startswith('{'):
                file_type = "json"
            
            # Extract any readable metadata
            metadata_strings = re.findall(r'[A-Za-z]{3,}(?:\s+[A-Za-z]{3,})*', readable_content)
            potential_names = [s for s in metadata_strings 
                             if 5 <= len(s) <= 50 and ' ' in s and 
                             not any(word in s.lower() for word in ['data', 'file', 'stream', 'format'])]
            
            file_type_analysis.append({
                'uuid': uuid,
                'size_mb': file_info['size_mb'],
                'detected_type': file_type,
                'potential_names': potential_names[:5],
                'last_modified': file_info['last_modified']
            })
            
            print(f"     📁 Detected type: {file_type}")
            if potential_names:
                print(f"     📝 Potential names: {potential_names[:3]}")
        
        except Exception as e:
            print(f"     ❌ Error analyzing {uuid}: {str(e)}")
            continue
    
    print(f"\n📊 Unnamed File Analysis Results:")
    type_distribution = {}
    for analysis in file_type_analysis:
        file_type = analysis['detected_type']
        type_distribution[file_type] = type_distribution.get(file_type, 0) + 1
    
    for file_type, count in type_distribution.items():
        print(f"   {file_type}: {count} files")
    
    return file_type_analysis

def correlate_all_findings_with_clients(content_clues, json_analysis, unnamed_analysis, df):
    """Correlate all findings with client data for mapping"""
    print(f"\n🎯 CORRELATING ALL FINDINGS WITH CLIENTS")
    print("=" * 50)
    
    all_mappings = []
    
    # Process large media file clues
    for clue_data in content_clues:
        best_matches = find_client_matches(clue_data['extracted_info'], df, clue_data['uuid'], 'large_media')
        all_mappings.extend(best_matches)
    
    # Process JSON analysis
    for json_data in json_analysis:
        best_matches = find_client_matches(json_data, df, json_data['uuid'], 'json_enhanced')
        all_mappings.extend(best_matches)
    
    # Process unnamed file analysis
    for unnamed_data in unnamed_analysis:
        if unnamed_data['potential_names']:
            search_data = {'potential_names': unnamed_data['potential_names']}
            best_matches = find_client_matches(search_data, df, unnamed_data['uuid'], 'unnamed_file')
            all_mappings.extend(best_matches)
    
    # Remove duplicates and sort by confidence
    unique_mappings = {}
    for mapping in all_mappings:
        key = f"{mapping['uuid']}_{mapping['client_row_id']}"
        if key not in unique_mappings or mapping['confidence'] > unique_mappings[key]['confidence']:
            unique_mappings[key] = mapping
    
    final_mappings = sorted(unique_mappings.values(), key=lambda x: x['confidence'], reverse=True)
    
    print(f"\n📊 Correlation Results:")
    print(f"   Total mappings found: {len(final_mappings)}")
    
    high_confidence = [m for m in final_mappings if m['confidence'] >= 0.7]
    medium_confidence = [m for m in final_mappings if 0.5 <= m['confidence'] < 0.7]
    
    print(f"   High confidence (≥70%): {len(high_confidence)}")
    print(f"   Medium confidence (50-70%): {len(medium_confidence)}")
    
    if high_confidence:
        print(f"\n🎯 High-Confidence Mappings Ready for Execution:")
        for mapping in high_confidence[:10]:  # Show top 10
            print(f"   {mapping['uuid']} → {mapping['client_name']} (Row {mapping['client_row_id']}) - {mapping['confidence']:.1%}")
            print(f"      Method: {mapping['method']}, Evidence: {mapping['evidence'][:50]}...")
    
    return final_mappings

def find_client_matches(search_data, df, uuid, method):
    """Find client matches for extracted data"""
    matches = []
    
    for _, client_row in df.iterrows():
        score = 0
        evidence = []
        
        client_name = str(client_row['name']).lower()
        client_email = str(client_row.get('email', '')).lower()
        client_youtube = str(client_row.get('youtube_links', ''))
        client_drive = str(client_row.get('drive_links', ''))
        client_doc = str(client_row.get('doc_link', ''))
        
        # Name matching
        names_to_check = []
        if 'potential_names' in search_data:
            names_to_check.extend(search_data['potential_names'])
        if 'names' in search_data:
            names_to_check.extend(search_data['names'])
        
        for found_name in names_to_check:
            name_lower = found_name.lower()
            client_parts = client_name.split()
            
            matches_count = sum(1 for part in client_parts 
                              if len(part) > 2 and part in name_lower)
            
            if matches_count >= 2:  # Full name match
                score += 0.8
                evidence.append(f"full_name:{found_name}")
            elif matches_count == 1:  # Partial name match
                score += 0.4
                evidence.append(f"partial_name:{found_name}")
        
        # Email matching
        emails_to_check = search_data.get('emails', [])
        for found_email in emails_to_check:
            if found_email.lower() == client_email:
                score += 0.9
                evidence.append(f"exact_email:{found_email}")
            elif client_email and found_email.lower().startswith(client_email.split('@')[0]):
                score += 0.6
                evidence.append(f"email_username:{found_email}")
        
        # YouTube ID matching
        youtube_ids = search_data.get('youtube_ids', [])
        for youtube_id in youtube_ids:
            if youtube_id in client_youtube:
                score += 0.8
                evidence.append(f"youtube_id:{youtube_id}")
        
        # Drive ID matching
        drive_ids = search_data.get('drive_ids', [])
        for drive_id in drive_ids:
            if drive_id in client_drive or drive_id in client_doc:
                score += 0.7
                evidence.append(f"drive_id:{drive_id[:10]}...")
        
        if score >= 0.5:  # Meaningful match
            matches.append({
                'uuid': uuid,
                'client_row_id': client_row['row_id'],
                'client_name': client_row['name'],
                'confidence': score,
                'method': method,
                'evidence': ', '.join(evidence)
            })
    
    return matches

def main():
    """Main advanced analysis function"""
    print("🚀 ADVANCED CONTENT ANALYSIS - REMAINING 79 FILES")
    print("=" * 60)
    
    # Load client data
    df = pd.read_csv('outputs/output.csv')
    
    # Step 1: Deep analysis of large media files
    content_clues = analyze_large_media_files_deep()
    
    # Step 2: Enhanced JSON analysis
    json_analysis = analyze_remaining_json_files()
    
    # Step 3: Analyze unnamed large files
    unnamed_analysis = analyze_unnamed_large_files()
    
    # Step 4: Correlate all findings with clients
    final_mappings = correlate_all_findings_with_clients(content_clues, json_analysis, unnamed_analysis, df)
    
    # Return results for potential execution
    return {
        'content_clues': content_clues,
        'json_analysis': json_analysis,
        'unnamed_analysis': unnamed_analysis,
        'final_mappings': final_mappings
    }

if __name__ == "__main__":
    results = main()
    
    high_confidence = [m for m in results['final_mappings'] if m['confidence'] >= 0.7]
    if high_confidence:
        print(f"\n✅ Found {len(high_confidence)} high-confidence mappings ready for execution!")
    else:
        print(f"\n📊 Analysis complete - review medium confidence candidates for manual verification")