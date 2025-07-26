#!/usr/bin/env python3
"""Targeted approach to recover specific high-value orphaned files"""

import boto3
import pandas as pd
import json
import re
from collections import defaultdict

def get_orphaned_file_inventory():
    """Get detailed inventory of all orphaned files"""
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    
    response = s3.list_objects_v2(Bucket=bucket, Prefix="files/")
    files = []
    
    if 'Contents' in response:
        for obj in response['Contents']:
            if not obj['Key'].endswith('/'):
                filename = obj['Key'].split('/')[-1]
                uuid = filename.split('.')[0]
                extension = filename.split('.')[-1] if '.' in filename else 'no_ext'
                
                files.append({
                    'uuid': uuid,
                    'filename': filename,
                    'extension': extension,
                    'size_bytes': obj['Size'],
                    'size_mb': obj['Size'] / (1024 * 1024),
                    'last_modified': obj['LastModified'],
                    's3_key': obj['Key']
                })
    
    return sorted(files, key=lambda x: x['size_mb'], reverse=True)

def analyze_file_patterns(files):
    """Analyze patterns in orphaned files"""
    print("📊 ANALYZING FILE PATTERNS")
    print("=" * 40)
    
    # Group by file type
    by_extension = defaultdict(list)
    by_size_range = defaultdict(list)
    by_date = defaultdict(list)
    
    for file_info in files:
        by_extension[file_info['extension']].append(file_info)
        
        # Size ranges
        size_mb = file_info['size_mb']
        if size_mb < 1:
            range_key = "< 1MB"
        elif size_mb < 10:
            range_key = "1-10MB"
        elif size_mb < 50:
            range_key = "10-50MB"
        elif size_mb < 200:
            range_key = "50-200MB"
        else:
            range_key = "> 200MB"
        
        by_size_range[range_key].append(file_info)
        
        # Date grouping
        date_key = file_info['last_modified'].date()
        by_date[date_key].append(file_info)
    
    print(f"📋 File Type Distribution:")
    for ext, files_list in sorted(by_extension.items()):
        total_size = sum(f['size_mb'] for f in files_list)
        print(f"   {ext}: {len(files_list)} files ({total_size:.1f} MB)")
    
    print(f"\n📏 Size Distribution:")
    for size_range, files_list in sorted(by_size_range.items()):
        print(f"   {size_range}: {len(files_list)} files")
    
    print(f"\n📅 Upload Date Distribution:")
    for date, files_list in sorted(by_date.items()):
        print(f"   {date}: {len(files_list)} files")
    
    return {
        'by_extension': by_extension,
        'by_size_range': by_size_range,
        'by_date': by_date
    }

def check_json_files_for_client_clues(json_files):
    """Deep dive into JSON files for client identification clues"""
    print("\n🔍 ANALYZING JSON FILES FOR CLIENT CLUES")
    print("=" * 40)
    
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    
    json_analysis = []
    
    for json_file in json_files:
        try:
            print(f"   Analyzing {json_file['filename']}...")
            
            # Get JSON content
            response = s3.get_object(Bucket=bucket, Key=json_file['s3_key'])
            content = response['Body'].read().decode('utf-8')
            
            if not content.strip():
                continue
                
            data = json.loads(content)
            
            # Extract all possible identifying information
            clues = {
                'uuid': json_file['uuid'],
                'size_mb': json_file['size_mb'],
                'urls': [],
                'drive_ids': [],
                'youtube_ids': [],
                'emails': [],
                'names': [],
                'metadata': {}
            }
            
            def extract_recursive(obj, path=""):
                if isinstance(obj, dict):
                    for key, value in obj.items():
                        if isinstance(value, str) and value.strip():
                            # Store important metadata
                            if key.lower() in ['title', 'name', 'uploader', 'channel', 'description']:
                                clues['metadata'][key] = value
                            
                            # Extract URLs
                            if 'http' in value:
                                clues['urls'].append(value)
                            
                            # Extract Google Drive IDs (various patterns)
                            drive_patterns = [
                                r'drive\.google\.com/file/d/([a-zA-Z0-9_-]+)',
                                r'docs\.google\.com/.*/([a-zA-Z0-9_-]{25,})',
                                r'/([a-zA-Z0-9_-]{33})[/?]',  # 33-char Drive IDs
                                r'/([a-zA-Z0-9_-]{28})[/?]',  # 28-char Drive IDs
                            ]
                            
                            for pattern in drive_patterns:
                                matches = re.findall(pattern, value)
                                clues['drive_ids'].extend(matches)
                            
                            # Extract YouTube video IDs
                            youtube_patterns = [
                                r'youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})',
                                r'youtu\.be/([a-zA-Z0-9_-]{11})',
                                r'^[a-zA-Z0-9_-]{11}$'  # Just the ID
                            ]
                            
                            for pattern in youtube_patterns:
                                matches = re.findall(pattern, value)
                                clues['youtube_ids'].extend(matches)
                            
                            # Extract email addresses
                            email_matches = re.findall(r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b', value)
                            clues['emails'].extend(email_matches)
                            
                            # Extract potential names (2+ capitalized words)
                            name_matches = re.findall(r'\b[A-Z][a-z]+\s+[A-Z][a-z]+(?:\s+[A-Z][a-z]+)?\b', value)
                            clues['names'].extend(name_matches)
                        
                        if isinstance(value, (dict, list)):
                            extract_recursive(value, f"{path}.{key}")
                
                elif isinstance(obj, list):
                    for i, item in enumerate(obj):
                        extract_recursive(item, f"{path}[{i}]")
            
            extract_recursive(data)
            
            # Remove duplicates
            for key in ['urls', 'drive_ids', 'youtube_ids', 'emails', 'names']:
                clues[key] = list(set(clues[key]))
            
            # Only keep files with meaningful clues
            if any(clues[key] for key in ['drive_ids', 'youtube_ids', 'emails', 'names']) or clues['metadata']:
                json_analysis.append(clues)
                
                print(f"     ✅ Found clues:")
                if clues['names']:
                    print(f"       Names: {clues['names'][:3]}")
                if clues['emails']:
                    print(f"       Emails: {clues['emails'][:2]}")
                if clues['drive_ids']:
                    print(f"       Drive IDs: {len(clues['drive_ids'])} found")
                if clues['youtube_ids']:
                    print(f"       YouTube IDs: {clues['youtube_ids'][:2]}")
        
        except Exception as e:
            print(f"     ❌ Error: {str(e)}")
            continue
    
    print(f"\n📊 JSON Analysis Results:")
    print(f"   Files with clues: {len(json_analysis)}")
    
    return json_analysis

def cross_reference_with_clients(json_analysis, df):
    """Cross-reference extracted clues with client data"""
    print("\n🎯 CROSS-REFERENCING WITH CLIENT DATA")
    print("=" * 40)
    
    matches = []
    
    for clue_data in json_analysis:
        uuid = clue_data['uuid']
        best_matches = []
        
        for _, client_row in df.iterrows():
            score = 0
            evidence = []
            
            client_name = str(client_row['name']).lower()
            client_email = str(client_row.get('email', '')).lower()
            client_youtube = str(client_row.get('youtube_links', ''))
            client_drive = str(client_row.get('drive_links', ''))
            client_doc = str(client_row.get('doc_link', ''))
            
            # Name matching
            for found_name in clue_data['names']:
                name_lower = found_name.lower()
                client_parts = client_name.split()
                
                matches_count = sum(1 for part in client_parts 
                                  if len(part) > 2 and part in name_lower)
                
                if matches_count >= 2:  # First and last name
                    score += 0.8
                    evidence.append(f"full_name:{found_name}")
                elif matches_count == 1:
                    score += 0.4
                    evidence.append(f"partial_name:{found_name}")
            
            # Email matching
            for found_email in clue_data['emails']:
                if found_email.lower() == client_email:
                    score += 0.9
                    evidence.append(f"exact_email:{found_email}")
                elif client_email and found_email.lower().startswith(client_email.split('@')[0]):
                    score += 0.6
                    evidence.append(f"email_username:{found_email}")
            
            # YouTube ID matching
            for youtube_id in clue_data['youtube_ids']:
                if youtube_id in client_youtube:
                    score += 0.8
                    evidence.append(f"youtube_id:{youtube_id}")
            
            # Drive ID matching
            for drive_id in clue_data['drive_ids']:
                if drive_id in client_drive or drive_id in client_doc:
                    score += 0.7
                    evidence.append(f"drive_id:{drive_id[:10]}...")
            
            if score >= 0.5:  # Meaningful match
                best_matches.append({
                    'client_row_id': client_row['row_id'],
                    'client_name': client_row['name'],
                    'score': score,
                    'evidence': evidence
                })
        
        if best_matches:
            # Sort by score and take best match
            best_match = max(best_matches, key=lambda x: x['score'])
            
            matches.append({
                'uuid': uuid,
                'clue_data': clue_data,
                'best_match': best_match
            })
            
            print(f"   ✅ {uuid} → {best_match['client_name']} (Row {best_match['client_row_id']})")
            print(f"      Score: {best_match['score']:.2f}, Evidence: {', '.join(best_match['evidence'][:3])}")
    
    print(f"\n📊 Cross-Reference Results:")
    print(f"   High-confidence matches: {len([m for m in matches if m['best_match']['score'] >= 0.7])}")
    print(f"   Medium-confidence matches: {len([m for m in matches if 0.5 <= m['best_match']['score'] < 0.7])}")
    
    return matches

def execute_targeted_recovery():
    """Execute the targeted recovery approach"""
    print("🎯 TARGETED ORPHANED FILE RECOVERY")
    print("=" * 50)
    
    # Step 1: Get inventory
    print("\n1️⃣ GETTING FILE INVENTORY")
    files = get_orphaned_file_inventory()
    print(f"   Found {len(files)} orphaned files")
    print(f"   Total size: {sum(f['size_mb'] for f in files):.1f} MB")
    
    # Step 2: Analyze patterns
    patterns = analyze_file_patterns(files)
    
    # Step 3: Focus on JSON files (most likely to contain identifying info)
    json_files = patterns['by_extension'].get('json', [])
    if not json_files:
        print("\n❌ No JSON files found to analyze")
        return
    
    json_analysis = check_json_files_for_client_clues(json_files)
    
    if not json_analysis:
        print("\n❌ No meaningful clues found in JSON files")
        return
    
    # Step 4: Cross-reference with client data
    df = pd.read_csv('outputs/output.csv')
    matches = cross_reference_with_clients(json_analysis, df)
    
    if not matches:
        print("\n❌ No client matches found")
        return
    
    # Step 5: Report findings
    print(f"\n🎉 RECOVERY OPPORTUNITIES FOUND")
    print(f"   Potential mappings: {len(matches)}")
    
    high_confidence = [m for m in matches if m['best_match']['score'] >= 0.7]
    if high_confidence:
        print(f"\n🎯 High-Confidence Mappings Ready for Execution:")
        for match in high_confidence:
            best = match['best_match']
            uuid = match['uuid']
            print(f"   {uuid} → {best['client_name']} (Row {best['client_row_id']}) - {best['score']:.1%}")
    
    return matches

if __name__ == "__main__":
    matches = execute_targeted_recovery()
    
    if matches:
        high_conf = len([m for m in matches if m['best_match']['score'] >= 0.7])
        print(f"\n✅ Found {high_conf} high-confidence mappings ready for execution!")
    else:
        print(f"\n❌ No mappings found with this approach")