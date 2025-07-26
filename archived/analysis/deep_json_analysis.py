#!/usr/bin/env python3
"""Deep analysis of JSON metadata files to correlate YouTube/Drive IDs with clients"""

import boto3
import pandas as pd
import json
import re
from orphaned_file_recovery_tracker import get_tracker

def extract_ids_from_json_content(json_data):
    """Extract YouTube video IDs and Google Drive IDs from JSON content"""
    ids = {
        'youtube_ids': [],
        'drive_ids': [],
        'urls': [],
        'titles': [],
        'metadata': {}
    }
    
    def recursive_extract(obj, path=""):
        if isinstance(obj, dict):
            for key, value in obj.items():
                current_path = f"{path}.{key}" if path else key
                
                # Extract specific metadata fields
                if key.lower() in ['title', 'webpage_url', 'url', 'id', 'uploader', 'channel', 'description']:
                    if isinstance(value, str) and value.strip():
                        ids['metadata'][key] = value
                
                # Extract YouTube video IDs
                if key.lower() in ['id', 'video_id', 'youtube_id'] and isinstance(value, str):
                    # Check if it's a YouTube video ID format (11 characters, alphanumeric + _ -)
                    if re.match(r'^[a-zA-Z0-9_-]{11}$', value):
                        ids['youtube_ids'].append(value)
                
                # Extract URLs
                if key.lower() in ['url', 'webpage_url', 'original_url'] and isinstance(value, str):
                    ids['urls'].append(value)
                    
                    # Extract YouTube IDs from URLs
                    youtube_patterns = [
                        r'youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})',
                        r'youtu\.be/([a-zA-Z0-9_-]{11})',
                        r'youtube\.com/embed/([a-zA-Z0-9_-]{11})'
                    ]
                    
                    for pattern in youtube_patterns:
                        matches = re.findall(pattern, value)
                        for match in matches:
                            if match not in ids['youtube_ids']:
                                ids['youtube_ids'].append(match)
                    
                    # Extract Google Drive IDs
                    drive_patterns = [
                        r'drive\.google\.com/file/d/([a-zA-Z0-9_-]+)',
                        r'docs\.google\.com/document/d/([a-zA-Z0-9_-]+)',
                        r'docs\.google\.com/spreadsheets/d/([a-zA-Z0-9_-]+)',
                        r'/([a-zA-Z0-9_-]{25,})[/?]'  # Generic long ID pattern
                    ]
                    
                    for pattern in drive_patterns:
                        matches = re.findall(pattern, value)
                        for match in matches:
                            if len(match) >= 20 and match not in ids['drive_ids']:  # Drive IDs are typically long
                                ids['drive_ids'].append(match)
                
                # Extract titles
                if key.lower() in ['title', 'name', 'filename'] and isinstance(value, str):
                    if value.strip() and len(value) > 3:
                        ids['titles'].append(value.strip())
                
                # Recurse into nested objects
                if isinstance(value, (dict, list)):
                    recursive_extract(value, current_path)
                    
        elif isinstance(obj, list):
            for i, item in enumerate(obj):
                recursive_extract(item, f"{path}[{i}]")
    
    recursive_extract(json_data)
    
    # Remove duplicates
    ids['youtube_ids'] = list(set(ids['youtube_ids']))
    ids['drive_ids'] = list(set(ids['drive_ids']))
    ids['urls'] = list(set(ids['urls']))
    ids['titles'] = list(set(ids['titles']))
    
    return ids

def correlate_with_clients(extracted_data, df):
    """Correlate extracted IDs with client data"""
    correlations = []
    
    for _, client_row in df.iterrows():
        client_score = 0
        correlation_evidence = []
        
        client_name = str(client_row['name']).lower()
        client_email = str(client_row.get('email', '')).lower()
        client_youtube_links = str(client_row.get('youtube_links', ''))
        client_drive_links = str(client_row.get('drive_links', ''))
        client_doc_link = str(client_row.get('doc_link', ''))
        
        # Check YouTube ID matches
        for youtube_id in extracted_data['youtube_ids']:
            if youtube_id in client_youtube_links:
                client_score += 0.8  # Very strong match
                correlation_evidence.append(f"youtube_id_match:{youtube_id}")
        
        # Check Drive ID matches
        for drive_id in extracted_data['drive_ids']:
            if drive_id in client_drive_links or drive_id in client_doc_link:
                client_score += 0.7  # Strong match
                correlation_evidence.append(f"drive_id_match:{drive_id}")
        
        # Check URL matches
        for url in extracted_data['urls']:
            if url in client_youtube_links or url in client_drive_links or url in client_doc_link:
                client_score += 0.6
                correlation_evidence.append(f"url_match:{url[:50]}...")
        
        # Check name matches in titles
        name_parts = client_name.split()
        for title in extracted_data['titles']:
            title_lower = title.lower()
            name_matches = 0
            for name_part in name_parts:
                if len(name_part) > 2 and name_part in title_lower:
                    name_matches += 1
            
            if name_matches > 0:
                name_score = min(name_matches * 0.2, 0.5)  # Cap at 0.5
                client_score += name_score
                correlation_evidence.append(f"name_in_title:{title[:30]}...")
        
        # Check email username in metadata
        if client_email and '@' in client_email:
            email_username = client_email.split('@')[0]
            for key, value in extracted_data['metadata'].items():
                if email_username in str(value).lower():
                    client_score += 0.4
                    correlation_evidence.append(f"email_in_metadata:{key}")
        
        if client_score > 0.3:  # Only include meaningful correlations
            correlations.append({
                'client_row_id': client_row['row_id'],
                'client_name': client_row['name'],
                'client_email': client_row.get('email', ''),
                'correlation_score': client_score,
                'evidence': correlation_evidence
            })
    
    return sorted(correlations, key=lambda x: x['correlation_score'], reverse=True)

def analyze_json_files_for_correlations():
    """Analyze all JSON files in orphaned directory for ID correlations"""
    tracker = get_tracker()
    
    print("🔍 Analyzing JSON files in orphaned directory...")
    
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    
    # Get all JSON files in orphaned directory
    response = s3.list_objects_v2(Bucket=bucket, Prefix="files/")
    json_files = []
    
    if 'Contents' in response:
        for obj in response['Contents']:
            if obj['Key'].endswith('.json'):
                json_files.append({
                    'key': obj['Key'],
                    'uuid': obj['Key'].split('/')[-1].replace('.json', ''),
                    'size': obj['Size']
                })
    
    print(f"   Found {len(json_files)} JSON files to analyze")
    
    # Load client data
    df = pd.read_csv('outputs/output.csv')
    
    # Analyze each JSON file
    potential_mappings = []
    
    for json_file in json_files:
        try:
            print(f"   Analyzing {json_file['uuid']}.json...")
            
            # Download and parse JSON content
            response = s3.get_object(Bucket=bucket, Key=json_file['key'])
            content = response['Body'].read().decode('utf-8')
            
            if not content.strip():
                continue
                
            json_data = json.loads(content)
            
            # Extract IDs and metadata
            extracted_data = extract_ids_from_json_content(json_data)
            
            if not any([extracted_data['youtube_ids'], extracted_data['drive_ids'], 
                       extracted_data['urls'], extracted_data['titles']]):
                continue
            
            print(f"     - YouTube IDs: {len(extracted_data['youtube_ids'])}")
            print(f"     - Drive IDs: {len(extracted_data['drive_ids'])}")
            print(f"     - URLs: {len(extracted_data['urls'])}")
            print(f"     - Titles: {len(extracted_data['titles'])}")
            
            # Correlate with clients
            correlations = correlate_with_clients(extracted_data, df)
            
            if correlations:
                best_correlation = correlations[0]
                if best_correlation['correlation_score'] >= 0.5:  # High confidence threshold
                    potential_mappings.append({
                        'uuid': json_file['uuid'],
                        'file_type': 'json',
                        'extracted_data': extracted_data,
                        'best_match': best_correlation,
                        'all_correlations': correlations[:3]  # Top 3 matches
                    })
                    
                    print(f"     ✅ Strong correlation found: {best_correlation['client_name']} "
                          f"(Score: {best_correlation['correlation_score']:.2f})")
                else:
                    print(f"     ⚠️  Weak correlation: {best_correlation['client_name']} "
                          f"(Score: {best_correlation['correlation_score']:.2f})")
        
        except Exception as e:
            print(f"     ❌ Error analyzing {json_file['uuid']}: {str(e)}")
            continue
    
    # Look for companion media files for each JSON mapping
    enriched_mappings = []
    
    for mapping in potential_mappings:
        uuid = mapping['uuid']
        
        # Check for companion media files with same UUID
        companion_files = []
        for ext in ['mp3', 'mp4', 'webm', 'm4a']:
            try:
                s3.head_object(Bucket=bucket, Key=f"files/{uuid}.{ext}")
                companion_files.append(f"{uuid}.{ext}")
            except:
                continue
        
        if companion_files:
            mapping['companion_files'] = companion_files
            enriched_mappings.append(mapping)
            print(f"   📦 {uuid}: JSON + {len(companion_files)} media files → {mapping['best_match']['client_name']}")
    
    print(f"\n📊 JSON Analysis Results:")
    print(f"   JSON files analyzed: {len(json_files)}")
    print(f"   High-confidence correlations: {len(enriched_mappings)}")
    print(f"   Total files that could be mapped: {sum(1 + len(m.get('companion_files', [])) for m in enriched_mappings)}")
    
    if enriched_mappings:
        print(f"\n🎯 Potential Mappings Found:")
        for i, mapping in enumerate(enriched_mappings, 1):
            match = mapping['best_match']
            total_files = 1 + len(mapping.get('companion_files', []))
            print(f"   {i}. {mapping['uuid']} ({total_files} files) → {match['client_name']} (Row {match['client_row_id']})")
            print(f"      Score: {match['correlation_score']:.2f}, Evidence: {', '.join(match['evidence'][:3])}")
            if 'companion_files' in mapping:
                print(f"      Files: {mapping['uuid']}.json + {', '.join(mapping['companion_files'])}")
    
    tracker.session_data['json_correlations'] = {
        'total_analyzed': len(json_files),
        'high_confidence_mappings': enriched_mappings,
        'total_recoverable_files': sum(1 + len(m.get('companion_files', [])) for m in enriched_mappings)
    }
    
    return enriched_mappings

if __name__ == "__main__":
    print("🔍 DEEP JSON CONTENT ANALYSIS")
    print("=" * 60)
    
    mappings = analyze_json_files_for_correlations()
    
    if mappings:
        print(f"\n✅ Found {len(mappings)} high-confidence mappings!")
        total_files = sum(1 + len(m.get('companion_files', [])) for m in mappings)
        print(f"   Total files ready for recovery: {total_files}")
    else:
        print(f"\n❌ No high-confidence correlations found in JSON analysis")