#!/usr/bin/env python3
"""Complete recovery system for all 88 remaining orphaned files"""

import boto3
import pandas as pd
import json
import re
from datetime import datetime
from orphaned_file_recovery_tracker import get_tracker

def get_current_orphaned_files():
    """Get detailed info on all current orphaned files"""
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    
    response = s3.list_objects_v2(Bucket=bucket, Prefix="files/")
    orphaned_files = []
    
    if 'Contents' in response:
        for obj in response['Contents']:
            if not obj['Key'].endswith('/'):
                filename = obj['Key'].split('/')[-1]
                uuid = filename.split('.')[0]
                extension = filename.split('.')[-1] if '.' in filename else 'no_ext'
                
                orphaned_files.append({
                    's3_key': obj['Key'],
                    'uuid': uuid,
                    'filename': filename,
                    'extension': extension,
                    'size': obj['Size'],
                    'size_mb': obj['Size'] / (1024 * 1024),
                    'last_modified': obj['LastModified']
                })
    
    return sorted(orphaned_files, key=lambda x: x['size'], reverse=True)

def search_expanded_logs():
    """Search through more log files for client-file mappings"""
    import glob
    
    print("🔍 Searching expanded log files for client mappings...")
    
    # Search all log files
    log_patterns = [
        "logs/**/*.log",
        "*.log",
        "**/*.txt",
        "*.json"
    ]
    
    client_file_mappings = {}
    
    for pattern in log_patterns:
        files = glob.glob(pattern, recursive=True)
        
        for file_path in files:
            try:
                if 'recovery_session' in file_path:
                    continue  # Skip our own recovery logs
                    
                with open(file_path, 'r', encoding='utf-8', errors='ignore') as f:
                    content = f.read()
                    
                    # Look for patterns that indicate client-file associations
                    patterns = [
                        # Pattern: "Missing in S3: files/UUID" with client context
                        r'📤.*?for\s+(.+?)\s+\(Row\s+(\d+)\).*?Missing in S3: files/([a-f0-9-]+)',
                        # Pattern: "Uploading X as files/UUID" 
                        r'Uploading\s+(.+?)\s+as\s+files/([a-f0-9-]+)',
                        # Pattern: Client name followed by UUID
                        r'(\w+\s+\w+).*?([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})',
                        # Pattern: Row ID with UUID
                        r'Row\s+(\d+).*?([a-f0-9]{8}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{4}-[a-f0-9]{12})'
                    ]
                    
                    for pattern in patterns:
                        matches = re.findall(pattern, content, re.MULTILINE | re.DOTALL)
                        for match in matches:
                            if len(match) >= 2:
                                if len(match) == 3:  # Pattern with client name, row, uuid
                                    client_name, row_id, uuid = match
                                    client_file_mappings[uuid] = {
                                        'client_name': client_name.strip(),
                                        'row_id': int(row_id),
                                        'source_file': file_path,
                                        'confidence': 0.9
                                    }
                                elif len(match) == 2:
                                    # Could be filename-uuid or name-uuid or row-uuid
                                    if re.match(r'^\d+$', match[0]):  # Row ID
                                        client_file_mappings[match[1]] = {
                                            'row_id': int(match[0]),
                                            'source_file': file_path,
                                            'confidence': 0.8
                                        }
                                    else:  # Client name or filename
                                        client_file_mappings[match[1]] = {
                                            'identifier': match[0],
                                            'source_file': file_path,
                                            'confidence': 0.6
                                        }
                        
            except Exception as e:
                continue
    
    print(f"   Found {len(client_file_mappings)} potential mappings in logs")
    return client_file_mappings

def analyze_media_content_deep(orphaned_files, sample_size=20):
    """Deep analysis of media file content for identifying information"""
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    
    print(f"🔍 Deep content analysis of {min(sample_size, len(orphaned_files))} largest media files...")
    
    # Focus on largest files first (more likely to contain valuable content)
    large_media_files = [f for f in orphaned_files[:sample_size] 
                        if f['extension'] in ['mp4', 'mp3', 'webm', 'm4a'] and f['size_mb'] > 10]
    
    content_clues = {}
    
    for file_info in large_media_files:
        try:
            print(f"   Analyzing {file_info['filename']} ({file_info['size_mb']:.1f} MB)...")
            
            # Get first 32KB for metadata analysis
            response = s3.get_object(
                Bucket=bucket,
                Key=file_info['s3_key'],
                Range='bytes=0-32767'
            )
            content = response['Body'].read()
            
            # Extract readable strings
            text_content = content.decode('utf-8', errors='ignore')
            
            # Look for metadata patterns
            metadata_patterns = {
                'names': re.findall(r'\b[A-Z][a-z]+\s+[A-Z][a-z]+\b', text_content),
                'emails': re.findall(r'\b[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}\b', text_content),
                'youtube_ids': re.findall(r'\b[a-zA-Z0-9_-]{11}\b', text_content),
                'urls': re.findall(r'https?://[^\s<>"]+', text_content),
                'dates': re.findall(r'\b\d{4}-\d{2}-\d{2}\b', text_content)
            }
            
            # Filter out common false positives
            filtered_names = [name for name in metadata_patterns['names'] 
                            if len(name) > 5 and not any(word in name.lower() 
                            for word in ['codec', 'audio', 'video', 'media', 'stream'])]
            
            if any(metadata_patterns.values()) or filtered_names:
                content_clues[file_info['uuid']] = {
                    'file_info': file_info,
                    'metadata': {k: v for k, v in metadata_patterns.items() if v},
                    'filtered_names': filtered_names
                }
                
                if filtered_names or metadata_patterns['emails']:
                    print(f"     ✅ Found identifying info: {filtered_names or metadata_patterns['emails']}")
                else:
                    print(f"     📊 Found metadata: {list(metadata_patterns.keys())}")
        
        except Exception as e:
            print(f"     ❌ Error analyzing {file_info['filename']}: {str(e)}")
            continue
    
    return content_clues

def correlate_with_client_data(orphaned_files, log_mappings, content_clues, df):
    """Correlate all available data with client records"""
    print("🔍 Correlating data with client records...")
    
    potential_mappings = []
    
    # Method 1: Direct log mappings
    for uuid, mapping_data in log_mappings.items():
        # Find corresponding orphaned file
        orphaned_file = next((f for f in orphaned_files if f['uuid'] == uuid), None)
        if not orphaned_file:
            continue
            
        if 'row_id' in mapping_data:
            client_row = df[df['row_id'] == mapping_data['row_id']]
            if not client_row.empty:
                potential_mappings.append({
                    'uuid': uuid,
                    'file_info': orphaned_file,
                    'client_row_id': mapping_data['row_id'],
                    'client_name': client_row.iloc[0]['name'],
                    'method': 'log_mapping',
                    'confidence': mapping_data['confidence'],
                    'evidence': f"Found in {mapping_data['source_file']}"
                })
    
    # Method 2: Content-based correlations
    for uuid, clue_data in content_clues.items():
        file_info = clue_data['file_info']
        metadata = clue_data['metadata']
        
        best_matches = []
        
        for _, client_row in df.iterrows():
            match_score = 0
            evidence = []
            
            client_name = str(client_row['name']).lower()
            client_email = str(client_row.get('email', '')).lower()
            
            # Name matching
            if 'filtered_names' in clue_data:
                for found_name in clue_data['filtered_names']:
                    name_lower = found_name.lower()
                    name_parts = client_name.split()
                    
                    name_matches = sum(1 for part in name_parts 
                                     if len(part) > 2 and part in name_lower)
                    
                    if name_matches >= 2:  # Both first and last name
                        match_score += 0.8
                        evidence.append(f"full_name_match:{found_name}")
                    elif name_matches == 1:  # Partial name match
                        match_score += 0.4
                        evidence.append(f"partial_name_match:{found_name}")
            
            # Email matching
            if 'emails' in metadata:
                for found_email in metadata['emails']:
                    if found_email.lower() == client_email:
                        match_score += 0.9
                        evidence.append(f"exact_email_match:{found_email}")
                    elif client_email and found_email.lower().startswith(client_email.split('@')[0]):
                        match_score += 0.6
                        evidence.append(f"email_username_match:{found_email}")
            
            # File size correlation (similar sized files for same client)
            existing_file_uuids = client_row.get('file_uuids', '{}')
            if existing_file_uuids != '{}':
                try:
                    file_uuids = json.loads(str(existing_file_uuids))
                    # This would need S3 size checking - simplified for now
                    if len(file_uuids) > 0:
                        match_score += 0.1  # Small boost for clients with existing files
                        evidence.append("has_existing_files")
                except:
                    pass
            
            if match_score >= 0.5:  # Meaningful correlation
                best_matches.append({
                    'client_row_id': client_row['row_id'],
                    'client_name': client_row['name'],
                    'match_score': match_score,
                    'evidence': evidence
                })
        
        # Add best match as potential mapping
        if best_matches:
            best_match = max(best_matches, key=lambda x: x['match_score'])
            potential_mappings.append({
                'uuid': uuid,
                'file_info': file_info,
                'client_row_id': best_match['client_row_id'],
                'client_name': best_match['client_name'],
                'method': 'content_correlation',
                'confidence': best_match['match_score'],
                'evidence': ', '.join(best_match['evidence'])
            })
    
    # Method 3: Timestamp correlation
    print("🔍 Analyzing timestamp patterns...")
    
    # Group files by upload date
    date_groups = {}
    for file_info in orphaned_files:
        date_key = file_info['last_modified'].date()
        if date_key not in date_groups:
            date_groups[date_key] = []
        date_groups[date_key].append(file_info)
    
    # Look for dates with multiple files (batch uploads)
    for date, files in date_groups.items():
        if len(files) >= 3:  # Batch upload likely
            print(f"   Found batch upload on {date}: {len(files)} files")
            
            # Try to correlate with client activity
            # This would need client onboarding date data - placeholder for now
            for file_info in files:
                if file_info['uuid'] not in [m['uuid'] for m in potential_mappings]:
                    # Add low-confidence batch correlation
                    potential_mappings.append({
                        'uuid': file_info['uuid'],
                        'file_info': file_info,
                        'client_row_id': None,
                        'client_name': f"Batch upload {date}",
                        'method': 'timestamp_batch',
                        'confidence': 0.3,
                        'evidence': f"Part of {len(files)}-file batch on {date}"
                    })
    
    return sorted(potential_mappings, key=lambda x: x['confidence'], reverse=True)

def execute_high_confidence_mappings(potential_mappings, confidence_threshold=0.7):
    """Execute mappings above confidence threshold"""
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    df = pd.read_csv('outputs/output.csv')
    
    high_confidence = [m for m in potential_mappings if m['confidence'] >= confidence_threshold and m['client_row_id']]
    
    print(f"🎯 Executing {len(high_confidence)} high-confidence mappings...")
    
    successful_recoveries = []
    
    for mapping in high_confidence:
        try:
            uuid = mapping['uuid']
            client_row_id = mapping['client_row_id']
            file_info = mapping['file_info']
            
            print(f"   📦 Mapping {uuid} → {mapping['client_name']} (Row {client_row_id})")
            
            # Update CSV with UUID mapping
            client_row = df[df['row_id'] == client_row_id]
            if client_row.empty:
                print(f"     ❌ Client row {client_row_id} not found")
                continue
            
            # Get current UUID data
            current_file_uuids = client_row.iloc[0].get('file_uuids', '{}')
            current_youtube_uuids = client_row.iloc[0].get('youtube_uuids', '[]')
            current_drive_uuids = client_row.iloc[0].get('drive_uuids', '[]')
            current_s3_paths = client_row.iloc[0].get('s3_paths', '{}')
            
            # Parse current data
            try:
                file_uuids = json.loads(str(current_file_uuids)) if current_file_uuids != '{}' else {}
                youtube_uuids = json.loads(str(current_youtube_uuids)) if current_youtube_uuids != '[]' else []
                drive_uuids = json.loads(str(current_drive_uuids)) if current_drive_uuids != '[]' else []
                s3_paths = json.loads(str(current_s3_paths)) if current_s3_paths != '{}' else {}
            except:
                file_uuids = {}
                youtube_uuids = []
                drive_uuids = []
                s3_paths = {}
            
            # Add new UUID
            filename = file_info['filename']
            file_uuids[filename] = uuid
            s3_paths[uuid] = file_info['s3_key']
            
            # Categorize by file type
            if file_info['extension'] in ['mp3', 'mp4', 'webm', 'm4a']:
                if uuid not in youtube_uuids:
                    youtube_uuids.append(uuid)
            else:
                if uuid not in drive_uuids:
                    drive_uuids.append(uuid)
            
            # Move file to client directory
            old_key = file_info['s3_key']
            new_key = f"clients/{client_row_id}/{filename}"
            
            # Copy to new location
            s3.copy_object(
                CopySource={'Bucket': bucket, 'Key': old_key},
                Bucket=bucket,
                Key=new_key
            )
            
            # Delete from old location
            s3.delete_object(Bucket=bucket, Key=old_key)
            
            # Update S3 path
            s3_paths[uuid] = new_key
            
            # Update CSV
            df.loc[df['row_id'] == client_row_id, 'file_uuids'] = json.dumps(file_uuids)
            df.loc[df['row_id'] == client_row_id, 'youtube_uuids'] = json.dumps(youtube_uuids)
            df.loc[df['row_id'] == client_row_id, 'drive_uuids'] = json.dumps(drive_uuids)
            df.loc[df['row_id'] == client_row_id, 's3_paths'] = json.dumps(s3_paths)
            
            successful_recoveries.append({
                'uuid': uuid,
                'filename': filename,
                'client_name': mapping['client_name'],
                'client_row_id': client_row_id,
                'size_mb': file_info['size_mb'],
                'method': mapping['method'],
                'confidence': mapping['confidence']
            })
            
            print(f"     ✅ Successfully mapped and moved")
            
        except Exception as e:
            print(f"     ❌ Error mapping {uuid}: {str(e)}")
            continue
    
    # Save updated CSV
    if successful_recoveries:
        df.to_csv('outputs/output.csv', index=False)
        print(f"\n🎉 Successfully recovered {len(successful_recoveries)} files!")
        
        total_size = sum(r['size_mb'] for r in successful_recoveries)
        print(f"   Total size recovered: {total_size:.1f} MB")
        
        for recovery in successful_recoveries:
            print(f"   - {recovery['filename']} → {recovery['client_name']} ({recovery['confidence']:.1%} confidence)")
    
    return successful_recoveries

def complete_orphan_recovery():
    """Main function to recover all remaining orphaned files"""
    tracker = get_tracker()
    
    print("🚀 COMPLETE ORPHANED FILE RECOVERY")
    print("=" * 60)
    
    # Step 1: Get current orphaned files
    print("\n1️⃣ ANALYZING CURRENT ORPHANED FILES")
    orphaned_files = get_current_orphaned_files()
    print(f"   Found {len(orphaned_files)} orphaned files")
    print(f"   Total size: {sum(f['size_mb'] for f in orphaned_files):.1f} MB")
    
    # Step 2: Search expanded logs
    print("\n2️⃣ SEARCHING EXPANDED LOG FILES")
    log_mappings = search_expanded_logs()
    
    # Step 3: Deep content analysis
    print("\n3️⃣ DEEP MEDIA CONTENT ANALYSIS")
    content_clues = analyze_media_content_deep(orphaned_files, sample_size=30)
    
    # Step 4: Load client data and correlate
    print("\n4️⃣ CORRELATING WITH CLIENT DATA")
    df = pd.read_csv('outputs/output.csv')
    potential_mappings = correlate_with_client_data(orphaned_files, log_mappings, content_clues, df)
    
    print(f"   Generated {len(potential_mappings)} potential mappings")
    
    # Step 5: Execute high-confidence mappings
    print("\n5️⃣ EXECUTING HIGH-CONFIDENCE MAPPINGS")
    successful_recoveries = execute_high_confidence_mappings(potential_mappings, confidence_threshold=0.7)
    
    # Step 6: Report results
    print("\n6️⃣ RECOVERY SUMMARY")
    remaining_orphaned = len(orphaned_files) - len(successful_recoveries)
    
    print(f"   Files processed: {len(orphaned_files)}")
    print(f"   Files recovered: {len(successful_recoveries)}")
    print(f"   Files remaining: {remaining_orphaned}")
    print(f"   Recovery rate: {len(successful_recoveries)/len(orphaned_files)*100:.1f}%")
    
    if len(successful_recoveries) > 0:
        total_recovered_mb = sum(r['size_mb'] for r in successful_recoveries)
        print(f"   Data recovered: {total_recovered_mb:.1f} MB")
    
    # Step 7: Save session data
    tracker.session_data['complete_recovery'] = {
        'orphaned_files_processed': len(orphaned_files),
        'successful_recoveries': successful_recoveries,
        'remaining_orphaned': remaining_orphaned,
        'potential_mappings_generated': len(potential_mappings)
    }
    
    return {
        'successful_recoveries': successful_recoveries,
        'remaining_orphaned': remaining_orphaned,
        'potential_mappings': potential_mappings
    }

if __name__ == "__main__":
    results = complete_orphan_recovery()
    
    if results['successful_recoveries']:
        print(f"\n✅ RECOVERY SUCCESSFUL!")
        print(f"   {len(results['successful_recoveries'])} files mapped to correct clients")
    else:
        print(f"\n⚠️  No additional high-confidence mappings found")
        print(f"   {results['remaining_orphaned']} files still require investigation")