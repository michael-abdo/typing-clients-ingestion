#!/usr/bin/env python3
"""Targeted correlation of specific IDs with client data"""

import boto3
import pandas as pd
import json
import re
from orphaned_file_recovery_tracker import get_tracker

def investigate_dan_jane_specifically():
    """Deep investigation of the Dan Jane JSON file for exact client mapping"""
    print("🎯 INVESTIGATING DAN JANE MAPPING")
    print("=" * 40)
    
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    
    # Get the Dan Jane JSON file content
    uuid = '774cc87d-1252-4d45-80d6-3580ee37e159'
    
    try:
        response = s3.get_object(Bucket=bucket, Key=f"files/{uuid}.json")
        content = response['Body'].read().decode('utf-8')
        data = json.loads(content)
        
        print(f"📄 Analyzing Dan Jane JSON file: {uuid}")
        print(f"   Content preview: {str(data)[:200]}...")
        
        # Load client data
        df = pd.read_csv('outputs/output.csv')
        
        # Search for Dan Jane variations more carefully
        print(f"\n🔍 Searching for Dan Jane variations in CSV...")
        
        dan_jane_candidates = []
        
        # Comprehensive name search
        name_variations = [
            'dan jane', 'jane dan', 'daniel jane', 'danielle jane', 
            'dan j', 'jane d', 'dj', 'danjane', 'jane_dan',
            'dan', 'jane'  # Individual names as last resort
        ]
        
        for _, row in df.iterrows():
            name = str(row['name']).lower()
            email = str(row.get('email', '')).lower()
            
            match_score = 0
            matched_variations = []
            
            for variation in name_variations:
                if variation in name:
                    if variation in ['dan jane', 'jane dan', 'daniel jane', 'danielle jane']:
                        match_score += 10  # High score for full name matches
                    elif variation in ['dan j', 'jane d', 'danjane', 'jane_dan']:
                        match_score += 8   # Medium-high for likely matches
                    elif variation in ['dan', 'jane']:
                        match_score += 2   # Low score for partial matches
                    
                    matched_variations.append(variation)
                
                # Also check email
                if variation in email:
                    match_score += 5
                    matched_variations.append(f"email:{variation}")
            
            if match_score > 0:
                dan_jane_candidates.append({
                    'row_id': row['row_id'],
                    'name': row['name'],
                    'email': row.get('email', ''),
                    'match_score': match_score,
                    'matched_variations': matched_variations
                })
        
        # Sort by match score
        dan_jane_candidates = sorted(dan_jane_candidates, key=lambda x: x['match_score'], reverse=True)
        
        print(f"   Found {len(dan_jane_candidates)} potential Dan Jane matches:")
        for i, candidate in enumerate(dan_jane_candidates[:10], 1):
            print(f"   {i}. Row {candidate['row_id']}: {candidate['name']} (Score: {candidate['match_score']})")
            print(f"      Email: {candidate['email']}")
            print(f"      Matches: {', '.join(candidate['matched_variations'])}")
        
        # Extract Drive ID from JSON
        drive_id = None
        def extract_drive_id(obj):
            nonlocal drive_id
            if isinstance(obj, dict):
                for key, value in obj.items():
                    if isinstance(value, str) and 'drive.google.com' in value:
                        match = re.search(r'/([a-zA-Z0-9_-]{25,})', value)
                        if match:
                            drive_id = match.group(1)
                            return drive_id
                    elif isinstance(value, (dict, list)):
                        result = extract_drive_id(value)
                        if result:
                            return result
            elif isinstance(obj, list):
                for item in obj:
                    result = extract_drive_id(item)
                    if result:
                        return result
            return None
        
        extract_drive_id(data)
        
        if drive_id:
            print(f"\n🔗 Found Drive ID in JSON: {drive_id}")
            
            # Check if this Drive ID appears in any client's data
            for candidate in dan_jane_candidates:
                client_row = df[df['row_id'] == candidate['row_id']].iloc[0]
                drive_links = str(client_row.get('drive_links', ''))
                doc_link = str(client_row.get('doc_link', ''))
                
                if drive_id in drive_links or drive_id in doc_link:
                    print(f"   ✅ EXACT MATCH: Drive ID found in {candidate['name']} (Row {candidate['row_id']})!")
                    return {
                        'uuid': uuid,
                        'client_row_id': candidate['row_id'],
                        'client_name': candidate['name'],
                        'confidence': 0.95,
                        'method': 'drive_id_exact_match',
                        'evidence': f"Drive ID {drive_id} matches client data"
                    }
        
        # If no exact Drive ID match, return best name match if confidence is high enough
        if dan_jane_candidates and dan_jane_candidates[0]['match_score'] >= 8:
            best_candidate = dan_jane_candidates[0]
            return {
                'uuid': uuid,
                'client_row_id': best_candidate['row_id'],
                'client_name': best_candidate['name'],
                'confidence': min(0.8, best_candidate['match_score'] / 10),
                'method': 'dan_jane_name_match',
                'evidence': f"Name variations: {', '.join(best_candidate['matched_variations'])}"
            }
        
        print(f"   ❌ No high-confidence Dan Jane match found")
        return None
        
    except Exception as e:
        print(f"   ❌ Error investigating Dan Jane: {str(e)}")
        return None

def correlate_youtube_ids_with_clients():
    """Cross-reference extracted YouTube IDs with client YouTube links"""
    print(f"\n🎬 CORRELATING YOUTUBE IDs WITH CLIENTS")
    print("=" * 40)
    
    # YouTube IDs found in large media files
    youtube_id_mappings = {
        'fa7fa69d-79c6-4eac-9b8a-570edbceb431': ['SvIkfz1KnhO', '_cO09w5Ni3A'],
        '0115f162-17c4-4392-bedb-870a1679da56': ['Q2wHkXC3BCr', 'Pe9X_Ny9a9v'],
        '274a60ab-5acf-491b-94a8-3346ba1f2000': ['Hal_xVkxSc4', 'm9yu81Svh-B', 'XhEDpAhz8AQ'],
        '1dad364a-0cc3-4ea8-8a4e-fbc5a2867fe9': ['BBU7Pxs9oyW', 'BLrasaXIYMo'],
        '006cecbd-6a7f-4e28-9dee-6a1a2ce13cad': ['_a-T0i5UjiN', 'labPlI5EIrJ', 'Zq9QunQgIUn'],
        '94e58533-dc61-4594-b463-438fa7d7e6f8': ['rlsgHQiE0q5'],
        '1f8274c6-4dc8-4681-a81a-03cef3b89aea': ['nJklQAK_U92', 'iU0LuOZiWz_', 'qWCxmEwK8Qu'],
        'ce9c48cb-080e-49ee-a9fd-1fafb9ee3b06': ['1HD6uuuDk-k', '9oNTXPzskJr', 'bAJgkEJq6jN'],
        'e9ee93b0-2169-4c35-854c-3f62130609af': ['5rZv_jE2VxM'],
        '24b1278b-bd3b-4339-b876-b759d60bfeda': ['PW5EcP6E9RI', 'TEMIQZ5pGEF', '100UUUUUD2X']
    }
    
    df = pd.read_csv('outputs/output.csv')
    
    youtube_matches = []
    
    print(f"   Checking {sum(len(ids) for ids in youtube_id_mappings.values())} YouTube IDs...")
    
    for uuid, youtube_ids in youtube_id_mappings.items():
        print(f"\n   🔍 Checking {uuid}:")
        
        for youtube_id in youtube_ids:
            print(f"     Searching for YouTube ID: {youtube_id}")
            
            # Search through all client YouTube links
            for _, client_row in df.iterrows():
                youtube_links = str(client_row.get('youtube_links', ''))
                
                if youtube_id in youtube_links:
                    print(f"     ✅ MATCH FOUND: {youtube_id} in {client_row['name']} (Row {client_row['row_id']})")
                    
                    youtube_matches.append({
                        'uuid': uuid,
                        'youtube_id': youtube_id,
                        'client_row_id': client_row['row_id'],
                        'client_name': client_row['name'],
                        'confidence': 0.9,
                        'method': 'youtube_id_exact_match',
                        'evidence': f"YouTube ID {youtube_id} matches client links"
                    })
                    break
            else:
                print(f"     ❌ No match found for {youtube_id}")
    
    print(f"\n📊 YouTube ID Correlation Results:")
    print(f"   YouTube matches found: {len(youtube_matches)}")
    
    return youtube_matches

def check_drive_id_exact_matches():
    """Check for exact Drive ID matches in client data"""
    print(f"\n💾 CHECKING DRIVE ID EXACT MATCHES")
    print("=" * 40)
    
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    df = pd.read_csv('outputs/output.csv')
    
    # JSON files with Drive IDs from previous analysis
    json_files_with_drive_ids = [
        '0d2b0e2b-2a12-4b91-ac7e-1f9a8e1d25eb',
        '5eebcbf6-6654-4744-a075-b1d6d402801a',
        '774cc87d-1252-4d45-80d6-3580ee37e159',  # Dan Jane - already processed
        '882d41b0-21d7-40ed-a8f1-10b3464fc977',
        'df0da9ee-da7c-4e18-b871-c0d05fbec66d'
    ]
    
    drive_matches = []
    
    for uuid in json_files_with_drive_ids:
        if uuid == '774cc87d-1252-4d45-80d6-3580ee37e159':
            continue  # Already processed Dan Jane
            
        try:
            print(f"\n   🔍 Analyzing {uuid}.json for Drive IDs...")
            
            response = s3.get_object(Bucket=bucket, Key=f"files/{uuid}.json")
            content = response['Body'].read().decode('utf-8')
            data = json.loads(content)
            
            # Extract all Drive IDs
            drive_ids = []
            
            def extract_drive_ids(obj):
                if isinstance(obj, dict):
                    for key, value in obj.items():
                        if isinstance(value, str):
                            # Multiple Drive ID patterns
                            patterns = [
                                r'drive\.google\.com/file/d/([a-zA-Z0-9_-]+)',
                                r'docs\.google\.com/.*/d/([a-zA-Z0-9_-]+)',
                                r'/([a-zA-Z0-9_-]{33})[/?]',
                                r'/([a-zA-Z0-9_-]{28})[/?]',
                                r'/([a-zA-Z0-9_-]{25})[/?]'
                            ]
                            
                            for pattern in patterns:
                                matches = re.findall(pattern, value)
                                drive_ids.extend(matches)
                        
                        elif isinstance(value, (dict, list)):
                            extract_drive_ids(value)
                
                elif isinstance(obj, list):
                    for item in obj:
                        extract_drive_ids(item)
            
            extract_drive_ids(data)
            drive_ids = list(set(drive_ids))  # Remove duplicates
            
            print(f"     Found {len(drive_ids)} Drive IDs: {drive_ids[:3]}...")
            
            # Check each Drive ID against client data
            for drive_id in drive_ids:
                if len(drive_id) < 20:  # Skip very short IDs (likely false positives)
                    continue
                    
                for _, client_row in df.iterrows():
                    drive_links = str(client_row.get('drive_links', ''))
                    doc_link = str(client_row.get('doc_link', ''))
                    
                    if drive_id in drive_links or drive_id in doc_link:
                        print(f"     ✅ EXACT MATCH: {drive_id} → {client_row['name']} (Row {client_row['row_id']})")
                        
                        drive_matches.append({
                            'uuid': uuid,
                            'drive_id': drive_id,
                            'client_row_id': client_row['row_id'],
                            'client_name': client_row['name'],
                            'confidence': 0.9,
                            'method': 'drive_id_exact_match',
                            'evidence': f"Drive ID {drive_id} matches client data"
                        })
                        break
        
        except Exception as e:
            print(f"     ❌ Error analyzing {uuid}: {str(e)}")
            continue
    
    print(f"\n📊 Drive ID Correlation Results:")
    print(f"   Drive matches found: {len(drive_matches)}")
    
    return drive_matches

def execute_confirmed_mappings(all_mappings):
    """Execute all confirmed high-confidence mappings"""
    print(f"\n🚀 EXECUTING CONFIRMED MAPPINGS")
    print("=" * 40)
    
    if not all_mappings:
        print("   No confirmed mappings to execute")
        return []
    
    s3 = boto3.client('s3')
    bucket = 'typing-clients-uuid-system'
    df = pd.read_csv('outputs/output.csv')
    
    successful_recoveries = []
    
    for mapping in all_mappings:
        uuid = mapping['uuid']
        client_row_id = mapping['client_row_id']
        client_name = mapping['client_name']
        
        try:
            print(f"\n📦 Executing: {uuid} → {client_name} (Row {client_row_id})")
            print(f"   Method: {mapping['method']}, Confidence: {mapping['confidence']:.1%}")
            
            # Find all files with this UUID (different extensions)
            response = s3.list_objects_v2(Bucket=bucket, Prefix=f"files/{uuid}")
            files_to_move = []
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    if not obj['Key'].endswith('/'):
                        filename = obj['Key'].split('/')[-1]
                        files_to_move.append({
                            'old_key': obj['Key'],
                            'filename': filename,
                            'size_mb': obj['Size'] / (1024 * 1024)
                        })
            
            if not files_to_move:
                print(f"   ❌ No files found for UUID {uuid}")
                continue
            
            print(f"   Found {len(files_to_move)} files to move:")
            for file_info in files_to_move:
                print(f"     - {file_info['filename']} ({file_info['size_mb']:.1f} MB)")
            
            # Update CSV with UUID mappings
            client_row = df[df['row_id'] == client_row_id]
            if client_row.empty:
                print(f"   ❌ Client row {client_row_id} not found in CSV")
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
            
            # Move files and update mappings
            moved_files = []
            for file_info in files_to_move:
                old_key = file_info['old_key']
                filename = file_info['filename']
                new_key = f"clients/{client_row_id}/{filename}"
                
                try:
                    # Copy to new location
                    s3.copy_object(
                        CopySource={'Bucket': bucket, 'Key': old_key},
                        Bucket=bucket,
                        Key=new_key
                    )
                    
                    # Delete from old location
                    s3.delete_object(Bucket=bucket, Key=old_key)
                    
                    # Update UUID mappings
                    file_uuids[filename] = uuid
                    s3_paths[uuid] = new_key
                    
                    # Categorize by file type
                    if any(filename.endswith(ext) for ext in ['.mp3', '.mp4', '.webm', '.m4a']):
                        if uuid not in youtube_uuids:
                            youtube_uuids.append(uuid)
                    else:
                        if uuid not in drive_uuids:
                            drive_uuids.append(uuid)
                    
                    moved_files.append(file_info)
                    print(f"     ✅ Moved {filename}")
                    
                except Exception as e:
                    print(f"     ❌ Failed to move {filename}: {str(e)}")
                    continue
            
            if moved_files:
                # Update CSV
                df.loc[df['row_id'] == client_row_id, 'file_uuids'] = json.dumps(file_uuids)
                df.loc[df['row_id'] == client_row_id, 'youtube_uuids'] = json.dumps(youtube_uuids)
                df.loc[df['row_id'] == client_row_id, 'drive_uuids'] = json.dumps(drive_uuids)
                df.loc[df['row_id'] == client_row_id, 's3_paths'] = json.dumps(s3_paths)
                
                successful_recoveries.append({
                    'uuid': uuid,
                    'client_name': client_name,
                    'client_row_id': client_row_id,
                    'files_moved': len(moved_files),
                    'total_size_mb': sum(f['size_mb'] for f in moved_files),
                    'method': mapping['method'],
                    'confidence': mapping['confidence']
                })
                
                print(f"   🎉 Successfully recovered {len(moved_files)} files!")
        
        except Exception as e:
            print(f"   ❌ Error executing mapping for {uuid}: {str(e)}")
            continue
    
    # Save updated CSV
    if successful_recoveries:
        df.to_csv('outputs/output.csv', index=False)
        
        print(f"\n🎊 EXECUTION COMPLETE!")
        print(f"   Total recoveries: {len(successful_recoveries)}")
        print(f"   Total files moved: {sum(r['files_moved'] for r in successful_recoveries)}")
        total_size = sum(r['total_size_mb'] for r in successful_recoveries)
        print(f"   Total size recovered: {total_size:.1f} MB")
    
    return successful_recoveries

def main():
    """Main targeted correlation function"""
    print("🎯 TARGETED ID CORRELATION ANALYSIS")
    print("=" * 50)
    
    all_mappings = []
    
    # Step 1: Investigate Dan Jane specifically
    dan_jane_mapping = investigate_dan_jane_specifically()
    if dan_jane_mapping:
        all_mappings.append(dan_jane_mapping)
    
    # Step 2: Correlate YouTube IDs
    youtube_matches = correlate_youtube_ids_with_clients()
    all_mappings.extend(youtube_matches)
    
    # Step 3: Check Drive ID exact matches
    drive_matches = check_drive_id_exact_matches()
    all_mappings.extend(drive_matches)
    
    # Step 4: Execute confirmed mappings
    successful_recoveries = execute_confirmed_mappings(all_mappings)
    
    return {
        'all_mappings': all_mappings,
        'successful_recoveries': successful_recoveries
    }

if __name__ == "__main__":
    results = main()
    
    if results['successful_recoveries']:
        print(f"\n✅ SUCCESS: Recovered files for {len(set(r['client_name'] for r in results['successful_recoveries']))} more clients!")
    else:
        print(f"\n📊 No additional high-confidence mappings found")