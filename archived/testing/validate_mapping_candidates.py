#!/usr/bin/env python3
"""Validate and refine the 176 mapping candidates with improved confidence scoring"""

import boto3
import pandas as pd
import json
import re
from orphaned_file_recovery_tracker import get_tracker
from advanced_orphan_recovery import analyze_file_content_for_clues, search_for_historical_references

def validate_and_refine_candidates():
    """Validate and improve confidence scoring for mapping candidates"""
    tracker = get_tracker()
    
    print("🔍 Loading previous analysis data...")
    
    # Re-run analysis to get latest data
    content_clues = analyze_file_content_for_clues()
    reference_findings = search_for_historical_references()
    
    # Load CSV for validation
    df = pd.read_csv('outputs/output.csv')
    
    print(f"📊 Creating refined mapping candidates...")
    
    validated_candidates = []
    
    # Method 1: Validate historical reference mappings with improved scoring
    for file_path, ref_data in reference_findings.items():
        
        # Only trust certain log files for mappings
        trusted_logs = ['logs/runs/2025-07-13_223356/main.log']
        
        if file_path in trusted_logs:
            # These are high confidence - we already validated Kaioxys
            confidence_multiplier = 1.0
        else:
            # Lower confidence for other references
            confidence_multiplier = 0.3
        
        for uuid in ref_data['found_uuids']:
            # Check if this UUID still exists in orphaned files
            s3 = boto3.client('s3')
            bucket = 'typing-clients-uuid-system'
            
            try:
                s3.head_object(Bucket=bucket, Key=f"files/{uuid}.mp3")
                file_exists = True
                file_ext = "mp3"
            except:
                try:
                    s3.head_object(Bucket=bucket, Key=f"files/{uuid}.mp4")
                    file_exists = True
                    file_ext = "mp4"
                except:
                    try:
                        s3.head_object(Bucket=bucket, Key=f"files/{uuid}.webm")
                        file_exists = True
                        file_ext = "webm"
                    except:
                        try:
                            s3.head_object(Bucket=bucket, Key=f"files/{uuid}.m4a")
                            file_exists = True
                            file_ext = "m4a"
                        except:
                            try:
                                s3.head_object(Bucket=bucket, Key=f"files/{uuid}.json")
                                file_exists = True
                                file_ext = "json"
                            except:
                                try:
                                    s3.head_object(Bucket=bucket, Key=f"files/{uuid}.part")
                                    file_exists = True
                                    file_ext = "part"
                                except:
                                    file_exists = False
                                    file_ext = None
            
            if not file_exists:
                continue  # Skip if file doesn't exist in orphaned files
            
            # Extract client context from file path
            if 'logs/runs/2025-07-13_223356/main.log' in file_path:
                # We know this maps to Kaioxys from the log content
                client_row = df[df['row_id'] == 472]
                if not client_row.empty:
                    validated_candidates.append({
                        'uuid': uuid,
                        'file_extension': file_ext,
                        'client_row_id': 472,
                        'client_name': client_row.iloc[0]['name'],
                        'method': 'validated_log_reference',
                        'confidence': 0.95,  # Very high confidence
                        'evidence': {
                            'source_log': file_path,
                            'validation': 'log_content_confirmed_client'
                        }
                    })
    
    # Method 2: Content-based validation with improved scoring
    for clue in content_clues:
        if not clue['clues']:
            continue
            
        # Check if this UUID still exists in orphaned files
        s3 = boto3.client('s3')
        bucket = 'typing-clients-uuid-system'
        
        uuid = clue['uuid']
        try:
            # Check various file extensions
            file_exists = False
            file_ext = None
            for ext in ['mp3', 'mp4', 'webm', 'm4a', 'json', 'part']:
                try:
                    s3.head_object(Bucket=bucket, Key=f"files/{uuid}.{ext}")
                    file_exists = True
                    file_ext = ext
                    break
                except:
                    continue
        except:
            continue
        
        if not file_exists:
            continue
        
        # Extract meaningful content for mapping
        meaningful_clues = {}
        
        for key, value in clue['clues'].items():
            if key in ['title', 'webpage_url', 'url', 'id', 'uploader', 'channel']:
                if isinstance(value, str) and len(value) > 3:
                    meaningful_clues[key] = value
        
        if not meaningful_clues:
            continue
        
        # Try to match content to clients
        best_matches = []
        
        for _, client_row in df.iterrows():
            match_score = 0
            match_evidence = []
            
            client_name = str(client_row['name']).lower()
            client_email = str(client_row.get('email', '')).lower()
            client_doc_link = str(client_row.get('doc_link', ''))
            
            # Check for name matches in content
            for clue_key, clue_value in meaningful_clues.items():
                clue_lower = str(clue_value).lower()
                
                # Name matching
                name_parts = client_name.split()
                for name_part in name_parts:
                    if len(name_part) > 2 and name_part in clue_lower:
                        match_score += 0.3
                        match_evidence.append(f"name_match_{name_part}")
                
                # Email matching
                if client_email and '@' in clue_lower and client_email.split('@')[0] in clue_lower:
                    match_score += 0.4
                    match_evidence.append("email_partial_match")
                
                # YouTube URL matching
                if 'youtube.com' in clue_lower or 'youtu.be' in clue_lower:
                    # Extract video ID from clue
                    youtube_patterns = [
                        r'youtube\.com/watch\?v=([a-zA-Z0-9_-]{11})',
                        r'youtu\.be/([a-zA-Z0-9_-]{11})',
                        r'([a-zA-Z0-9_-]{11})'  # Just the ID
                    ]
                    
                    for pattern in youtube_patterns:
                        matches = re.findall(pattern, clue_lower)
                        if matches:
                            video_id = matches[0]
                            # Check if this video ID appears in client's data
                            client_youtube_links = str(client_row.get('youtube_links', ''))
                            if video_id in client_youtube_links:
                                match_score += 0.8  # Very strong match
                                match_evidence.append(f"youtube_id_match_{video_id}")
            
            if match_score > 0.3:  # Only consider meaningful matches
                best_matches.append({
                    'client_row_id': client_row['row_id'],
                    'client_name': client_row['name'],
                    'match_score': match_score,
                    'evidence': match_evidence
                })
        
        # Add best matches as candidates
        for match in sorted(best_matches, key=lambda x: x['match_score'], reverse=True)[:3]:
            confidence = min(match['match_score'], 0.85)  # Cap at 85% for content-based matching
            
            validated_candidates.append({
                'uuid': uuid,
                'file_extension': file_ext,
                'client_row_id': match['client_row_id'],
                'client_name': match['client_name'],
                'method': 'content_analysis_validation',
                'confidence': confidence,
                'evidence': {
                    'content_clues': meaningful_clues,
                    'match_evidence': match['evidence'],
                    'match_score': match['match_score']
                }
            })
    
    # Remove duplicates and sort by confidence
    unique_candidates = {}
    for candidate in validated_candidates:
        key = f"{candidate['uuid']}_{candidate['client_row_id']}"
        if key not in unique_candidates or candidate['confidence'] > unique_candidates[key]['confidence']:
            unique_candidates[key] = candidate
    
    final_candidates = sorted(unique_candidates.values(), key=lambda x: x['confidence'], reverse=True)
    
    # Filter to only high-confidence candidates
    high_confidence_candidates = [c for c in final_candidates if c['confidence'] >= 0.7]
    medium_confidence_candidates = [c for c in final_candidates if 0.4 <= c['confidence'] < 0.7]
    
    print(f"\n📊 Validation Results:")
    print(f"   Original candidates: 176")
    print(f"   Validated candidates: {len(final_candidates)}")
    print(f"   High confidence (≥70%): {len(high_confidence_candidates)}")
    print(f"   Medium confidence (40-70%): {len(medium_confidence_candidates)}")
    
    if high_confidence_candidates:
        print(f"\n🎯 High Confidence Candidates:")
        for i, candidate in enumerate(high_confidence_candidates[:10], 1):
            print(f"   {i}. UUID {candidate['uuid']} → {candidate['client_name']} (Row {candidate['client_row_id']})")
            print(f"      Method: {candidate['method']}, Confidence: {candidate['confidence']:.1%}")
            if candidate['method'] == 'content_analysis_validation':
                print(f"      Evidence: {', '.join(candidate['evidence']['match_evidence'])}")
    
    tracker.session_data['validated_candidates'] = {
        'high_confidence': high_confidence_candidates,
        'medium_confidence': medium_confidence_candidates,
        'total_validated': len(final_candidates)
    }
    
    return {
        'high_confidence': high_confidence_candidates,
        'medium_confidence': medium_confidence_candidates,
        'all_candidates': final_candidates
    }

if __name__ == "__main__":
    print("🔍 VALIDATING MAPPING CANDIDATES")
    print("=" * 60)
    
    results = validate_and_refine_candidates()
    
    print(f"\n✅ Validation complete!")
    print(f"   High confidence mappings ready for execution: {len(results['high_confidence'])}")
    print(f"   Medium confidence mappings need review: {len(results['medium_confidence'])}")