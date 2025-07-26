#!/usr/bin/env python3
"""Parse Sam Torode pipeline data and extract UUID mappings"""

import json
from orphaned_file_recovery_tracker import get_tracker

def parse_sam_pipeline_data():
    """Parse Sam's pipeline data and extract UUID mappings"""
    tracker = get_tracker()
    tracker.start_phase('sam_pipeline')
    
    try:
        # Load pipeline data
        with open('sam_torode_pipeline_complete.json', 'r') as f:
            pipeline_data = json.load(f)
        
        tracker.log("INFO", "Loaded Sam's pipeline data", {
            'keys': list(pipeline_data.keys()),
            'uploads_count': len(pipeline_data.get('uploads', []))
        })
        
        # Extract mappings
        sam_mappings = []
        uploads = pipeline_data.get('uploads', [])
        
        for upload in uploads:
            mapping = {
                'file_uuid': upload.get('file_uuid'),
                'original_filename': upload.get('original_filename'),
                'file_type': upload.get('file_type'),
                's3_key': upload.get('s3_key'),
                'size': upload.get('size'),
                'upload_timestamp': upload.get('upload_timestamp'),
                'client_id': 502  # Sam Torode's row ID
            }
            
            # Validate required fields
            if mapping['file_uuid'] and mapping['original_filename']:
                sam_mappings.append(mapping)
                tracker.log("DEBUG", "Extracted mapping", mapping)
            else:
                tracker.add_error('sam_pipeline', {
                    'error_type': 'missing_required_fields',
                    'error_message': 'Missing UUID or filename',
                    'context': upload
                })
        
        # Save mappings to session
        tracker.session_data['sam_pipeline_mappings'] = sam_mappings
        
        tracker.log("SUCCESS", f"Parsed Sam's pipeline data", {
            'total_uploads': len(uploads),
            'valid_mappings': len(sam_mappings),
            'invalid_mappings': len(uploads) - len(sam_mappings)
        })
        
        # Display results
        print(f"\n📊 SAM'S PIPELINE DATA PARSED:")
        print(f"   Total uploads: {len(uploads)}")
        print(f"   Valid mappings: {len(sam_mappings)}")
        print(f"\n📋 UUID Mappings:")
        
        for i, mapping in enumerate(sam_mappings, 1):
            print(f"   {i}. {mapping['original_filename']}")
            print(f"      UUID: {mapping['file_uuid']}")
            print(f"      Type: {mapping['file_type']}")
            print(f"      Size: {mapping.get('size', 'Unknown')}")
        
        return sam_mappings
        
    except Exception as e:
        tracker.add_error('sam_pipeline', {
            'error_type': 'parsing_error',
            'error_message': str(e),
            'context': {'file': 'sam_torode_pipeline_complete.json'}
        })
        raise

if __name__ == "__main__":
    mappings = parse_sam_pipeline_data()
    print(f"\n✅ Extracted {len(mappings)} UUID mappings from Sam's pipeline data")