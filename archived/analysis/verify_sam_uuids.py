#!/usr/bin/env python3
"""Verify Sam's UUIDs exist in S3 orphaned files"""

import boto3
import json
from orphaned_file_recovery_tracker import get_tracker

def verify_sam_uuids():
    """Verify that Sam's UUIDs exist in S3 orphaned files directory"""
    tracker = get_tracker()
    
    try:
        # Load Sam's mappings from previous step
        with open('sam_torode_pipeline_complete.json', 'r') as f:
            pipeline_data = json.load(f)
        
        uploads = pipeline_data.get('uploads', [])
        
        # Initialize S3 client
        s3 = boto3.client('s3')
        bucket = 'typing-clients-uuid-system'
        
        tracker.log("INFO", "Starting UUID verification in S3", {
            'bucket': bucket,
            'uuids_to_verify': len(uploads)
        })
        
        verified_mappings = []
        missing_uuids = []
        
        # Check each UUID
        for upload in uploads:
            file_uuid = upload.get('file_uuid')
            original_filename = upload.get('original_filename')
            
            if not file_uuid:
                continue
                
            # Check different possible file extensions for this UUID
            possible_extensions = ['.mp3', '.mp4', '.m4a', '.webm', '.json', '']
            found_file = None
            
            for ext in possible_extensions:
                s3_key = f"files/{file_uuid}{ext}"
                
                try:
                    # Check if object exists
                    response = s3.head_object(Bucket=bucket, Key=s3_key)
                    found_file = {
                        's3_key': s3_key,
                        'size': response['ContentLength'],
                        'last_modified': response['LastModified'],
                        'extension': ext
                    }
                    break
                except s3.exceptions.NoSuchKey:
                    continue
                except Exception as e:
                    tracker.log("WARNING", f"Error checking {s3_key}", {'error': str(e)})
            
            if found_file:
                verified_mapping = {
                    'file_uuid': file_uuid,
                    'original_filename': original_filename,
                    'file_type': upload.get('file_type'),
                    's3_key': found_file['s3_key'],
                    'size': found_file['size'],
                    'last_modified': found_file['last_modified'].isoformat(),
                    'extension': found_file['extension'],
                    'client_id': 502,
                    'verification_status': 'found'
                }
                verified_mappings.append(verified_mapping)
                
                tracker.log("SUCCESS", f"UUID verified in S3", {
                    'uuid': file_uuid,
                    'filename': original_filename,
                    's3_key': found_file['s3_key'],
                    'size': found_file['size']
                })
                
                print(f"✅ {original_filename}")
                print(f"   UUID: {file_uuid}")
                print(f"   S3 Key: {found_file['s3_key']}")
                print(f"   Size: {found_file['size']:,} bytes")
                
            else:
                missing_uuid = {
                    'file_uuid': file_uuid,
                    'original_filename': original_filename,
                    'verification_status': 'missing'
                }
                missing_uuids.append(missing_uuid)
                
                tracker.add_error('sam_pipeline', {
                    'error_type': 'uuid_not_found',
                    'error_message': f'UUID not found in S3: {file_uuid}',
                    'context': {
                        'uuid': file_uuid,
                        'filename': original_filename
                    }
                })
                
                print(f"❌ {original_filename}")
                print(f"   UUID: {file_uuid} - NOT FOUND in S3")
        
        # Store results
        tracker.session_data['sam_verified_mappings'] = verified_mappings
        tracker.session_data['sam_missing_uuids'] = missing_uuids
        
        # Summary
        total_uuids = len(uploads)
        verified_count = len(verified_mappings)
        missing_count = len(missing_uuids)
        
        tracker.log("INFO", "UUID verification completed", {
            'total_uuids': total_uuids,
            'verified': verified_count,
            'missing': missing_count,
            'success_rate': f"{(verified_count/total_uuids)*100:.1f}%"
        })
        
        print(f"\n📊 VERIFICATION RESULTS:")
        print(f"   Total UUIDs: {total_uuids}")
        print(f"   Verified: {verified_count}")
        print(f"   Missing: {missing_count}")
        print(f"   Success Rate: {(verified_count/total_uuids)*100:.1f}%")
        
        if missing_count > 0:
            print(f"\n⚠️  MISSING UUIDS:")
            for missing in missing_uuids:
                print(f"   - {missing['original_filename']} ({missing['file_uuid']})")
        
        return verified_mappings
        
    except Exception as e:
        tracker.add_error('sam_pipeline', {
            'error_type': 'verification_error',
            'error_message': str(e),
            'context': {'operation': 'uuid_verification'}
        })
        raise

if __name__ == "__main__":
    print("🔍 VERIFYING SAM'S UUIDS IN S3...")
    print("=" * 50)
    
    verified = verify_sam_uuids()
    
    if verified:
        print(f"\n✅ Verification complete: {len(verified)} UUID mappings ready for CSV import")
    else:
        print(f"\n❌ No UUIDs verified - check errors")