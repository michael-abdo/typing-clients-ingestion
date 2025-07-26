#!/usr/bin/env python3
"""Upload Sam Torode's files to S3 with UUID naming and update database."""

import os
import uuid
import json
import boto3
from pathlib import Path
from utils.logging_config import get_logger

logger = get_logger(__name__)

# S3 configuration
BUCKET_NAME = 'xenodex-client-output-new'
S3_PREFIX = 'files/'

def upload_files_for_person(person_id=502, person_name="Sam Torode"):
    """Upload files for a person to S3 with UUID naming."""
    
    # Initialize S3 client
    s3_client = boto3.client('s3')
    
    # Directory containing the files
    person_dir = f"downloads/{person_id}_{person_name.replace(' ', '_')}"
    
    if not os.path.exists(person_dir):
        logger.error(f"Directory not found: {person_dir}")
        return False
    
    # Results tracking
    upload_results = {
        'person_id': person_id,
        'person_name': person_name,
        'files': []
    }
    
    # Process each file in the directory
    for filename in os.listdir(person_dir):
        file_path = os.path.join(person_dir, filename)
        
        if not os.path.isfile(file_path):
            continue
        
        # Generate UUID for the file
        file_uuid = str(uuid.uuid4())
        
        # Determine file type and extension
        if filename.startswith('youtube_'):
            file_type = 'youtube'
            extension = Path(filename).suffix
        elif filename.startswith('drive_'):
            file_type = 'drive'
            extension = Path(filename).suffix
        else:
            file_type = 'other'
            extension = Path(filename).suffix
        
        # Create S3 key with UUID
        s3_key = f"{S3_PREFIX}{file_uuid}{extension}"
        
        logger.info(f"Uploading {filename} as {s3_key}")
        
        try:
            # Upload to S3
            s3_client.upload_file(
                file_path,
                BUCKET_NAME,
                s3_key,
                ExtraArgs={
                    'Metadata': {
                        'person_id': str(person_id),
                        'person_name': person_name,
                        'original_filename': filename,
                        'file_type': file_type
                    }
                }
            )
            
            # Verify upload
            s3_client.head_object(Bucket=BUCKET_NAME, Key=s3_key)
            
            logger.info(f"✅ Successfully uploaded {filename}")
            
            # Track results
            upload_results['files'].append({
                'original_filename': filename,
                'file_uuid': file_uuid,
                's3_key': s3_key,
                'file_type': file_type,
                'file_size': os.path.getsize(file_path),
                'success': True
            })
            
        except Exception as e:
            logger.error(f"❌ Failed to upload {filename}: {e}")
            upload_results['files'].append({
                'original_filename': filename,
                'file_uuid': file_uuid,
                's3_key': s3_key,
                'file_type': file_type,
                'error': str(e),
                'success': False
            })
    
    # Save results to JSON
    results_file = f"sam_torode_s3_upload_{person_id}.json"
    with open(results_file, 'w') as f:
        json.dump(upload_results, f, indent=2)
    
    logger.info(f"Upload results saved to {results_file}")
    
    # Summary
    total_files = len(upload_results['files'])
    successful = sum(1 for f in upload_results['files'] if f['success'])
    
    logger.info(f"\n{'='*60}")
    logger.info(f"UPLOAD SUMMARY for {person_name} (ID: {person_id})")
    logger.info(f"{'='*60}")
    logger.info(f"Total files: {total_files}")
    logger.info(f"Successful uploads: {successful}")
    logger.info(f"Failed uploads: {total_files - successful}")
    
    if successful > 0:
        logger.info(f"\nUploaded files:")
        for file_info in upload_results['files']:
            if file_info['success']:
                logger.info(f"  - {file_info['original_filename']} → {file_info['s3_key']}")
    
    return upload_results

if __name__ == "__main__":
    upload_results = upload_files_for_person()
    
    # Print database insert statements
    if any(f['success'] for f in upload_results['files']):
        print("\n" + "="*60)
        print("DATABASE INSERT STATEMENTS")
        print("="*60)
        print("\n-- Insert files into database:")
        for file_info in upload_results['files']:
            if file_info['success']:
                print(f"""
INSERT INTO files (file_uuid, person_id, original_filename, file_type, s3_key, file_size)
VALUES (
    '{file_info['file_uuid']}',
    {upload_results['person_id']},
    '{file_info['original_filename']}',
    '{file_info['file_type']}',
    '{file_info['s3_key']}',
    {file_info['file_size']}
);""")