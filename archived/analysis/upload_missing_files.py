#!/usr/bin/env python3
"""
Upload missing files to S3 for clients 502-506.
Auto-generated script - review before running.
"""

import boto3
import os
import sys
from pathlib import Path
from datetime import datetime

def upload_file_to_s3(local_path, s3_key, bucket_name='typing-clients-storage-2025'):
    """Upload a single file to S3."""
    s3_client = boto3.client('s3')
    
    try:
        print(f"Uploading {local_path} to s3://{bucket_name}/{s3_key}")
        
        # Get file size for progress tracking
        file_size = Path(local_path).stat().st_size
        
        # Upload with progress callback
        with open(local_path, 'rb') as f:
            s3_client.upload_fileobj(
                f, 
                bucket_name, 
                s3_key,
                Callback=lambda bytes_transferred: print(f"  Progress: {bytes_transferred}/{file_size} bytes", end='\r')
            )
        
        print(f"  ✅ Upload complete: {s3_key}")
        return True
    except Exception as e:
        print(f"  ❌ Error uploading {local_path}: {e}")
        return False

def main():
    """Main upload function."""
    # Upload plan
    uploads = 
    # Client 502
    ('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/downloads/502_Sam_Torode/youtube_7cufMri1c5o.mp4', '502_Sam_Torode/youtube_7cufMri1c5o.mp4'),
    ('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/downloads/502_Sam_Torode/youtube_sV5oH7itRyo.mp4', '502_Sam_Torode/youtube_sV5oH7itRyo.mp4'),
    ('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/downloads/502_Sam_Torode/youtube_q2QMw4nGV0A.mp4', '502_Sam_Torode/youtube_q2QMw4nGV0A.mp4'),
    ('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/downloads/502_Sam_Torode/youtube_jgmL98lDNDU.mp4', '502_Sam_Torode/youtube_jgmL98lDNDU.mp4'),
    ('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/downloads/502_Sam_Torode/youtube_cfZmeDJ7Rls.mp4', '502_Sam_Torode/youtube_cfZmeDJ7Rls.mp4'),
    ('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/downloads/502_Sam_Torode/youtube_M36f9CGC0QY.mp4', '502_Sam_Torode/youtube_M36f9CGC0QY.mp4'),

    # Client 503
    ('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/downloads/503_Augusto_Evangelista/youtube_H-yfWylQeKM.mp4', '503_Augusto_Evangelista/youtube_H-yfWylQeKM.mp4'),
    ('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/downloads/503_Augusto_Evangelista/youtube_jthAa5FEqZg.mp4', '503_Augusto_Evangelista/youtube_jthAa5FEqZg.mp4'),
    ('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/downloads/503_Augusto_Evangelista/youtube_ZxIdoPEbHIA.mp4', '503_Augusto_Evangelista/youtube_ZxIdoPEbHIA.mp4'),
    ('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/downloads/503_Augusto_Evangelista/youtube_MTDf5DeieeM.mp4', '503_Augusto_Evangelista/youtube_MTDf5DeieeM.mp4'),
    ('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/downloads/503_Augusto_Evangelista/youtube_H-yfWylQeKM.m4a', '503_Augusto_Evangelista/youtube_H-yfWylQeKM.m4a'),
    ('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/downloads/503_Augusto_Evangelista/youtube_e_ypuna0tus.m4a', '503_Augusto_Evangelista/youtube_e_ypuna0tus.m4a'),
    ('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/downloads/503_Augusto_Evangelista/youtube_l7djwhCyJMI.mp4', '503_Augusto_Evangelista/youtube_l7djwhCyJMI.mp4'),
    ('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/downloads/503_Augusto_Evangelista/youtube_PGprS6kWY88.mp4', '503_Augusto_Evangelista/youtube_PGprS6kWY88.mp4'),
    ('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/downloads/503_Augusto_Evangelista/youtube_Q9gPcEmbyMA.m4a', '503_Augusto_Evangelista/youtube_Q9gPcEmbyMA.m4a'),
    ('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/downloads/503_Augusto_Evangelista/youtube_Q9gPcEmbyMA.mp4', '503_Augusto_Evangelista/youtube_Q9gPcEmbyMA.mp4'),
    ('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/downloads/503_Augusto_Evangelista/youtube_qIV2XD4v-ZM.mp4', '503_Augusto_Evangelista/youtube_qIV2XD4v-ZM.mp4'),
    ('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/downloads/503_Augusto_Evangelista/youtube_e_ypuna0tus.mp4', '503_Augusto_Evangelista/youtube_e_ypuna0tus.mp4'),
    ('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/downloads/503_Augusto_Evangelista/youtube_2JC1aaqk2tU.mp4', '503_Augusto_Evangelista/youtube_2JC1aaqk2tU.mp4'),

    # Client 504
    ('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/downloads/504_Navid_Ghahremani/Video 1.mp4', '504_Navid_Ghahremani/Video 1.mp4'),
    ('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/downloads/504_Navid_Ghahremani/Video 2.mp4', '504_Navid_Ghahremani/Video 2.mp4'),

    # Client 506
    ('/home/Mike/projects/xenodex/typing-clients-ingestion-minimal/downloads/506_Ryan_Madera/VIDEO TYPING - Ryan Madera.mkv', '506_Ryan_Madera/VIDEO TYPING - Ryan Madera.mkv'),
    ]
    
    print(f"Starting upload of {len(uploads)} files...")
    success_count = 0
    
    for local_path, s3_key in uploads:
        if upload_file_to_s3(local_path, s3_key):
            success_count += 1
    
    print(f"\n\nUpload complete: {success_count}/{len(uploads)} files uploaded successfully")

if __name__ == "__main__":
    main()
