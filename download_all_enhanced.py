#!/usr/bin/env python3
"""
Enhanced Download Pipeline - Supports both local and S3 storage
Uses configuration to determine default storage mode
"""

import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime

# Add utils to path
sys.path.append(str(Path(__file__).parent))

# Import consolidated modules
try:
    from utils.downloader import UnifiedDownloader, DownloadConfig, DownloadStrategy, RetryStrategy
    from utils.s3_manager import UnifiedS3Manager, UploadMode
    from utils.database_manager import get_database_manager
    from utils.config import get_config
    from utils.logging_config import get_logger
except ImportError as e:
    print(f"Error: Could not import required modules: {e}")
    sys.exit(1)

logger = get_logger(__name__)


def main():
    """Main function with storage mode detection"""
    parser = argparse.ArgumentParser(description="Enhanced download pipeline with S3/local support")
    parser.add_argument("--no-timeout", action="store_true", 
                       help="Disable download timeouts")
    parser.add_argument("--storage", choices=['local', 's3', 'auto'], default='auto',
                       help="Storage mode: local, s3, or auto (default: auto uses config)")
    parser.add_argument("--csv", default=None,
                       help="CSV file path (default: from config)")
    parser.add_argument("--rows", type=str, default=None,
                       help="Specific row IDs to process (comma-separated)")
    parser.add_argument("--max-rows", type=int, default=None,
                       help="Maximum number of rows to process")
    parser.add_argument("--bucket", default=None,
                       help="S3 bucket (default: from config)")
    parser.add_argument("--use-database", action="store_true",
                       help="Use database instead of CSV file")
    
    args = parser.parse_args()
    
    # Get configuration
    config = get_config()
    
    # Determine storage mode
    if args.storage == 'auto':
        storage_mode = config.get('downloads.storage_mode', 'local')
    else:
        storage_mode = args.storage
    
    logger.info("🚀 Starting Enhanced Download Pipeline")
    logger.info(f"🗄️  Storage Mode: {storage_mode.upper()}")
    logger.info("=" * 70)
    
    # Parse target rows
    target_rows = None
    if args.rows:
        target_rows = [int(row.strip()) for row in args.rows.split(',')]
        logger.info(f"🎯 Processing specific rows: {target_rows}")
    
    # Get data source
    if args.use_database:
        logger.info("📊 Using DATABASE as data source")
        db = get_database_manager()
        if not db.test_connection():
            logger.error("Database connection failed")
            return 1
    else:
        logger.info("📄 Using CSV as data source")
    
    if storage_mode == 's3':
        # Direct S3 streaming mode
        bucket = args.bucket or config.get('downloads.s3.default_bucket', 'typing-clients-uuid-system')
        logger.info(f"🪣 S3 Bucket: {bucket}")
        logger.info("📡 Direct streaming enabled - no local storage")
        
        # Initialize S3 manager
        from utils.s3_manager import S3Config
        s3_config = S3Config(
            bucket_name=bucket,
            upload_mode=UploadMode.DIRECT_STREAMING
        )
        s3_manager = UnifiedS3Manager(config=s3_config)
        
        # Get data
        import pandas as pd
        if args.use_database:
            if target_rows:
                people_df = pd.DataFrame()
                for row_id in target_rows:
                    person = db.get_person_by_row_id(row_id)
                    if person:
                        people_df = pd.concat([people_df, pd.DataFrame([person])])
            else:
                people_df = db.get_all_people()
                if args.max_rows:
                    people_df = people_df.head(args.max_rows)
        else:
            csv_file = args.csv or config.get('paths.output_csv', 'outputs/output.csv')
            people_df = pd.read_csv(csv_file)
            if target_rows:
                people_df = people_df[people_df['row_id'].isin(target_rows)]
            elif args.max_rows:
                people_df = people_df.head(args.max_rows)
        
        logger.info(f"👥 Processing {len(people_df)} people")
        
        # Save temporary CSV for S3 manager (it expects CSV path)
        temp_csv = Path("temp_s3_upload.csv")
        people_df.to_csv(temp_csv, index=False)
        
        # Process direct streaming
        results = s3_manager.process_direct_streaming(str(temp_csv))
        
        # Clean up temp file
        temp_csv.unlink()
        
        # Save report
        report_file = Path("s3_direct_upload_report.json")
        with open(report_file, 'w') as f:
            json.dump(results, f, indent=2)
        
        # Print summary
        total_success = sum(1 for r in results['uploads'] if r.get('success', False))
        total_failed = len(results['uploads']) - total_success
        
        logger.info(f"\n{'='*70}")
        logger.info("📊 S3 STREAMING SUMMARY")
        logger.info(f"{'='*70}")
        logger.info(f"✅ Successful uploads: {total_success}")
        logger.info(f"❌ Failed uploads: {total_failed}")
        logger.info(f"🪣 S3 Bucket: s3://{bucket}/")
        logger.info(f"📊 Report: {report_file}")
        
    else:
        # Local download mode
        logger.info("💾 Local storage mode")
        logger.info("📁 Files will be downloaded to local disk")
        
        # Configure downloader
        dl_config = DownloadConfig(
            timeout=None if args.no_timeout else 120,
            retry_strategy=RetryStrategy.NO_TIMEOUT if args.no_timeout else RetryStrategy.BASIC_RETRY,
            youtube_strategy=DownloadStrategy.VIDEO_BEST,
            youtube_format="mp4",
            show_progress=True,
            create_metadata=True
        )
        
        # Initialize downloader
        downloader = UnifiedDownloader(dl_config)
        
        # Process downloads
        if args.use_database:
            results = downloader.process_people_from_database(
                db, 
                target_rows=target_rows,
                max_rows=args.max_rows
            )
        else:
            results = downloader.process_csv(
                csv_file=args.csv,
                target_rows=target_rows,
                download_files=True
            )
        
        # Optional: Upload to S3 after local download
        if config.get('downloads.s3.auto_upload', False):
            logger.info("\n🔄 Auto-uploading to S3...")
            bucket = config.get('downloads.s3.default_bucket', 'typing-clients-uuid-system')
            s3_config = S3Config(
                bucket_name=bucket,
                upload_mode=UploadMode.LOCAL_THEN_UPLOAD
            )
            s3_uploader = UnifiedS3Manager(config=s3_config)
            upload_results = s3_uploader.process_local_upload(downloader.output_dir)
            logger.info(f"✅ Uploaded to s3://{bucket}/")
    
    logger.info("\n✨ Pipeline completed successfully!")
    return 0


if __name__ == "__main__":
    sys.exit(main())