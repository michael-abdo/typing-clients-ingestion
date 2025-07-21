#!/usr/bin/env python3
"""
Direct S3 Download Pipeline - Downloads directly to S3 without local storage
"""

import sys
import argparse
from pathlib import Path

# Add utils to path
sys.path.append(str(Path(__file__).parent))

from utils.s3_manager import UnifiedS3Manager, UploadMode
from utils.database_manager import get_database_manager
from utils.logging_config import get_logger
import pandas as pd

logger = get_logger(__name__)


def main():
    """Main function for direct S3 downloads"""
    parser = argparse.ArgumentParser(description="Download content directly to S3")
    parser.add_argument("--csv", default=None, help="CSV file path (default: from config)")
    parser.add_argument("--rows", type=str, help="Specific row IDs to process (comma-separated)")
    parser.add_argument("--bucket", default='typing-clients-uuid-system', help="S3 bucket name")
    parser.add_argument("--use-database", action="store_true", help="Use database instead of CSV")
    parser.add_argument("--max-rows", type=int, help="Maximum number of rows to process")
    
    args = parser.parse_args()
    
    logger.info("🚀 Starting Direct S3 Download Pipeline")
    logger.info(f"🪣 Target bucket: {args.bucket}")
    logger.info("📡 Mode: Direct streaming to S3 (no local storage)")
    logger.info("=" * 70)
    
    # Initialize S3 manager in streaming mode
    s3_manager = UnifiedS3Manager(
        bucket_name=args.bucket,
        mode=UploadMode.DIRECT_STREAMING
    )
    
    # Determine target rows
    target_rows = None
    if args.rows:
        target_rows = [int(row.strip()) for row in args.rows.split(',')]
        logger.info(f"🎯 Processing specific rows: {target_rows}")
    
    # Get data source
    if args.use_database:
        logger.info("📊 Using database as data source")
        db = get_database_manager()
        if not db.test_connection():
            logger.error("Database connection failed")
            return 1
        
        # Get people data
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
        # Use CSV
        csv_file = args.csv or 'outputs/output.csv'
        logger.info(f"📄 Using CSV: {csv_file}")
        people_df = pd.read_csv(csv_file)
        
        if target_rows:
            people_df = people_df[people_df['row_id'].isin(target_rows)]
        elif args.max_rows:
            people_df = people_df.head(args.max_rows)
    
    logger.info(f"👥 Processing {len(people_df)} people")
    
    # Process direct streaming
    results = s3_manager.process_direct_streaming(people_df)
    
    # Print summary
    logger.info("\n" + "=" * 70)
    logger.info("📊 DIRECT S3 UPLOAD SUMMARY")
    logger.info("=" * 70)
    
    total_success = sum(1 for r in results['uploads'] if r.get('success', False))
    total_failed = len(results['uploads']) - total_success
    
    logger.info(f"✅ Successful uploads: {total_success}")
    logger.info(f"❌ Failed uploads: {total_failed}")
    logger.info(f"⏱️  Total time: {results['stats']['total_time']:.1f}s")
    
    if results['stats'].get('total_size', 0) > 0:
        size_gb = results['stats']['total_size'] / (1024**3)
        logger.info(f"💾 Total data: {size_gb:.2f} GB")
    
    logger.info(f"\n📁 S3 Bucket: s3://{args.bucket}/")
    logger.info("✨ All content uploaded directly to S3 without local storage!")
    
    return 0


if __name__ == "__main__":
    sys.exit(main())