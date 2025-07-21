#!/usr/bin/env python3
"""
Simple Workflow with Direct S3 Storage
Extracts data from Google Sheets and stores all media directly in S3
"""

import sys
import json
from pathlib import Path
from datetime import datetime

# Add parent directory to path for imports
sys.path.append(str(Path(__file__).parent))

from utils.cli_parser import create_workflow_parser, get_csv_columns
from utils.extract_links import extract_links_from_html
from utils.csv_manager import CSVManager, CSVMode
from utils.google_docs_http import extract_google_doc_via_http_export
from utils.s3_manager import UnifiedS3Manager, UploadMode
from utils.config import get_config
from utils.logging_config import get_logger
import pandas as pd

logger = get_logger(__name__)


def run_s3_workflow(args):
    """Run the simplified S3 workflow"""
    
    start_time = datetime.now()
    config = get_config()
    
    # Check storage mode from config
    storage_mode = config.get('downloads.storage_mode', 's3')
    logger.info(f"🗄️  Storage Mode: {storage_mode.upper()}")
    
    if storage_mode != 's3':
        logger.warning("⚠️  Config storage_mode is not 's3'. Proceeding with S3 storage anyway.")
    
    # Initialize S3 manager
    s3_bucket = config.get('downloads.s3.default_bucket', 'typing-clients-uuid-system')
    from utils.s3_manager import S3Config
    s3_config = S3Config(
        bucket_name=s3_bucket,
        upload_mode=UploadMode.DIRECT_STREAMING
    )
    s3_manager = UnifiedS3Manager(config=s3_config)
    
    logger.info(f"🪣 S3 Bucket: {s3_bucket}")
    logger.info("📡 Direct streaming enabled - no local storage")
    
    # Step 1: Extract data from Google Sheets (same as before)
    logger.info("\n📊 Step 1: Extracting data from Google Sheets...")
    
    # Initialize CSV manager
    columns = get_csv_columns(args.mode)
    csv_manager = CSVManager(csv_path=args.output, columns=columns)
    
    # Get Google Sheets URL from config
    google_sheet_url = config.get('google_sheets.url')
    
    # Extract links
    links_data = extract_links_from_html(google_sheet_url)
    
    if not links_data:
        logger.error("No data extracted from Google Sheets")
        return 1
    
    logger.info(f"✅ Extracted {len(links_data)} rows from Google Sheets")
    
    # Convert to DataFrame
    df = pd.DataFrame(links_data)
    
    # Step 2: Process Google Docs if in text or full mode
    if args.mode in [CSVMode.TEXT, CSVMode.FULL]:
        logger.info("\n📄 Step 2: Processing Google Docs...")
        doc_count = 0
        
        for idx, row in df.iterrows():
            if row.get('link') and 'docs.google.com' in str(row['link']):
                logger.info(f"  Extracting doc for {row['name']} (row {row['row_id']})")
                text = extract_google_doc_via_http_export(row['link'])
                if text:
                    df.at[idx, 'document_text'] = text
                    doc_count += 1
        
        logger.info(f"✅ Processed {doc_count} Google Docs")
    
    # Step 3: Save CSV (still needed for record keeping)
    logger.info("\n💾 Step 3: Saving extracted data to CSV...")
    csv_manager.save_data(df)
    logger.info(f"✅ Saved to {args.output}")
    
    # Step 4: Stream media directly to S3
    if args.download:
        logger.info("\n☁️  Step 4: Streaming media directly to S3...")
        
        # Filter to target rows if specified
        if args.rows:
            target_rows = [int(r.strip()) for r in args.rows.split(',')]
            df_to_process = df[df['row_id'].isin(target_rows)]
            logger.info(f"🎯 Processing {len(df_to_process)} specific rows")
        else:
            df_to_process = df
        
        # Process direct streaming to S3
        results = s3_manager.process_direct_streaming(df_to_process)
        
        # Save S3 upload report
        report_path = Path("s3_direct_upload_report.json")
        with open(report_path, 'w') as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"✅ S3 upload report saved to {report_path}")
        
        # Print summary
        total_success = sum(1 for r in results['uploads'] if r.get('success', False))
        total_failed = len(results['uploads']) - total_success
        
        logger.info(f"\n📊 S3 Upload Summary:")
        logger.info(f"  ✅ Successful: {total_success}")
        logger.info(f"  ❌ Failed: {total_failed}")
        logger.info(f"  🪣 Bucket: s3://{s3_bucket}/")
    
    # Complete
    elapsed = (datetime.now() - start_time).total_seconds()
    logger.info(f"\n✨ Workflow completed in {elapsed:.1f} seconds")
    logger.info("📡 All media stored directly in S3 - no local downloads!")
    
    return 0


def main():
    """Main entry point"""
    parser = create_workflow_parser(
        description="Simple workflow with direct S3 storage"
    )
    parser.add_argument(
        "--download", 
        action="store_true",
        help="Stream media files directly to S3"
    )
    parser.add_argument(
        "--rows",
        type=str,
        help="Specific row IDs to process (comma-separated)"
    )
    
    args = parser.parse_args()
    
    logger.info("🚀 Starting Simple S3 Workflow")
    logger.info(f"Mode: {args.mode.value}")
    
    try:
        return run_s3_workflow(args)
    except KeyboardInterrupt:
        logger.info("\n⚠️  Workflow interrupted by user")
        return 1
    except Exception as e:
        logger.error(f"❌ Workflow failed: {e}")
        return 1


if __name__ == "__main__":
    sys.exit(main())