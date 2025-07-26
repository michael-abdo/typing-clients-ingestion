#!/usr/bin/env python3
"""
Download all links from the most recent 30 people with better error handling
Enhanced with options for no-timeout mode and missing-people-only mode

CONSOLIDATED: Now uses UnifiedDownloader (DRY Refactoring Phase 2.5)
"""

import os
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime

# Add utils to path
sys.path.append(str(Path(__file__).parent))

# Import consolidated downloader and database manager
try:
    from utils.downloader import UnifiedDownloader, DownloadConfig, DownloadStrategy, RetryStrategy
    from utils.database_manager import get_database_manager
except ImportError as e:
    print(f"Error: Could not import required modules: {e}")
    sys.exit(1)


def main():
    """Main function with argument parsing"""
    parser = argparse.ArgumentParser(description="Download all links with configurable options")
    parser.add_argument("--no-timeout", action="store_true", 
                       help="Disable download timeouts (downloads may take longer)")
    parser.add_argument("--missing-only", action="store_true",
                       help="Process only missing people (rows 472-486)")
    parser.add_argument("--max-rows", type=int, default=None,
                       help="Maximum number of rows to process")
    parser.add_argument("--csv", default=None,
                       help="CSV file path (default: from config)")
    parser.add_argument("--output-dir", default=None,
                       help="Output directory (default: from config)")
    parser.add_argument("--use-database", action="store_true",
                       help="Use database instead of CSV file")
    parser.add_argument("--rows", type=str, default=None,
                       help="Specific row IDs to process (comma-separated)")
    
    args = parser.parse_args()
    
    # Missing people row IDs (for missing-only mode)
    missing_rows = [486, 485, 484, 483, 482, 481, 480, 477, 476, 474, 473, 472]
    
    # Configure UnifiedDownloader based on command-line arguments
    config = DownloadConfig(
        output_dir=args.output_dir,
        timeout=None if args.no_timeout else 120,
        retry_strategy=RetryStrategy.NO_TIMEOUT if args.no_timeout else RetryStrategy.BASIC_RETRY,
        youtube_strategy=DownloadStrategy.VIDEO_BEST,
        youtube_format="mp4",
        youtube_quality="128K",
        show_progress=True,
        create_metadata=True
    )
    
    # Initialize unified downloader
    downloader = UnifiedDownloader(config)
    
    # Display mode information
    mode_desc = "NO TIMEOUT MODE" if args.no_timeout else "MISSING PEOPLE ONLY" if args.missing_only else "STANDARD MODE"
    print(f"DOWNLOADING CONTENT - {mode_desc} (CONSOLIDATED)")
    print("=" * 70)
    print("NOTE: YouTube videos will be downloaded as MP4 video files")
    print("      Drive files will be downloaded (public files only)")
    if args.no_timeout:
        print("      NO TIMEOUT: Downloads may take longer but won't be interrupted")
    if args.missing_only:
        print("      MISSING ONLY: Processing only rows 472-486")
    print("=" * 70)
    
    # Determine target rows
    target_rows = None
    if args.rows:
        # Parse comma-separated row IDs
        target_rows = [int(row.strip()) for row in args.rows.split(',')]
    elif args.missing_only:
        target_rows = missing_rows
    
    # Process data based on source
    if args.use_database:
        print("Using DATABASE as data source")
        print("=" * 70)
        
        # Get database manager
        db = get_database_manager()
        
        # Test database connection
        if not db.test_connection():
            print("ERROR: Database connection failed. Falling back to CSV mode.")
            args.use_database = False
        else:
            # Get people data from database
            if target_rows:
                # Process specific rows
                results = downloader.process_people_from_database(db, target_rows=target_rows)
            else:
                # Process all people
                results = downloader.process_people_from_database(db, max_rows=args.max_rows)
    
    if not args.use_database:
        # Process CSV using UnifiedDownloader
        results = downloader.process_csv(
            csv_file=args.csv,
            target_rows=target_rows,
            download_files=False  # Only save Drive info, don't download files
        )
    
    # Generate legacy-compatible report
    output_dir = args.output_dir or downloader.output_dir
    report_file = Path(output_dir) / "download_report.json"
    with open(report_file, 'w') as f:
        json.dump({
            "started_at": downloader.stats.started_at,
            "completed_at": downloader.stats.completed_at,
            "mode": mode_desc.lower().replace(" ", "_"),
            "stats": {
                "total_people": downloader.stats.total_people,
                "people_with_links": downloader.stats.people_processed,
                "youtube_success": downloader.stats.youtube_success,
                "youtube_failed": downloader.stats.youtube_failed,
                "drive_saved": downloader.stats.drive_success,
                "drive_attempted": downloader.stats.drive_success + downloader.stats.drive_failed
            },
            "downloads": downloader.stats.downloads,
            "errors": downloader.stats.errors
        }, f, indent=2)
    
    # Print summary
    print(f"\n{'='*70}")
    print("DOWNLOAD SUMMARY")
    print(f"{'='*70}")
    print(f"Total people: {downloader.stats.total_people}")
    print(f"People with links: {downloader.stats.people_processed}")
    print(f"\nYouTube:")
    print(f"  ✅ Downloaded: {downloader.stats.youtube_success}")
    print(f"  ❌ Failed: {downloader.stats.youtube_failed}")
    print(f"\nGoogle Drive:")
    print(f"  📁 Info saved: {downloader.stats.drive_success}")
    print(f"\n📊 Full report: {report_file}")
    print(f"📁 Downloads directory: {downloader.output_dir}")
    print(f"\n🎯 CONSOLIDATED: Using UnifiedDownloader (DRY refactoring complete)")


if __name__ == "__main__":
    main()