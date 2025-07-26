#!/usr/bin/env python3
"""
S3 Migration Executor
Created: 2025-07-13

Executes the systematic file duplication with:
- Batch processing with checkpoints
- UUID generation and verification
- Comprehensive logging and error handling
- Progress tracking and resumability
"""

import json
import time
import logging
from pathlib import Path
from typing import List, Dict
from migration_utilities import (
    DatabaseManager, S3MigrationManager, BatchProcessor, 
    FileInfo, DEFAULT_DB_CONFIG
)

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class MigrationExecutor:
    """Executes the complete S3 migration process"""
    
    def __init__(self, source_bucket: str, destination_bucket: str, 
                 inventory_file: str, batch_size: int = 100):
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.inventory_file = inventory_file
        self.batch_size = batch_size
        
        self.logger = logging.getLogger(__name__)
        
        # Initialize managers
        self.db_manager = DatabaseManager(DEFAULT_DB_CONFIG)
        self.s3_manager = S3MigrationManager(source_bucket, destination_bucket)
        self.batch_processor = BatchProcessor(
            self.db_manager, 
            self.s3_manager, 
            batch_size=batch_size,
            max_workers=4
        )
        
        # Statistics
        self.total_files = 0
        self.completed_files = 0
        self.failed_files = 0
        self.total_size = 0
        self.start_time = None
        
    def load_inventory(self) -> List[FileInfo]:
        """Load file inventory from JSON file"""
        self.logger.info(f"📋 Loading inventory from: {self.inventory_file}")
        
        with open(self.inventory_file, 'r') as f:
            inventory_data = json.load(f)
        
        # Extract file information
        files = []
        for file_data in inventory_data['files']:
            file_info = FileInfo(
                source_key=file_data['source_key'],
                person_id=file_data['person_id'],
                person_name=file_data['person_name'],
                file_extension=file_data['file_extension'],
                file_size=file_data['file_size'],
                source_checksum=file_data['source_checksum']
            )
            files.append(file_info)
        
        self.total_files = len(files)
        self.total_size = sum(f.file_size or 0 for f in files)
        
        self.logger.info(f"✅ Loaded {self.total_files} files ({self.total_size / (1024**3):.2f} GB)")
        
        return files
    
    def create_migration_summary(self) -> Dict:
        """Create migration summary for logging"""
        return {
            'migration_metadata': {
                'start_time': self.start_time,
                'end_time': time.time(),
                'duration_seconds': time.time() - self.start_time if self.start_time else 0,
                'source_bucket': self.source_bucket,
                'destination_bucket': self.destination_bucket,
                'batch_size': self.batch_size
            },
            'statistics': {
                'total_files': self.total_files,
                'completed_files': self.completed_files,
                'failed_files': self.failed_files,
                'success_rate': (self.completed_files / self.total_files * 100) if self.total_files > 0 else 0,
                'total_size_gb': self.total_size / (1024**3)
            }
        }
    
    def print_progress_update(self, batch_num: int, total_batches: int, 
                            batch_completed: int, batch_failed: int):
        """Print progress update"""
        self.completed_files += batch_completed
        self.failed_files += batch_failed
        
        elapsed_time = time.time() - self.start_time
        progress_pct = (self.completed_files + self.failed_files) / self.total_files * 100
        
        files_per_second = (self.completed_files + self.failed_files) / elapsed_time if elapsed_time > 0 else 0
        eta_seconds = (self.total_files - self.completed_files - self.failed_files) / files_per_second if files_per_second > 0 else 0
        
        self.logger.info(f"\n📊 Progress Update - Batch {batch_num}/{total_batches}")
        self.logger.info(f"   ✅ Completed: {self.completed_files}")
        self.logger.info(f"   ❌ Failed: {self.failed_files}")
        self.logger.info(f"   📈 Progress: {progress_pct:.1f}%")
        self.logger.info(f"   ⏱️  ETA: {eta_seconds/60:.1f} minutes")
        self.logger.info(f"   🏃 Speed: {files_per_second:.1f} files/sec")
    
    def execute_migration(self, start_from_batch: int = 0) -> Dict:
        """Execute the complete migration process"""
        self.start_time = time.time()
        
        self.logger.info("=" * 70)
        self.logger.info("🚀 STARTING S3 MIGRATION EXECUTION")
        self.logger.info("=" * 70)
        self.logger.info(f"Source: {self.source_bucket}")
        self.logger.info(f"Destination: {self.destination_bucket}")
        self.logger.info(f"Batch Size: {self.batch_size}")
        
        try:
            # Load inventory
            files = self.load_inventory()
            
            if not files:
                self.logger.error("❌ No files to migrate")
                return self.create_migration_summary()
            
            # Filter out already processed files if resuming
            if start_from_batch > 0:
                start_index = start_from_batch * self.batch_size
                files = files[start_index:]
                self.logger.info(f"🔄 Resuming from batch {start_from_batch} (file {start_index})")
            
            # Create migration batch record
            batch_name = f"migration_{int(self.start_time)}"
            main_batch_id = self.db_manager.create_batch(batch_name, len(files))
            
            self.logger.info(f"📝 Created migration batch: {main_batch_id}")
            
            # Process files in batches
            file_batches = list(self.batch_processor.create_file_batches(files))
            total_batches = len(file_batches)
            
            self.logger.info(f"🔄 Processing {len(files)} files in {total_batches} batches")
            
            for batch_num, file_batch in enumerate(file_batches, 1):
                batch_start_time = time.time()
                
                self.logger.info(f"\n{'='*50}")
                self.logger.info(f"🔄 BATCH {batch_num}/{total_batches}")
                self.logger.info(f"{'='*50}")
                
                # Create sub-batch for tracking
                sub_batch_name = f"{batch_name}_batch_{batch_num}"
                sub_batch_id = self.db_manager.create_batch(sub_batch_name, len(file_batch))
                
                # Process the batch
                completed, failed = self.batch_processor.process_file_batch(file_batch, sub_batch_id)
                
                batch_time = time.time() - batch_start_time
                
                self.logger.info(f"⏱️  Batch {batch_num} completed in {batch_time:.2f} seconds")
                self.logger.info(f"   ✅ Success: {completed}")
                self.logger.info(f"   ❌ Failed: {failed}")
                
                # Update progress
                self.print_progress_update(batch_num, total_batches, completed, failed)
                
                # Small delay between batches to avoid overwhelming S3
                if batch_num < total_batches:
                    time.sleep(1)
            
            # Final summary
            migration_summary = self.create_migration_summary()
            
            # Update main batch
            self.db_manager.update_batch_progress(
                main_batch_id, 
                self.completed_files, 
                self.failed_files
            )
            
            # Save migration summary
            summary_file = f"migration_summary_{int(self.start_time)}.json"
            with open(summary_file, 'w') as f:
                json.dump(migration_summary, f, indent=2)
            
            self.print_final_summary(migration_summary, summary_file)
            
            return migration_summary
            
        except Exception as e:
            self.logger.error(f"❌ Migration failed with error: {e}")
            raise
    
    def print_final_summary(self, summary: Dict, summary_file: str):
        """Print final migration summary"""
        stats = summary['statistics']
        metadata = summary['migration_metadata']
        
        self.logger.info("\n" + "=" * 70)
        self.logger.info("🎉 MIGRATION EXECUTION COMPLETED")
        self.logger.info("=" * 70)
        self.logger.info(f"⏱️  Duration: {metadata['duration_seconds']/60:.1f} minutes")
        self.logger.info(f"✅ Completed: {stats['completed_files']}")
        self.logger.info(f"❌ Failed: {stats['failed_files']}")
        self.logger.info(f"📊 Success Rate: {stats['success_rate']:.1f}%")
        self.logger.info(f"💾 Total Size: {stats['total_size_gb']:.2f} GB")
        self.logger.info(f"📄 Summary saved: {summary_file}")
        
        if stats['failed_files'] > 0:
            self.logger.warning(f"\n⚠️  {stats['failed_files']} files failed - check database for details")
            failed_operations = self.db_manager.get_failed_operations()
            if failed_operations:
                self.logger.info("Failed files:")
                for op in failed_operations[:5]:  # Show first 5
                    self.logger.info(f"   - {op['source_path']}: {op['error_message']}")
                if len(failed_operations) > 5:
                    self.logger.info(f"   ... and {len(failed_operations) - 5} more")
        else:
            self.logger.info("\n🎉 ALL FILES MIGRATED SUCCESSFULLY!")

def main():
    """Main execution function"""
    # Configuration
    SOURCE_BUCKET = 'typing-clients-storage-2025'
    DESTINATION_BUCKET = 'typing-clients-uuid-system'
    INVENTORY_FILE = 's3_inventory_20250713_204320.json'
    BATCH_SIZE = 100
    
    # Check if inventory file exists
    if not Path(INVENTORY_FILE).exists():
        print(f"❌ Inventory file not found: {INVENTORY_FILE}")
        print("Run file_discovery.py first to generate the inventory")
        return
    
    # Create executor
    executor = MigrationExecutor(
        SOURCE_BUCKET, 
        DESTINATION_BUCKET, 
        INVENTORY_FILE, 
        BATCH_SIZE
    )
    
    # Execute migration
    try:
        summary = executor.execute_migration()
        
        if summary['statistics']['success_rate'] == 100:
            print("\n🎉 Migration completed successfully!")
            print("Ready to proceed to next phase")
        else:
            print(f"\n⚠️  Migration completed with {summary['statistics']['failed_files']} failures")
            print("Review failed operations before proceeding")
            
    except Exception as e:
        print(f"\n❌ Migration failed: {e}")
        print("Check logs and database for details")

if __name__ == "__main__":
    main()