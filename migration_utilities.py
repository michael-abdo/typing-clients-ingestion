#!/usr/bin/env python3
"""
S3 Migration Utilities for UUID-based File Organization
Created: 2025-07-13

Provides infrastructure for systematic S3 file migration with:
- UUID generation
- Batch processing with checkpoints
- Progress tracking and resumability
- Comprehensive error handling and logging
"""

import os
import uuid
import hashlib
import boto3
import psycopg2
from psycopg2.extras import RealDictCursor
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Optional, Tuple, Generator
from dataclasses import dataclass
import logging
import time
import json
import re
from concurrent.futures import ThreadPoolExecutor, as_completed

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

@dataclass
class FileInfo:
    """Information about a file to be migrated"""
    source_key: str
    person_id: int
    person_name: str
    file_extension: str
    file_size: Optional[int] = None
    source_checksum: Optional[str] = None

@dataclass
class MigrationResult:
    """Result of a file migration operation"""
    success: bool
    file_uuid: Optional[str] = None
    destination_key: Optional[str] = None
    error_message: Optional[str] = None
    execution_time: Optional[float] = None

class DatabaseManager:
    """Manages database connections and operations for migration tracking"""
    
    def __init__(self, db_config: Dict[str, str]):
        self.db_config = db_config
        self.logger = logging.getLogger(__name__)
    
    def get_connection(self):
        """Get database connection"""
        return psycopg2.connect(**self.db_config)
    
    def log_migration_operation(self, file_info: FileInfo, result: MigrationResult, batch_id: int):
        """Log a migration operation to the database"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO migration_state (
                        operation_type, source_path, destination_path, file_uuid,
                        person_id, person_name, file_size, operation_status,
                        error_message, started_at, completed_at
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    'file_copy',
                    file_info.source_key,
                    result.destination_key,
                    result.file_uuid,
                    file_info.person_id,
                    file_info.person_name,
                    file_info.file_size,
                    'completed' if result.success else 'failed',
                    result.error_message,
                    datetime.now(),
                    datetime.now() if result.success else None
                ))
                conn.commit()
    
    def create_batch(self, batch_name: str, total_files: int) -> int:
        """Create a new migration batch"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO migration_batches (batch_name, total_files)
                    VALUES (%s, %s) RETURNING batch_id
                """, (batch_name, total_files))
                batch_id = cur.fetchone()[0]
                conn.commit()
                return batch_id
    
    def update_batch_progress(self, batch_id: int, completed_files: int, failed_files: int):
        """Update batch progress"""
        with self.get_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    UPDATE migration_batches 
                    SET completed_files = %s, failed_files = %s, 
                        status = CASE WHEN completed_files + failed_files >= total_files 
                                     THEN 'completed' ELSE 'in_progress' END,
                        completed_at = CASE WHEN completed_files + failed_files >= total_files 
                                           THEN CURRENT_TIMESTAMP ELSE NULL END
                    WHERE batch_id = %s
                """, (completed_files, failed_files, batch_id))
                conn.commit()
    
    def get_migration_progress(self) -> Dict:
        """Get overall migration progress"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("SELECT * FROM migration_progress")
                return dict(cur.fetchone())
    
    def get_failed_operations(self) -> List[Dict]:
        """Get list of failed operations for retry"""
        with self.get_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT * FROM migration_state 
                    WHERE operation_status = 'failed'
                    ORDER BY started_at
                """)
                return [dict(row) for row in cur.fetchall()]

class UUIDGenerator:
    """Generates and manages UUIDs for file migration"""
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self.generated_uuids = set()
    
    def generate_file_uuid(self, file_info: FileInfo) -> str:
        """Generate a unique UUID for a file"""
        # Generate UUID4 (random)
        file_uuid = str(uuid.uuid4())
        
        # Ensure uniqueness (extremely unlikely collision, but safety first)
        while file_uuid in self.generated_uuids:
            file_uuid = str(uuid.uuid4())
        
        self.generated_uuids.add(file_uuid)
        return file_uuid
    
    def generate_s3_key(self, file_uuid: str, file_extension: str) -> str:
        """Generate S3 key for UUID-based storage"""
        # Structure: files/{uuid}.{extension}
        clean_extension = file_extension.lstrip('.')
        return f"files/{file_uuid}.{clean_extension}"

class S3MigrationManager:
    """Manages S3 file migration operations"""
    
    def __init__(self, source_bucket: str, destination_bucket: str, region: str = 'us-east-1'):
        self.source_bucket = source_bucket
        self.destination_bucket = destination_bucket
        self.region = region
        self.s3_client = boto3.client('s3', region_name=region)
        self.logger = logging.getLogger(__name__)
        self.uuid_generator = UUIDGenerator()
    
    def calculate_s3_checksum(self, bucket: str, key: str) -> Optional[str]:
        """Calculate MD5 checksum of S3 object"""
        try:
            response = self.s3_client.head_object(Bucket=bucket, Key=key)
            etag = response['ETag'].strip('"')
            
            # ETag is MD5 for single-part uploads
            if '-' not in etag:
                return etag
            
            # For multipart uploads, calculate actual MD5
            response = self.s3_client.get_object(Bucket=bucket, Key=key)
            md5_hash = hashlib.md5()
            for chunk in iter(lambda: response['Body'].read(8192), b""):
                md5_hash.update(chunk)
            return md5_hash.hexdigest()
            
        except Exception as e:
            self.logger.error(f"Failed to calculate checksum for {bucket}/{key}: {e}")
            return None
    
    def copy_file_with_uuid(self, file_info: FileInfo) -> MigrationResult:
        """Copy a file from source to destination with UUID naming"""
        start_time = time.time()
        
        try:
            # Generate UUID and destination key
            file_uuid = self.uuid_generator.generate_file_uuid(file_info)
            destination_key = self.uuid_generator.generate_s3_key(file_uuid, file_info.file_extension)
            
            self.logger.info(f"  📁 Copying {file_info.source_key} → {destination_key}")
            
            # Perform S3 copy operation
            copy_source = {
                'Bucket': self.source_bucket,
                'Key': file_info.source_key
            }
            
            # Add metadata to preserve context
            extra_args = {
                'MetadataDirective': 'REPLACE',
                'Metadata': {
                    'migration_timestamp': datetime.now().isoformat(),
                    'original_key': file_info.source_key,
                    'person_id': str(file_info.person_id),
                    'person_name': file_info.person_name,
                    'migration_batch': 'uuid_migration_2025'
                }
            }
            
            # Copy the file
            self.s3_client.copy_object(
                CopySource=copy_source,
                Bucket=self.destination_bucket,
                Key=destination_key,
                **extra_args
            )
            
            # Verify copy by checking existence and size
            response = self.s3_client.head_object(Bucket=self.destination_bucket, Key=destination_key)
            copied_size = response['ContentLength']
            
            execution_time = time.time() - start_time
            
            self.logger.info(f"     ✅ Copied successfully ({copied_size} bytes, {execution_time:.2f}s)")
            
            return MigrationResult(
                success=True,
                file_uuid=file_uuid,
                destination_key=destination_key,
                execution_time=execution_time
            )
            
        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = str(e)[:500]  # Truncate long error messages
            
            self.logger.error(f"     ❌ Copy failed: {error_msg}")
            
            return MigrationResult(
                success=False,
                error_message=error_msg,
                execution_time=execution_time
            )
    
    def verify_copy(self, source_key: str, destination_key: str) -> bool:
        """Verify that a copied file matches the source"""
        try:
            # Get source file info
            source_response = self.s3_client.head_object(Bucket=self.source_bucket, Key=source_key)
            source_size = source_response['ContentLength']
            
            # Get destination file info
            dest_response = self.s3_client.head_object(Bucket=self.destination_bucket, Key=destination_key)
            dest_size = dest_response['ContentLength']
            
            if source_size != dest_size:
                self.logger.error(f"Size mismatch: {source_key}({source_size}) vs {destination_key}({dest_size})")
                return False
            
            return True
            
        except Exception as e:
            self.logger.error(f"Verification failed for {source_key}: {e}")
            return False

class BatchProcessor:
    """Handles batch processing of migration operations"""
    
    def __init__(self, db_manager: DatabaseManager, s3_manager: S3MigrationManager, 
                 batch_size: int = 100, max_workers: int = 4):
        self.db_manager = db_manager
        self.s3_manager = s3_manager
        self.batch_size = batch_size
        self.max_workers = max_workers
        self.logger = logging.getLogger(__name__)
    
    def process_file_batch(self, files: List[FileInfo], batch_id: int) -> Tuple[int, int]:
        """Process a batch of files"""
        completed = 0
        failed = 0
        
        self.logger.info(f"🔄 Processing batch {batch_id} with {len(files)} files")
        
        # Process files in parallel for better performance
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Submit all copy operations
            future_to_file = {
                executor.submit(self.s3_manager.copy_file_with_uuid, file_info): file_info
                for file_info in files
            }
            
            # Process completed operations
            for future in as_completed(future_to_file):
                file_info = future_to_file[future]
                
                try:
                    result = future.result()
                    
                    # Log operation to database
                    self.db_manager.log_migration_operation(file_info, result, batch_id)
                    
                    if result.success:
                        completed += 1
                    else:
                        failed += 1
                        
                except Exception as e:
                    self.logger.error(f"Exception processing {file_info.source_key}: {e}")
                    # Log as failed operation
                    error_result = MigrationResult(success=False, error_message=str(e))
                    self.db_manager.log_migration_operation(file_info, error_result, batch_id)
                    failed += 1
        
        # Update batch progress
        self.db_manager.update_batch_progress(batch_id, completed, failed)
        
        self.logger.info(f"✅ Batch {batch_id} completed: {completed} success, {failed} failed")
        
        return completed, failed
    
    def create_file_batches(self, files: List[FileInfo]) -> Generator[List[FileInfo], None, None]:
        """Split files into batches for processing"""
        for i in range(0, len(files), self.batch_size):
            yield files[i:i + self.batch_size]

def extract_person_info_from_key(s3_key: str) -> Tuple[Optional[int], Optional[str]]:
    """Extract person ID and name from S3 key"""
    # Pattern: {row_id}/{Person_Name}/filename
    # or: {row_id}_Person_Name/filename
    
    parts = s3_key.split('/')
    if len(parts) < 2:
        return None, None
    
    first_part = parts[0]
    
    # Try pattern: {row_id} or {row_id}_Person_Name
    if '_' in first_part:
        # Pattern: {row_id}_Person_Name
        try:
            row_id = int(first_part.split('_')[0])
            person_name = '_'.join(first_part.split('_')[1:])
            return row_id, person_name
        except (ValueError, IndexError):
            pass
    
    # Try pattern: just {row_id}
    try:
        row_id = int(first_part)
        # Person name is the second part
        if len(parts) > 1:
            person_name = parts[1]
            return row_id, person_name
    except ValueError:
        pass
    
    return None, None

def get_file_extension(filename: str) -> str:
    """Extract file extension from filename"""
    return Path(filename).suffix.lower()

# Configuration
DEFAULT_DB_CONFIG = {
    'host': '127.0.0.1',
    'port': 5432,
    'database': 'typing_clients_uuid',
    'user': 'migration_user',
    'password': 'simple123'
}

if __name__ == "__main__":
    print("🚀 S3 Migration Utilities")
    print("Use this module as a library for migration operations")
    
    # Example usage
    db_manager = DatabaseManager(DEFAULT_DB_CONFIG)
    progress = db_manager.get_migration_progress()
    print(f"Migration Progress: {progress}")