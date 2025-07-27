#!/usr/bin/env python3
"""
Streaming Integration Module - Bridge between link extraction and S3 streaming
Converts extracted links (YouTube, Drive files/folders) directly to S3 UUIDs
"""

import uuid
import json
from typing import Dict, List, Any, Optional
from datetime import datetime

try:
    from .s3_manager import UnifiedS3Manager, S3Config, UploadMode
    from .logging_config import get_logger
    from .download_drive import extract_file_id, list_folder_files
    from .patterns import extract_drive_id, extract_youtube_id
    from .error_handling import with_standard_error_handling
except ImportError:
    from s3_manager import UnifiedS3Manager, S3Config, UploadMode
    from logging_config import get_logger
    from download_drive import extract_file_id, list_folder_files
    from patterns import extract_drive_id, extract_youtube_id
    from error_handling import with_standard_error_handling

logger = get_logger(__name__)


class StreamingProgress:
    """Track streaming progress for multiple files"""
    def __init__(self, total_files: int):
        self.total_files = total_files
        self.completed_files = 0
        self.failed_files = 0
        self.start_time = datetime.now()
        self.current_file = None
        
    def update(self, file_name: str, success: bool):
        """Update progress for a completed file"""
        if success:
            self.completed_files += 1
        else:
            self.failed_files += 1
        
        progress_pct = ((self.completed_files + self.failed_files) / self.total_files) * 100
        elapsed = (datetime.now() - self.start_time).total_seconds()
        
        logger.info(f"📊 Progress: {progress_pct:.1f}% ({self.completed_files}/{self.total_files}) - "
                   f"Elapsed: {elapsed:.1f}s - Current: {file_name}")


@with_standard_error_handling("streaming_integration")
def stream_extracted_links(person: Dict[str, Any], links: Dict[str, List[str]], 
                          s3_manager: Optional[UnifiedS3Manager] = None) -> Dict[str, Dict]:
    """
    Convert extracted links to S3 UUIDs by streaming content directly to S3
    
    Args:
        person: Person data with row_id, name, etc.
        links: Extracted links dictionary with keys: youtube, drive_files, drive_folders
        s3_manager: Optional S3Manager instance (will create if not provided)
        
    Returns:
        Dictionary with 'file_uuids' and 's3_paths' mappings
    """
    logger.info(f"🚀 Starting S3 streaming for {person['name']} (Row {person.get('row_id', 'Unknown')})")
    
    # Initialize S3 manager if not provided
    if not s3_manager:
        s3_config = S3Config(
            bucket_name='typing-clients-uuid-system',
            upload_mode=UploadMode.DIRECT_STREAMING,
            organize_by_person=False,
            add_metadata=True
        )
        s3_manager = UnifiedS3Manager(s3_config)
    
    # Initialize results
    s3_results = {
        'file_uuids': {},  # Description -> UUID mapping
        's3_paths': {}     # UUID -> S3 path mapping
    }
    
    # Count total files for progress tracking
    total_files = (len(links.get('youtube', [])) + 
                  len(links.get('drive_files', [])) + 
                  len(links.get('drive_folders', [])))
    
    if total_files == 0:
        logger.info("No links to stream for this person")
        return s3_results
    
    progress = StreamingProgress(total_files)
    
    # Stream YouTube videos
    for i, youtube_url in enumerate(links.get('youtube', [])):
        logger.info(f"\n📹 Streaming YouTube video {i+1}/{len(links.get('youtube', []))}")
        success = stream_youtube_link(youtube_url, person, s3_results, s3_manager)
        progress.update(f"YouTube: {youtube_url}", success)
    
    # Stream Google Drive files
    for i, drive_url in enumerate(links.get('drive_files', [])):
        logger.info(f"\n📄 Streaming Drive file {i+1}/{len(links.get('drive_files', []))}")
        success = stream_drive_file(drive_url, person, s3_results, s3_manager)
        progress.update(f"Drive file: {drive_url}", success)
    
    # Stream Google Drive folders (list contents and stream each)
    for i, folder_url in enumerate(links.get('drive_folders', [])):
        logger.info(f"\n📁 Processing Drive folder {i+1}/{len(links.get('drive_folders', []))}")
        folder_files = stream_drive_folder(folder_url, person, s3_results, s3_manager, progress)
        # Progress is updated within stream_drive_folder for each file
    
    # Summary
    logger.info(f"\n✅ Streaming complete for {person['name']}:")
    logger.info(f"   Total files: {progress.total_files}")
    logger.info(f"   Successful: {progress.completed_files}")
    logger.info(f"   Failed: {progress.failed_files}")
    logger.info(f"   S3 UUIDs generated: {len(s3_results['file_uuids'])}")
    
    return s3_results


def stream_youtube_link(url: str, person: Dict[str, Any], s3_results: Dict, 
                       s3_manager: UnifiedS3Manager) -> bool:
    """Stream a single YouTube video to S3"""
    try:
        # Extract video ID for logging
        video_id = extract_youtube_id(url)
        logger.info(f"   YouTube URL: {url}")
        logger.info(f"   Video ID: {video_id}")
        
        # Generate UUID and S3 key
        file_uuid = str(uuid.uuid4())
        # DRY CONSOLIDATION - Step 1: Use centralized S3 key generation
        from .s3_manager import UnifiedS3Manager
        s3_key = UnifiedS3Manager.generate_uuid_s3_key(file_uuid, '.mp4')
        
        logger.info(f"   UUID: {file_uuid}")
        logger.info(f"   S3 Key: {s3_key}")
        
        # Stream to S3
        result = s3_manager.stream_youtube_to_s3(url, s3_key, person['name'])
        
        if result.success:
            # Update mappings
            description = f"YouTube: {video_id or url}"
            s3_results['file_uuids'][description] = file_uuid
            s3_results['s3_paths'][file_uuid] = s3_key
            
            logger.info(f"   ✅ Successfully streamed to S3")
            logger.info(f"   S3 URL: {result.s3_url}")
            logger.info(f"   Upload time: {result.upload_time:.2f}s")
            return True
        else:
            logger.error(f"   ❌ Failed to stream: {result.error}")
            return False
            
    except Exception as e:
        logger.error(f"   ❌ Error streaming YouTube video: {str(e)}")
        return False


def stream_drive_file(url: str, person: Dict[str, Any], s3_results: Dict, 
                     s3_manager: UnifiedS3Manager) -> bool:
    """Stream a single Google Drive file to S3"""
    try:
        # Extract file ID
        file_id = extract_drive_id(url) or extract_file_id(url)
        if not file_id:
            logger.error(f"   ❌ Could not extract Drive file ID from: {url}")
            return False
            
        logger.info(f"   Drive URL: {url}")
        logger.info(f"   File ID: {file_id}")
        
        # Generate UUID and S3 key
        file_uuid = str(uuid.uuid4())
        # Note: Using .bin extension as we don't know file type yet
        # This can be fixed later with fix_s3_extensions.py if needed
        s3_key = UnifiedS3Manager.generate_uuid_s3_key(file_uuid, '.bin')
        
        logger.info(f"   UUID: {file_uuid}")
        logger.info(f"   S3 Key: {s3_key}")
        
        # Stream to S3
        result = s3_manager.stream_drive_to_s3(file_id, s3_key)
        
        if result.success:
            # Update mappings
            description = f"Drive file: {file_id}"
            s3_results['file_uuids'][description] = file_uuid
            s3_results['s3_paths'][file_uuid] = s3_key
            
            logger.info(f"   ✅ Successfully streamed to S3")
            logger.info(f"   S3 URL: {result.s3_url}")
            if result.file_size:
                size_mb = result.file_size / (1024 * 1024)
                logger.info(f"   File size: {size_mb:.1f} MB")
            logger.info(f"   Upload time: {result.upload_time:.2f}s")
            return True
        else:
            logger.error(f"   ❌ Failed to stream: {result.error}")
            return False
            
    except Exception as e:
        logger.error(f"   ❌ Error streaming Drive file: {str(e)}")
        return False


def stream_drive_folder(folder_url: str, person: Dict[str, Any], s3_results: Dict, 
                       s3_manager: UnifiedS3Manager, progress: StreamingProgress) -> int:
    """Stream all files from a Google Drive folder to S3"""
    try:
        # Extract folder ID
        folder_id = extract_drive_id(folder_url) or extract_file_id(folder_url)
        if not folder_id:
            logger.error(f"   ❌ Could not extract Drive folder ID from: {folder_url}")
            return 0
            
        logger.info(f"   Folder URL: {folder_url}")
        logger.info(f"   Folder ID: {folder_id}")
        
        # List folder contents
        folder_files = list_folder_files(folder_id)
        if not folder_files:
            logger.warning(f"   ⚠️ No files found in folder or unable to access")
            return 0
            
        logger.info(f"   📂 Found {len(folder_files)} files in folder")
        
        # Update total files count for progress
        progress.total_files += len(folder_files) - 1  # -1 because folder itself was counted
        
        streamed_count = 0
        
        # Stream each file in the folder
        for file_info in folder_files:
            file_id = file_info.get('id')
            file_name = file_info.get('name', 'unknown')
            
            logger.info(f"\n   📄 Streaming folder file: {file_name}")
            logger.info(f"      File ID: {file_id}")
            
            # Generate UUID and S3 key
            file_uuid = str(uuid.uuid4())
            
            # Try to preserve file extension
            # DRY CONSOLIDATION - Step 2: Use centralized extension handling
            from .path_utils import get_extension_or_default
            file_ext = get_extension_or_default(file_name, '.bin')
            
            s3_key = UnifiedS3Manager.generate_uuid_s3_key(file_uuid, file_ext)
            
            logger.info(f"      UUID: {file_uuid}")
            logger.info(f"      S3 Key: {s3_key}")
            
            # Stream to S3
            result = s3_manager.stream_drive_to_s3(file_id, s3_key)
            
            if result.success:
                # Update mappings
                description = f"Folder file: {file_name}"
                s3_results['file_uuids'][description] = file_uuid
                s3_results['s3_paths'][file_uuid] = s3_key
                
                logger.info(f"      ✅ Successfully streamed")
                streamed_count += 1
                progress.update(f"Folder: {file_name}", True)
            else:
                logger.error(f"      ❌ Failed to stream: {result.error}")
                progress.update(f"Folder: {file_name}", False)
        
        logger.info(f"   📁 Folder complete: {streamed_count}/{len(folder_files)} files streamed")
        return streamed_count
        
    except Exception as e:
        logger.error(f"   ❌ Error streaming Drive folder: {str(e)}")
        return 0


def generate_streaming_report(person: Dict[str, Any], s3_results: Dict, 
                            start_time: datetime) -> Dict[str, Any]:
    """Generate a detailed report of the streaming operation"""
    elapsed_time = (datetime.now() - start_time).total_seconds()
    
    report = {
        'person': {
            'row_id': person.get('row_id'),
            'name': person.get('name'),
            'email': person.get('email')
        },
        'summary': {
            'total_files_streamed': len(s3_results['file_uuids']),
            'total_uuids_generated': len(s3_results['s3_paths']),
            'elapsed_time_seconds': elapsed_time
        },
        'file_mappings': s3_results['file_uuids'],
        's3_paths': s3_results['s3_paths'],
        'timestamp': datetime.now().isoformat()
    }
    
    return report