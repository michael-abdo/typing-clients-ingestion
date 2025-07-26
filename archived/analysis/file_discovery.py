#!/usr/bin/env python3
"""
S3 File Discovery and Validation
Created: 2025-07-13

Scans the source S3 bucket to:
- Discover all files for migration
- Validate file accessibility
- Generate comprehensive inventory with checksums
- Extract person information from file paths
"""

import boto3
import json
import csv
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import logging
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import hashlib
from migration_utilities import extract_person_info_from_key, get_file_extension, FileInfo

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

class S3FileDiscovery:
    """Discovers and validates files in S3 bucket for migration"""
    
    def __init__(self, bucket_name: str, region: str = 'us-east-1'):
        self.bucket_name = bucket_name
        self.region = region
        self.s3_client = boto3.client('s3', region_name=region)
        self.logger = logging.getLogger(__name__)
        
        # Stats tracking
        self.total_files = 0
        self.valid_files = 0
        self.invalid_files = 0
        self.total_size = 0
        
    def list_all_objects(self) -> List[Dict]:
        """List all objects in the S3 bucket"""
        self.logger.info(f"🔍 Scanning S3 bucket: {self.bucket_name}")
        
        objects = []
        paginator = self.s3_client.get_paginator('list_objects_v2')
        page_iterator = paginator.paginate(Bucket=self.bucket_name)
        
        for page in page_iterator:
            if 'Contents' in page:
                for obj in page['Contents']:
                    objects.append({
                        'Key': obj['Key'],
                        'Size': obj['Size'],
                        'LastModified': obj['LastModified'].isoformat(),
                        'ETag': obj['ETag'].strip('"')
                    })
                    self.total_files += 1
                    self.total_size += obj['Size']
        
        self.logger.info(f"✅ Found {self.total_files} objects ({self.total_size / (1024**3):.2f} GB)")
        return objects
    
    def validate_file_access(self, s3_key: str) -> Tuple[bool, Optional[str]]:
        """Validate that a file is accessible and get metadata"""
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            return True, None
        except Exception as e:
            return False, str(e)
    
    def get_file_checksum(self, s3_key: str) -> Optional[str]:
        """Get file checksum (ETag for single-part uploads)"""
        try:
            response = self.s3_client.head_object(Bucket=self.bucket_name, Key=s3_key)
            etag = response['ETag'].strip('"')
            
            # For single-part uploads, ETag is the MD5 hash
            if '-' not in etag:
                return etag
            else:
                # For multipart uploads, we'll calculate it if needed
                return etag  # Keep the multipart ETag for now
                
        except Exception as e:
            self.logger.error(f"Failed to get checksum for {s3_key}: {e}")
            return None
    
    def process_file_batch(self, files: List[Dict]) -> List[FileInfo]:
        """Process a batch of files to extract information"""
        file_infos = []
        
        for file_obj in files:
            s3_key = file_obj['Key']
            
            # Extract person information from the key
            person_id, person_name = extract_person_info_from_key(s3_key)
            
            if person_id is None:
                self.logger.warning(f"⚠️  Could not extract person info from: {s3_key}")
                self.invalid_files += 1
                continue
            
            # Get file extension
            file_extension = get_file_extension(s3_key)
            
            # Validate file access
            is_accessible, error = self.validate_file_access(s3_key)
            
            if not is_accessible:
                self.logger.error(f"❌ File not accessible: {s3_key} - {error}")
                self.invalid_files += 1
                continue
            
            # Get checksum
            checksum = self.get_file_checksum(s3_key)
            
            # Create FileInfo object
            file_info = FileInfo(
                source_key=s3_key,
                person_id=person_id,
                person_name=person_name,
                file_extension=file_extension,
                file_size=file_obj['Size'],
                source_checksum=checksum
            )
            
            file_infos.append(file_info)
            self.valid_files += 1
            
        return file_infos
    
    def discover_all_files(self, max_workers: int = 10) -> List[FileInfo]:
        """Discover and validate all files in the bucket"""
        start_time = time.time()
        
        # Get all objects
        all_objects = self.list_all_objects()
        
        if not all_objects:
            self.logger.warning("No objects found in bucket")
            return []
        
        # Process in batches for better performance
        batch_size = 100
        file_infos = []
        
        self.logger.info(f"🔄 Processing {len(all_objects)} files in batches of {batch_size}")
        
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Submit batches for processing
            futures = []
            for i in range(0, len(all_objects), batch_size):
                batch = all_objects[i:i + batch_size]
                future = executor.submit(self.process_file_batch, batch)
                futures.append(future)
            
            # Collect results
            for future in as_completed(futures):
                try:
                    batch_results = future.result()
                    file_infos.extend(batch_results)
                except Exception as e:
                    self.logger.error(f"Batch processing failed: {e}")
        
        execution_time = time.time() - start_time
        
        self.logger.info(f"✅ File discovery completed in {execution_time:.2f} seconds")
        self.logger.info(f"📊 Results: {self.valid_files} valid, {self.invalid_files} invalid files")
        
        return file_infos
    
    def generate_inventory_report(self, file_infos: List[FileInfo], output_file: str = None):
        """Generate comprehensive inventory report"""
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"s3_inventory_{timestamp}.json"
        
        # Create summary statistics
        file_types = {}
        people_stats = {}
        
        for file_info in file_infos:
            # File type statistics
            ext = file_info.file_extension.lower()
            if ext not in file_types:
                file_types[ext] = {'count': 0, 'total_size': 0}
            file_types[ext]['count'] += 1
            file_types[ext]['total_size'] += file_info.file_size or 0
            
            # People statistics
            if file_info.person_id not in people_stats:
                people_stats[file_info.person_id] = {
                    'name': file_info.person_name,
                    'file_count': 0,
                    'total_size': 0
                }
            people_stats[file_info.person_id]['file_count'] += 1
            people_stats[file_info.person_id]['total_size'] += file_info.file_size or 0
        
        # Create comprehensive report
        report = {
            'discovery_metadata': {
                'timestamp': datetime.now().isoformat(),
                'source_bucket': self.bucket_name,
                'total_files_found': self.total_files,
                'valid_files': self.valid_files,
                'invalid_files': self.invalid_files,
                'total_size_bytes': self.total_size,
                'total_size_gb': round(self.total_size / (1024**3), 2)
            },
            'file_type_breakdown': file_types,
            'people_statistics': people_stats,
            'files': [
                {
                    'source_key': f.source_key,
                    'person_id': f.person_id,
                    'person_name': f.person_name,
                    'file_extension': f.file_extension,
                    'file_size': f.file_size,
                    'source_checksum': f.source_checksum
                }
                for f in file_infos
            ]
        }
        
        # Save report
        with open(output_file, 'w') as f:
            json.dump(report, f, indent=2)
        
        self.logger.info(f"📄 Inventory report saved to: {output_file}")
        
        # Print summary
        self.print_summary(report)
        
        return report
    
    def generate_csv_inventory(self, file_infos: List[FileInfo], output_file: str = None):
        """Generate CSV inventory for easy viewing"""
        if not output_file:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            output_file = f"s3_inventory_{timestamp}.csv"
        
        with open(output_file, 'w', newline='') as csvfile:
            fieldnames = [
                'source_key', 'person_id', 'person_name', 'file_extension',
                'file_size', 'file_size_mb', 'source_checksum'
            ]
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for file_info in file_infos:
                writer.writerow({
                    'source_key': file_info.source_key,
                    'person_id': file_info.person_id,
                    'person_name': file_info.person_name,
                    'file_extension': file_info.file_extension,
                    'file_size': file_info.file_size or 0,
                    'file_size_mb': round((file_info.file_size or 0) / (1024*1024), 2),
                    'source_checksum': file_info.source_checksum or ''
                })
        
        self.logger.info(f"📊 CSV inventory saved to: {output_file}")
        return output_file
    
    def print_summary(self, report: Dict):
        """Print summary statistics"""
        metadata = report['discovery_metadata']
        file_types = report['file_type_breakdown']
        people_count = len(report['people_statistics'])
        
        self.logger.info("\n" + "=" * 60)
        self.logger.info("📊 S3 INVENTORY SUMMARY")
        self.logger.info("=" * 60)
        self.logger.info(f"Total Files: {metadata['total_files_found']}")
        self.logger.info(f"Valid Files: {metadata['valid_files']}")
        self.logger.info(f"Invalid Files: {metadata['invalid_files']}")
        self.logger.info(f"Total Size: {metadata['total_size_gb']} GB")
        self.logger.info(f"People: {people_count}")
        
        self.logger.info("\nFile Types:")
        for ext, stats in sorted(file_types.items()):
            size_mb = stats['total_size'] / (1024*1024)
            self.logger.info(f"  {ext}: {stats['count']} files ({size_mb:.1f} MB)")

def main():
    """Main function for file discovery"""
    discovery = S3FileDiscovery('typing-clients-storage-2025')
    
    # Discover all files
    file_infos = discovery.discover_all_files()
    
    if file_infos:
        # Generate reports
        json_report = discovery.generate_inventory_report(file_infos)
        csv_report = discovery.generate_csv_inventory(file_infos)
        
        print(f"\n✅ Discovery complete!")
        print(f"📄 JSON Report: {json_report}")
        print(f"📊 CSV Report: {csv_report}")
    else:
        print("❌ No valid files found for migration")

if __name__ == "__main__":
    main()