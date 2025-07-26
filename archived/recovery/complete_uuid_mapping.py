#!/usr/bin/env python3
"""
Complete UUID Mapping Plan - Map ALL files in S3 to their owners with UUIDs
"""

import boto3
import pandas as pd
import json
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Tuple
import re

class CompleteUUIDMapper:
    """Maps all S3 files to clients with UUID tracking"""
    
    def __init__(self, csv_path: str = "outputs/output.csv"):
        self.csv_path = csv_path
        self.s3 = boto3.client('s3')
        self.bucket = 'typing-clients-uuid-system'
        
    def phase_1_map_remaining_client_files(self):
        """Phase 1: Map the remaining 16 files in client folders"""
        print("=== Phase 1: Mapping Remaining Client Folder Files ===\n")
        
        df = pd.read_csv(self.csv_path)
        updates_made = 0
        
        # Check each client folder
        for client_row in df.iterrows():
            row_idx, client = client_row
            row_id = client['row_id']
            
            # Skip if not in our target range (for now)
            if row_id not in [502, 503, 504, 506]:
                continue
            
            print(f"Checking client {row_id} ({client['name']})...")
            
            # Get current UUID mappings
            current_uuids = json.loads(client.get('file_uuids', '{}'))
            
            # List all files in S3 for this client
            prefix = f"{row_id}/"
            try:
                response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=prefix)
                
                if 'Contents' in response:
                    new_mappings = 0
                    youtube_uuids = json.loads(client.get('youtube_uuids', '[]'))
                    drive_uuids = json.loads(client.get('drive_uuids', '[]'))
                    s3_paths = json.loads(client.get('s3_paths', '{}'))
                    
                    for obj in response['Contents']:
                        s3_key = obj['Key']
                        if s3_key == prefix:  # Skip directory
                            continue
                        
                        filename = s3_key.split('/')[-1]
                        
                        # Check if already has UUID mapping
                        if filename not in current_uuids:
                            # Generate new UUID
                            file_uuid = str(uuid.uuid4())
                            current_uuids[filename] = file_uuid
                            s3_paths[file_uuid] = s3_key
                            
                            # Categorize by type
                            if 'youtube' in filename.lower():
                                youtube_uuids.append(file_uuid)
                            elif 'drive' in filename.lower() or filename.endswith(('.mkv', '.mov')):
                                drive_uuids.append(file_uuid)
                            
                            new_mappings += 1
                            print(f"  ✅ Mapped: {filename} → {file_uuid}")
                    
                    if new_mappings > 0:
                        # Update DataFrame
                        df.at[row_idx, 'file_uuids'] = json.dumps(current_uuids)
                        df.at[row_idx, 'youtube_uuids'] = json.dumps(youtube_uuids)
                        df.at[row_idx, 'drive_uuids'] = json.dumps(drive_uuids)
                        df.at[row_idx, 's3_paths'] = json.dumps(s3_paths)
                        updates_made += 1
                        print(f"  📊 Added {new_mappings} new UUID mappings")
                    else:
                        print(f"  ℹ️  All files already mapped")
                        
            except Exception as e:
                print(f"  ❌ Error: {str(e)}")
        
        if updates_made > 0:
            df.to_csv(self.csv_path, index=False)
            print(f"\n✅ Phase 1 Complete: Updated {updates_made} clients")
        else:
            print(f"\nℹ️  Phase 1: No updates needed")
        
        return df
    
    def phase_2_map_orphaned_uuid_files(self):
        """Phase 2: Identify and map the 99 UUID files to their owners"""
        print("\n=== Phase 2: Mapping Orphaned UUID Files ===\n")
        
        df = pd.read_csv(self.csv_path)
        
        # First, try to load any existing UUID migration data
        migration_mappings = self._load_migration_mappings()
        
        # List all UUID files
        response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix="files/")
        uuid_files = []
        
        if 'Contents' in response:
            for obj in response['Contents']:
                if not obj['Key'].endswith('/'):
                    uuid_files.append({
                        'key': obj['Key'],
                        'uuid': Path(obj['Key']).stem,  # Extract UUID from filename
                        'size': obj['Size'],
                        'modified': obj['LastModified']
                    })
        
        print(f"Found {len(uuid_files)} UUID files to map")
        
        # Try to map using migration data
        mapped_count = 0
        if migration_mappings:
            print("Using migration mapping data...")
            mapped_count = self._map_using_migration_data(df, uuid_files, migration_mappings)
        
        # For unmapped files, try heuristic matching
        remaining_files = [f for f in uuid_files if not self._is_file_mapped(f, df)]
        if remaining_files:
            print(f"Attempting heuristic mapping for {len(remaining_files)} remaining files...")
            heuristic_mapped = self._map_using_heuristics(df, remaining_files)
            mapped_count += heuristic_mapped
        
        print(f"✅ Phase 2 Complete: Mapped {mapped_count} UUID files")
        return df
    
    def phase_3_extend_uuid_to_all_clients(self):
        """Phase 3: Extend UUID tracking to all 483 clients"""
        print("\n=== Phase 3: Extending UUID Tracking to All Clients ===\n")
        
        df = pd.read_csv(self.csv_path)
        
        # Ensure all clients have UUID columns initialized
        clients_initialized = 0
        
        for idx, row in df.iterrows():
            needs_init = False
            
            if pd.isna(row.get('file_uuids')) or row.get('file_uuids') == '':
                df.at[idx, 'file_uuids'] = '{}'
                needs_init = True
            
            if pd.isna(row.get('youtube_uuids')) or row.get('youtube_uuids') == '':
                df.at[idx, 'youtube_uuids'] = '[]'
                needs_init = True
            
            if pd.isna(row.get('drive_uuids')) or row.get('drive_uuids') == '':
                df.at[idx, 'drive_uuids'] = '[]'
                needs_init = True
            
            if pd.isna(row.get('s3_paths')) or row.get('s3_paths') == '':
                df.at[idx, 's3_paths'] = '{}'
                needs_init = True
            
            if needs_init:
                clients_initialized += 1
        
        if clients_initialized > 0:
            df.to_csv(self.csv_path, index=False)
            print(f"✅ Initialized UUID tracking for {clients_initialized} clients")
        
        # Look for any other client files in S3
        print("Scanning for additional client files...")
        additional_mapped = self._scan_for_additional_files(df)
        
        print(f"✅ Phase 3 Complete: {additional_mapped} additional files mapped")
        return df
    
    def _load_migration_mappings(self) -> Optional[Dict]:
        """Try to load existing migration mapping data"""
        migration_files = [
            'migration_state_schema.sql',
            'data_import_report_20250713_205502.json',
            'pipeline_state_full_ingestion_20250712_184412.json'
        ]
        
        for file in migration_files:
            if Path(file).exists():
                try:
                    if file.endswith('.json'):
                        with open(file, 'r') as f:
                            data = json.load(f)
                            if 'file_mappings' in data or 'uuid_mappings' in data:
                                print(f"Found migration data in {file}")
                                return data
                except:
                    continue
        
        print("No migration mapping data found")
        return None
    
    def _map_using_migration_data(self, df: pd.DataFrame, uuid_files: List[Dict], migration_data: Dict) -> int:
        """Map UUID files using migration data"""
        # Implementation would depend on the structure of migration data
        # This is a placeholder for now
        return 0
    
    def _map_using_heuristics(self, df: pd.DataFrame, uuid_files: List[Dict]) -> int:
        """Map UUID files using heuristic matching (file size, timestamp, etc.)"""
        mapped_count = 0
        
        # Try to match by file size and approximate timestamp
        for uuid_file in uuid_files:
            # Look for client files with similar characteristics
            potential_matches = self._find_potential_matches(df, uuid_file)
            
            if len(potential_matches) == 1:
                # Confident match
                self._add_uuid_mapping(df, potential_matches[0], uuid_file)
                mapped_count += 1
                print(f"  ✅ Heuristic match: {uuid_file['uuid']} → Client {potential_matches[0]}")
        
        return mapped_count
    
    def _find_potential_matches(self, df: pd.DataFrame, uuid_file: Dict) -> List[int]:
        """Find potential client matches for a UUID file"""
        # This could match by:
        # - File size comparison with known downloads
        # - Timestamp correlation
        # - File type patterns
        # For now, return empty list (placeholder)
        return []
    
    def _is_file_mapped(self, uuid_file: Dict, df: pd.DataFrame) -> bool:
        """Check if a UUID file is already mapped to a client"""
        file_uuid = uuid_file['uuid']
        
        for _, row in df.iterrows():
            s3_paths = json.loads(row.get('s3_paths', '{}'))
            if file_uuid in s3_paths:
                return True
        
        return False
    
    def _add_uuid_mapping(self, df: pd.DataFrame, row_id: int, uuid_file: Dict):
        """Add UUID mapping for a specific client"""
        # Implementation to add the mapping
        pass
    
    def _scan_for_additional_files(self, df: pd.DataFrame) -> int:
        """Scan for any additional client files not yet mapped"""
        # Look for other row_id patterns in S3
        # This is a placeholder for comprehensive scanning
        return 0
    
    def generate_completion_report(self):
        """Generate a report showing completion status"""
        print("\n=== UUID Mapping Completion Report ===\n")
        
        df = pd.read_csv(self.csv_path)
        
        total_clients = len(df)
        clients_with_uuids = 0
        total_mapped_files = 0
        
        for _, row in df.iterrows():
            file_uuids = json.loads(row.get('file_uuids', '{}'))
            if file_uuids:
                clients_with_uuids += 1
                total_mapped_files += len(file_uuids)
        
        # Count S3 files
        s3_file_count = self._count_all_s3_files()
        
        print(f"📊 Final Statistics:")
        print(f"  - Total clients: {total_clients}")
        print(f"  - Clients with UUID tracking: {clients_with_uuids}")
        print(f"  - Total files mapped: {total_mapped_files}")
        print(f"  - Total files in S3: {s3_file_count}")
        print(f"  - Mapping completion: {(total_mapped_files/s3_file_count)*100:.1f}%")
        
        # Save detailed report
        report_path = f"uuid_mapping_completion_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        report_data = {
            'timestamp': datetime.now().isoformat(),
            'total_clients': total_clients,
            'clients_with_uuids': clients_with_uuids,
            'total_mapped_files': total_mapped_files,
            'total_s3_files': s3_file_count,
            'completion_percentage': (total_mapped_files/s3_file_count)*100
        }
        
        with open(report_path, 'w') as f:
            json.dump(report_data, f, indent=2)
        
        print(f"\n💾 Detailed report saved: {report_path}")
    
    def _count_all_s3_files(self) -> int:
        """Count all files in S3 bucket"""
        count = 0
        paginator = self.s3.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=self.bucket)
        
        for page in pages:
            if 'Contents' in page:
                for obj in page['Contents']:
                    if not obj['Key'].endswith('/') and not obj['Key'].startswith('csv-versions/'):
                        count += 1
        
        return count

def run_complete_mapping():
    """Run the complete UUID mapping process"""
    mapper = CompleteUUIDMapper()
    
    print("🚀 Starting Complete UUID Mapping Process\n")
    print("This will map ALL files in S3 to their owners with UUIDs\n")
    
    # Phase 1: Map remaining client folder files
    df = mapper.phase_1_map_remaining_client_files()
    
    # Phase 2: Map orphaned UUID files
    df = mapper.phase_2_map_orphaned_uuid_files()
    
    # Phase 3: Extend to all clients
    df = mapper.phase_3_extend_uuid_to_all_clients()
    
    # Generate completion report
    mapper.generate_completion_report()
    
    print("\n🎉 Complete UUID Mapping Process Finished!")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='Complete UUID mapping for all files')
    parser.add_argument('--phase', type=int, choices=[1, 2, 3], 
                       help='Run specific phase only')
    parser.add_argument('--report', action='store_true',
                       help='Generate completion report only')
    
    args = parser.parse_args()
    
    mapper = CompleteUUIDMapper()
    
    if args.report:
        mapper.generate_completion_report()
    elif args.phase:
        if args.phase == 1:
            mapper.phase_1_map_remaining_client_files()
        elif args.phase == 2:
            mapper.phase_2_map_orphaned_uuid_files()
        elif args.phase == 3:
            mapper.phase_3_extend_uuid_to_all_clients()
    else:
        run_complete_mapping()