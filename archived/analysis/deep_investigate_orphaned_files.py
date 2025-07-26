#!/usr/bin/env python3
"""
Deep Investigation of Orphaned UUID Files
Comprehensive analysis to find mapping clues
"""

import boto3
import pandas as pd
import json
from datetime import datetime, timedelta
from pathlib import Path
from collections import defaultdict
import re

class OrphanedFileInvestigator:
    """Deep investigation of orphaned UUID files"""
    
    def __init__(self):
        self.s3 = boto3.client('s3')
        self.bucket = 'typing-clients-uuid-system'
        self.csv_path = 'outputs/output.csv'
        
    def investigate_all(self):
        """Run comprehensive investigation"""
        print("🔍 DEEP INVESTIGATION: Orphaned UUID Files")
        print("=" * 60)
        
        # Get all orphaned files
        orphaned_files = self._get_orphaned_files()
        print(f"\n📊 Found {len(orphaned_files)} orphaned UUID files")
        
        # Multiple investigation approaches
        self._analyze_file_patterns(orphaned_files)
        self._analyze_timestamps(orphaned_files)
        self._check_migration_reports(orphaned_files)
        self._analyze_file_sizes(orphaned_files)
        self._check_metadata_files(orphaned_files)
        self._look_for_naming_patterns(orphaned_files)
        self._correlate_with_known_clients(orphaned_files)
        
        # Generate detailed report
        self._generate_investigation_report(orphaned_files)
        
    def _get_orphaned_files(self):
        """Get all orphaned UUID files with detailed info"""
        response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix="files/")
        orphaned_files = []
        
        if 'Contents' in response:
            for obj in response['Contents']:
                if not obj['Key'].endswith('/'):
                    file_info = {
                        'key': obj['Key'],
                        'uuid': Path(obj['Key']).stem,
                        'filename': Path(obj['Key']).name,
                        'extension': Path(obj['Key']).suffix,
                        'size': obj['Size'],
                        'modified': obj['LastModified'],
                        'potential_matches': []
                    }
                    orphaned_files.append(file_info)
        
        return sorted(orphaned_files, key=lambda x: x['modified'])
    
    def _analyze_file_patterns(self, orphaned_files):
        """Analyze file type and naming patterns"""
        print(f"\n🎯 1. FILE PATTERN ANALYSIS")
        print("-" * 40)
        
        # Group by extension
        by_extension = defaultdict(list)
        for file in orphaned_files:
            by_extension[file['extension']].append(file)
        
        print("📁 Files by Type:")
        for ext, files in sorted(by_extension.items(), key=lambda x: len(x[1]), reverse=True):
            if not ext:
                ext = '(no extension)'
            print(f"  {ext}: {len(files)} files")
            
            # Show samples
            for i, file in enumerate(files[:3]):
                size_mb = file['size'] / (1024 * 1024)
                print(f"    - {file['filename']} ({size_mb:.1f} MB)")
        
        # Look for file naming patterns
        print(f"\n🔤 Filename Pattern Analysis:")
        patterns = {
            'youtube_pattern': r'youtube.*\.(mp4|webm|m4a)',
            'metadata_pattern': r'.*\.json$',
            'audio_pattern': r'\.(mp3|m4a|wav)$',
            'video_pattern': r'\.(mp4|webm|mkv|mov)$'
        }
        
        for pattern_name, pattern in patterns.items():
            matches = [f for f in orphaned_files if re.search(pattern, f['filename'], re.I)]
            if matches:
                print(f"  {pattern_name}: {len(matches)} files")
                for file in matches[:2]:
                    print(f"    - {file['filename']}")
    
    def _analyze_timestamps(self, orphaned_files):
        """Analyze file timestamps for patterns"""
        print(f"\n📅 2. TIMESTAMP ANALYSIS")
        print("-" * 40)
        
        # Group by date
        by_date = defaultdict(list)
        for file in orphaned_files:
            date_str = file['modified'].strftime('%Y-%m-%d')
            by_date[date_str].append(file)
        
        print("📆 Files by Upload Date:")
        for date, files in sorted(by_date.items()):
            print(f"  {date}: {len(files)} files")
        
        # Look for patterns around known client timestamps
        print(f"\n🕐 Correlation with Known Client Activity:")
        df = pd.read_csv(self.csv_path)
        
        # Get known client file timestamps from current mappings
        known_timestamps = []
        for _, row in df.iterrows():
            if pd.notna(row.get('last_download_attempt')):
                try:
                    timestamp = pd.to_datetime(row['last_download_attempt'])
                    known_timestamps.append({
                        'row_id': row['row_id'],
                        'name': row['name'],
                        'timestamp': timestamp
                    })
                except:
                    pass
        
        # Find orphaned files close to known activity
        for known in known_timestamps:
            time_window = timedelta(hours=24)  # Within 24 hours
            nearby_files = []
            
            for file in orphaned_files:
                file_time = file['modified'].replace(tzinfo=None)
                known_time = known['timestamp'].replace(tzinfo=None)
                
                if abs(file_time - known_time) <= time_window:
                    nearby_files.append(file)
            
            if nearby_files:
                print(f"  Near {known['name']} ({known['row_id']}) activity: {len(nearby_files)} files")
                for file in nearby_files[:2]:
                    print(f"    - {file['filename']} ({file['modified']})")
    
    def _check_migration_reports(self, orphaned_files):
        """Check migration report files for clues"""
        print(f"\n📋 3. MIGRATION REPORT ANALYSIS")
        print("-" * 40)
        
        migration_files = [
            'data_import_report_20250713_205502.json',
            'data_import_report_20250713_205537.json',
            'pipeline_state_full_ingestion_20250712_184412.json',
            'sam_torode_pipeline_complete.json'
        ]
        
        for report_file in migration_files:
            if Path(report_file).exists():
                print(f"\n📄 Analyzing: {report_file}")
                try:
                    with open(report_file, 'r') as f:
                        data = json.load(f)
                    
                    # Look for UUID references
                    uuid_refs = self._find_uuid_references(data)
                    if uuid_refs:
                        print(f"  Found {len(uuid_refs)} UUID references")
                        for ref in uuid_refs[:3]:
                            print(f"    - {ref}")
                    else:
                        print(f"  No UUID references found")
                    
                    # Look for file references
                    file_refs = self._find_file_references(data)
                    if file_refs:
                        print(f"  Found {len(file_refs)} file references")
                        for ref in file_refs[:3]:
                            print(f"    - {ref}")
                    
                except Exception as e:
                    print(f"  Error reading file: {str(e)}")
            else:
                print(f"❌ {report_file} not found")
    
    def _find_uuid_references(self, data, path=""):
        """Recursively find UUID-like strings in data"""
        uuid_pattern = r'[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}'
        uuids = []
        
        if isinstance(data, dict):
            for key, value in data.items():
                current_path = f"{path}.{key}" if path else key
                if isinstance(value, str) and re.match(uuid_pattern, value):
                    uuids.append(f"{current_path}: {value}")
                else:
                    uuids.extend(self._find_uuid_references(value, current_path))
        elif isinstance(data, list):
            for i, item in enumerate(data):
                current_path = f"{path}[{i}]"
                uuids.extend(self._find_uuid_references(item, current_path))
        elif isinstance(data, str):
            matches = re.findall(uuid_pattern, data)
            for match in matches:
                uuids.append(f"{path}: {match}")
        
        return uuids
    
    def _find_file_references(self, data, path=""):
        """Find file references in data"""
        file_refs = []
        
        if isinstance(data, dict):
            for key, value in data.items():
                if 'file' in key.lower() or 'path' in key.lower():
                    if isinstance(value, str):
                        file_refs.append(f"{key}: {value}")
                file_refs.extend(self._find_file_references(value, f"{path}.{key}" if path else key))
        elif isinstance(data, list):
            for i, item in enumerate(data):
                file_refs.extend(self._find_file_references(item, f"{path}[{i}]"))
        
        return file_refs
    
    def _analyze_file_sizes(self, orphaned_files):
        """Analyze file sizes for patterns"""
        print(f"\n📏 4. FILE SIZE ANALYSIS")
        print("-" * 40)
        
        # Get file sizes from known clients for comparison
        df = pd.read_csv(self.csv_path)
        known_client_ids = [502, 503, 504, 506]
        
        print("🔍 Comparing with known client file sizes...")
        
        for client_id in known_client_ids:
            client_row = df[df['row_id'] == client_id]
            if not client_row.empty:
                client = client_row.iloc[0]
                print(f"\n  Client {client_id} ({client['name']}):")
                
                # Get their S3 files for size comparison
                try:
                    response = self.s3.list_objects_v2(Bucket=self.bucket, Prefix=f"{client_id}/")
                    if 'Contents' in response:
                        client_files = []
                        for obj in response['Contents']:
                            if not obj['Key'].endswith('/'):
                                client_files.append({
                                    'name': obj['Key'].split('/')[-1],
                                    'size': obj['Size']
                                })
                        
                        print(f"    Known files: {len(client_files)}")
                        for cf in client_files[:2]:
                            size_mb = cf['size'] / (1024 * 1024)
                            print(f"      - {cf['name']} ({size_mb:.1f} MB)")
                        
                        # Look for orphaned files with similar sizes
                        size_tolerance = 0.1  # 10% tolerance
                        for cf in client_files:
                            similar_orphans = []
                            for orphan in orphaned_files:
                                size_diff = abs(orphan['size'] - cf['size']) / cf['size']
                                if size_diff <= size_tolerance:
                                    similar_orphans.append(orphan)
                            
                            if similar_orphans:
                                cf_size_mb = cf['size'] / (1024 * 1024)
                                print(f"    🎯 Similar sizes to {cf['name']} ({cf_size_mb:.1f} MB): {len(similar_orphans)} orphans")
                                for so in similar_orphans[:2]:
                                    so_size_mb = so['size'] / (1024 * 1024)
                                    print(f"      → {so['filename']} ({so_size_mb:.1f} MB)")
                
                except Exception as e:
                    print(f"    Error: {str(e)}")
    
    def _check_metadata_files(self, orphaned_files):
        """Check JSON metadata files for clues"""
        print(f"\n📄 5. METADATA FILE ANALYSIS")
        print("-" * 40)
        
        json_files = [f for f in orphaned_files if f['extension'] == '.json']
        print(f"Found {len(json_files)} JSON metadata files")
        
        for json_file in json_files[:5]:  # Check first 5
            print(f"\n🔍 Examining: {json_file['filename']}")
            try:
                # Download and examine the JSON file
                response = self.s3.get_object(Bucket=self.bucket, Key=json_file['key'])
                content = response['Body'].read().decode('utf-8')
                
                if content.strip():
                    data = json.loads(content)
                    
                    # Look for identifying information
                    interesting_keys = ['title', 'uploader', 'channel', 'filename', 'id', 'webpage_url']
                    clues = {}
                    
                    for key in interesting_keys:
                        if key in data:
                            clues[key] = str(data[key])[:100]  # Truncate long values
                    
                    if clues:
                        print(f"  📋 Metadata clues:")
                        for key, value in clues.items():
                            print(f"    {key}: {value}")
                    else:
                        print(f"  📋 Keys found: {list(data.keys())[:10]}")
                else:
                    print(f"  📋 Empty file")
                    
            except Exception as e:
                print(f"  ❌ Error reading metadata: {str(e)}")
    
    def _look_for_naming_patterns(self, orphaned_files):
        """Look for patterns in the UUID naming that might give clues"""
        print(f"\n🔤 6. UUID PATTERN ANALYSIS")
        print("-" * 40)
        
        # Group UUIDs by first few characters (might indicate generation patterns)
        uuid_prefixes = defaultdict(list)
        for file in orphaned_files:
            prefix = file['uuid'][:8]  # First 8 characters
            uuid_prefixes[prefix].append(file)
        
        # Show prefixes with multiple files (might indicate batch generation)
        print("🎯 UUID prefixes with multiple files:")
        for prefix, files in uuid_prefixes.items():
            if len(files) > 1:
                print(f"  {prefix}*: {len(files)} files")
                for file in files[:2]:
                    print(f"    - {file['filename']}")
    
    def _correlate_with_known_clients(self, orphaned_files):
        """Try to correlate with known client data"""
        print(f"\n👥 7. CLIENT CORRELATION ANALYSIS")
        print("-" * 40)
        
        df = pd.read_csv(self.csv_path)
        
        # Look for clients with YouTube/Drive data but no files
        clients_with_links = df[
            (df['youtube_playlist'].notna() & (df['youtube_playlist'] != '')) |
            (df['google_drive'].notna() & (df['google_drive'] != ''))
        ]
        
        clients_without_files = clients_with_links[
            (clients_without_files['file_uuids'].isna()) | 
            (clients_without_files['file_uuids'] == '{}')
        ]
        
        print(f"📊 Clients with links but no files: {len(clients_without_files)}")
        
        if len(clients_without_files) > 0:
            print(f"🎯 Potential orphan file owners:")
            for _, client in clients_without_files.head(10).iterrows():
                youtube_links = str(client.get('youtube_playlist', ''))[:50]
                drive_links = str(client.get('google_drive', ''))[:50]
                print(f"  Row {client['row_id']}: {client['name']}")
                if youtube_links and youtube_links != 'nan':
                    print(f"    YouTube: {youtube_links}...")
                if drive_links and drive_links != 'nan':
                    print(f"    Drive: {drive_links}...")
    
    def _generate_investigation_report(self, orphaned_files):
        """Generate detailed investigation report"""
        print(f"\n📊 8. INVESTIGATION SUMMARY")
        print("=" * 60)
        
        report = {
            'investigation_date': datetime.now().isoformat(),
            'total_orphaned_files': len(orphaned_files),
            'file_type_breakdown': {},
            'size_analysis': {},
            'timestamp_range': {
                'earliest': min(f['modified'] for f in orphaned_files).isoformat(),
                'latest': max(f['modified'] for f in orphaned_files).isoformat()
            },
            'investigation_recommendations': []
        }
        
        # File type breakdown
        by_ext = defaultdict(int)
        total_size = 0
        for file in orphaned_files:
            ext = file['extension'] or 'no_extension'
            by_ext[ext] += 1
            total_size += file['size']
        
        report['file_type_breakdown'] = dict(by_ext)
        report['size_analysis'] = {
            'total_size_mb': total_size / (1024 * 1024),
            'average_size_mb': (total_size / len(orphaned_files)) / (1024 * 1024)
        }
        
        # Recommendations
        recommendations = [
            "Check migration reports for UUID-to-client mappings",
            "Manually review JSON metadata files for ownership clues",
            "Consider file size/timestamp correlation with known clients",
            "Review files uploaded on specific dates for batch processing clues",
            "Check if orphaned files represent failed/incomplete migrations"
        ]
        report['investigation_recommendations'] = recommendations
        
        # Save report
        report_path = f"orphaned_files_investigation_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(report_path, 'w') as f:
            json.dump(report, f, indent=2, default=str)
        
        print(f"💾 Investigation report saved: {report_path}")
        
        print(f"\n🎯 KEY FINDINGS:")
        print(f"  - {len(orphaned_files)} orphaned files from {report['timestamp_range']['earliest'][:10]} to {report['timestamp_range']['latest'][:10]}")
        print(f"  - Total size: {report['size_analysis']['total_size_mb']:.1f} MB")
        print(f"  - File types: {dict(list(by_ext.items())[:3])}")
        
        print(f"\n💡 NEXT STEPS:")
        for i, rec in enumerate(recommendations[:3], 1):
            print(f"  {i}. {rec}")

def main():
    investigator = OrphanedFileInvestigator()
    investigator.investigate_all()

if __name__ == "__main__":
    main()