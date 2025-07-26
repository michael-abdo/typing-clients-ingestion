#!/usr/bin/env python3
"""
S3 Client Format Analyzer
Scans S3 bucket to identify clients with only MP3 files (requiring MP4 reprocessing)
"""

import boto3
import json
import re
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Tuple, Set
import sys
import os

class S3ClientFormatAnalyzer:
    def __init__(self, bucket_name: str = 'typing-clients-storage-2025'):
        self.bucket_name = bucket_name
        self.s3_client = boto3.client('s3', region_name='us-east-1')
        
        # Data structures for analysis
        self.client_files = defaultdict(lambda: {
            'mp3_files': [],
            'mp4_files': [],
            'other_files': [],
            'total_youtube_videos': 0,
            'folder_name': ''
        })
        
        # Pattern matching
        self.youtube_pattern = re.compile(r'youtube_([a-zA-Z0-9_-]+)\.(mp3|mp4|webm|m4a)')
        self.client_folder_pattern = re.compile(r'^(\d+)/(.+?)/')
        
    def scan_s3_bucket(self) -> None:
        """Scan entire S3 bucket and categorize files by client and format"""
        print(f"🔍 Scanning S3 bucket: {self.bucket_name}")
        print("=" * 60)
        
        try:
            paginator = self.s3_client.get_paginator('list_objects_v2')
            page_iterator = paginator.paginate(Bucket=self.bucket_name)
            
            total_objects = 0
            processed_objects = 0
            
            for page in page_iterator:
                if 'Contents' not in page:
                    continue
                    
                for obj in page['Contents']:
                    total_objects += 1
                    key = obj['Key']
                    size = obj['Size']
                    
                    # Parse client folder and file info
                    if self._process_file(key, size):
                        processed_objects += 1
                    
                    # Progress indicator
                    if total_objects % 100 == 0:
                        print(f"📊 Processed {total_objects} objects...")
            
            print(f"✅ Scan complete: {total_objects} total objects, {processed_objects} client files processed")
            
        except Exception as e:
            print(f"❌ Error scanning S3: {str(e)}")
            raise
    
    def _process_file(self, key: str, size: int) -> bool:
        """Process individual S3 object and categorize it"""
        
        # Extract client folder info
        folder_match = self.client_folder_pattern.match(key)
        if not folder_match:
            return False  # Skip non-client files
        
        row_id = folder_match.group(1)
        client_name = folder_match.group(2)
        client_key = f"{row_id}_{client_name}"
        
        # Store folder name for reference
        self.client_files[client_key]['folder_name'] = f"{row_id}/{client_name}"
        
        # Extract filename from full path
        filename = key.split('/')[-1]
        
        # Check if it's a YouTube video file
        youtube_match = self.youtube_pattern.search(filename)
        if youtube_match:
            video_id = youtube_match.group(1)
            extension = youtube_match.group(2).lower()
            
            file_info = {
                'filename': filename,
                'video_id': video_id,
                'extension': extension,
                'size_bytes': size,
                'size_mb': round(size / (1024 * 1024), 2),
                's3_key': key
            }
            
            # Categorize by format
            if extension == 'mp3':
                self.client_files[client_key]['mp3_files'].append(file_info)
            elif extension in ['mp4', 'webm']:
                self.client_files[client_key]['mp4_files'].append(file_info)
            else:
                self.client_files[client_key]['other_files'].append(file_info)
            
            self.client_files[client_key]['total_youtube_videos'] += 1
            return True
        
        else:
            # Non-YouTube files (Drive, playlists, etc.)
            file_info = {
                'filename': filename,
                'size_bytes': size,
                'size_mb': round(size / (1024 * 1024), 2),
                's3_key': key
            }
            self.client_files[client_key]['other_files'].append(file_info)
            return True
    
    def analyze_client_formats(self) -> Dict:
        """Analyze clients and categorize by format status"""
        analysis = {
            'mp3_only_clients': [],      # Need reprocessing
            'mp4_only_clients': [],      # Already correct
            'mixed_format_clients': [],  # Partial reprocessing needed
            'no_youtube_clients': [],    # No YouTube content
            'summary': {
                'total_clients': 0,
                'mp3_only_count': 0,
                'mp4_only_count': 0,
                'mixed_format_count': 0,
                'no_youtube_count': 0,
                'total_youtube_videos': 0,
                'total_mp3_videos': 0,
                'total_mp4_videos': 0
            }
        }
        
        for client_key, files in self.client_files.items():
            client_data = {
                'client_id': client_key,
                'folder_name': files['folder_name'],
                'mp3_count': len(files['mp3_files']),
                'mp4_count': len(files['mp4_files']),
                'other_count': len(files['other_files']),
                'total_youtube': files['total_youtube_videos'],
                'mp3_files': files['mp3_files'],
                'mp4_files': files['mp4_files']
            }
            
            # Categorize client
            has_mp3 = len(files['mp3_files']) > 0
            has_mp4 = len(files['mp4_files']) > 0
            has_youtube = files['total_youtube_videos'] > 0
            
            if not has_youtube:
                analysis['no_youtube_clients'].append(client_data)
            elif has_mp3 and not has_mp4:
                analysis['mp3_only_clients'].append(client_data)
            elif has_mp4 and not has_mp3:
                analysis['mp4_only_clients'].append(client_data)
            elif has_mp3 and has_mp4:
                analysis['mixed_format_clients'].append(client_data)
            
            # Update summary
            analysis['summary']['total_youtube_videos'] += files['total_youtube_videos']
            analysis['summary']['total_mp3_videos'] += len(files['mp3_files'])
            analysis['summary']['total_mp4_videos'] += len(files['mp4_files'])
        
        # Final summary counts
        analysis['summary']['total_clients'] = len(self.client_files)
        analysis['summary']['mp3_only_count'] = len(analysis['mp3_only_clients'])
        analysis['summary']['mp4_only_count'] = len(analysis['mp4_only_clients'])
        analysis['summary']['mixed_format_count'] = len(analysis['mixed_format_clients'])
        analysis['summary']['no_youtube_count'] = len(analysis['no_youtube_clients'])
        
        return analysis
    
    def generate_report(self, analysis: Dict, output_file: str = None) -> str:
        """Generate comprehensive analysis report"""
        
        report_lines = []
        
        # Header
        report_lines.append("S3 CLIENT FORMAT ANALYSIS REPORT")
        report_lines.append("=" * 60)
        report_lines.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report_lines.append(f"Bucket: {self.bucket_name}")
        report_lines.append("")
        
        # Executive Summary
        summary = analysis['summary']
        report_lines.append("📊 EXECUTIVE SUMMARY")
        report_lines.append("-" * 30)
        report_lines.append(f"Total Clients: {summary['total_clients']}")
        report_lines.append(f"Total YouTube Videos: {summary['total_youtube_videos']}")
        report_lines.append("")
        report_lines.append("Format Distribution:")
        report_lines.append(f"  🔴 MP3-Only Clients: {summary['mp3_only_count']} ({summary['total_mp3_videos']} videos)")
        report_lines.append(f"  🟢 MP4-Only Clients: {summary['mp4_only_count']} ({summary['total_mp4_videos']} videos)")
        report_lines.append(f"  🟡 Mixed Format Clients: {summary['mixed_format_count']}")
        report_lines.append(f"  ⚪ No YouTube Clients: {summary['no_youtube_count']}")
        report_lines.append("")
        
        # Critical: MP3-Only Clients (Need Reprocessing)
        if analysis['mp3_only_clients']:
            report_lines.append("🚨 CLIENTS REQUIRING MP4 REPROCESSING (MP3-ONLY)")
            report_lines.append("-" * 50)
            for client in sorted(analysis['mp3_only_clients'], key=lambda x: x['mp3_count'], reverse=True):
                report_lines.append(f"• {client['folder_name']}: {client['mp3_count']} MP3 videos")
                for mp3_file in client['mp3_files']:
                    report_lines.append(f"    - {mp3_file['filename']} ({mp3_file['size_mb']} MB)")
            report_lines.append("")
        
        # Mixed Format Clients (Partial Reprocessing)
        if analysis['mixed_format_clients']:
            report_lines.append("⚠️  CLIENTS WITH MIXED FORMATS (PARTIAL REPROCESSING)")
            report_lines.append("-" * 55)
            for client in analysis['mixed_format_clients']:
                report_lines.append(f"• {client['folder_name']}: {client['mp3_count']} MP3, {client['mp4_count']} MP4")
                if client['mp3_files']:
                    report_lines.append("    MP3 files needing reprocessing:")
                    for mp3_file in client['mp3_files']:
                        report_lines.append(f"      - {mp3_file['filename']}")
            report_lines.append("")
        
        # MP4-Only Clients (Already Correct)
        if analysis['mp4_only_clients']:
            report_lines.append("✅ CLIENTS WITH CORRECT MP4 FORMAT")
            report_lines.append("-" * 35)
            for client in analysis['mp4_only_clients'][:10]:  # Show first 10
                report_lines.append(f"• {client['folder_name']}: {client['mp4_count']} MP4 videos")
            if len(analysis['mp4_only_clients']) > 10:
                report_lines.append(f"  ... and {len(analysis['mp4_only_clients']) - 10} more clients")
            report_lines.append("")
        
        # Generate report string
        report_content = "\n".join(report_lines)
        
        # Save to file if requested
        if output_file:
            try:
                with open(output_file, 'w') as f:
                    f.write(report_content)
                print(f"📄 Report saved to: {output_file}")
            except Exception as e:
                print(f"❌ Error saving report: {str(e)}")
        
        return report_content
    
    def get_reprocessing_candidates(self, analysis: Dict) -> List[Dict]:
        """Get list of clients requiring MP4 reprocessing"""
        candidates = []
        
        # MP3-only clients (full reprocessing)
        for client in analysis['mp3_only_clients']:
            candidates.append({
                'client_id': client['client_id'],
                'folder_name': client['folder_name'],
                'reprocess_type': 'full',
                'video_count': client['mp3_count'],
                'videos': [f['video_id'] for f in client['mp3_files']]
            })
        
        # Mixed format clients (partial reprocessing)
        for client in analysis['mixed_format_clients']:
            if client['mp3_count'] > 0:
                candidates.append({
                    'client_id': client['client_id'],
                    'folder_name': client['folder_name'],
                    'reprocess_type': 'partial',
                    'video_count': client['mp3_count'],
                    'videos': [f['video_id'] for f in client['mp3_files']]
                })
        
        return candidates

def main():
    """Main execution function"""
    print("🎬 S3 Client Format Analyzer")
    print("=" * 60)
    
    # Initialize analyzer
    analyzer = S3ClientFormatAnalyzer()
    
    # Scan S3 bucket
    analyzer.scan_s3_bucket()
    
    # Analyze formats
    print("\n🔍 Analyzing client formats...")
    analysis = analyzer.analyze_client_formats()
    
    # Generate report
    print("\n📋 Generating analysis report...")
    report_file = f"s3_client_format_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.txt"
    report = analyzer.generate_report(analysis, report_file)
    
    # Print summary to console
    print("\n" + "=" * 60)
    print("📊 ANALYSIS COMPLETE")
    print("=" * 60)
    print(report[:2000] + "\n..." if len(report) > 2000 else report)
    
    # Generate reprocessing candidates
    candidates = analyzer.get_reprocessing_candidates(analysis)
    if candidates:
        candidates_file = f"reprocessing_candidates_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        with open(candidates_file, 'w') as f:
            json.dump(candidates, f, indent=2)
        print(f"\n📝 Reprocessing candidates saved to: {candidates_file}")
    
    return analysis, candidates

if __name__ == "__main__":
    try:
        analysis, candidates = main()
        print(f"\n✅ Analysis complete: {analysis['summary']['mp3_only_count']} clients need MP4 reprocessing")
    except Exception as e:
        print(f"❌ Analysis failed: {str(e)}")
        sys.exit(1)