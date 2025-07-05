#!/usr/bin/env python3
"""
Comprehensive File Mapper - Maps ALL files to personality types and identifies issues.
Uses multiple strategies to achieve 100% file identification.

UPDATED: Now uses CleanFileMapper to eliminate directory-based contamination.
"""

import json
import os
import re
import hashlib
import glob
import shutil
import pandas as pd
from pathlib import Path
from collections import defaultdict
from datetime import datetime
from typing import Dict, List, Set, Tuple, Optional
import argparse
try:
    from utils.clean_file_mapper import CleanFileMapper
except ImportError:
    from clean_file_mapper import CleanFileMapper


class ComprehensiveFileMapper:
    """Complete file analysis and mapping system (contamination-free)"""
    
    def __init__(self, csv_path: str = 'outputs/output.csv'):
        self.csv_path = csv_path
        self.df = pd.read_csv(csv_path)
        
        # Use CleanFileMapper to eliminate contamination
        self.clean_mapper = CleanFileMapper(csv_path)
        self.clean_mapper.map_all_files()
        
        # File categories
        self.mapped_files = {}          # file_path -> mapping_info
        self.unmapped_files = []        # files with no mapping
        self.duplicate_files = defaultdict(list)  # hash -> [file_paths]
        self.temporary_files = []       # .part, .ytdl, etc.
        self.orphaned_csv_entries = []  # in CSV but not on disk
        self.missing_metadata = []      # files without metadata
        
        # Mapping sources tracking
        self.mapping_sources = defaultdict(int)
        
        # File hashes for duplicate detection
        self.file_hashes = {}
        
    def scan_all_files(self) -> None:
        """Scan all files in download directories"""
        print("=== PHASE 1: COMPREHENSIVE FILE SCAN ===")
        
        all_files = []
        for pattern in ['youtube_downloads/**/*', 'drive_downloads/**/*']:
            all_files.extend(glob.glob(pattern, recursive=True))
        
        total_files = len([f for f in all_files if os.path.isfile(f)])
        print(f"Found {total_files} total files to analyze")
        
        # Categorize files
        for file_path in all_files:
            if not os.path.isfile(file_path):
                continue
                
            basename = os.path.basename(file_path)
            
            # Check for temporary files
            if any(basename.endswith(ext) for ext in ['.part', '.ytdl', '.tmp', '.temp']):
                self.temporary_files.append(file_path)
                continue
            
            # Skip metadata files for now (will process separately)
            if basename.endswith('metadata.json'):
                continue
                
            # Calculate file hash for duplicate detection (first 1MB only for speed)
            file_hash = self._calculate_file_hash(file_path)
            if file_hash:
                self.file_hashes[file_path] = file_hash
                self.duplicate_files[file_hash].append(file_path)
    
    def map_from_metadata(self) -> None:
        """Strategy 1: Use CleanFileMapper results (contamination-free)"""
        print("\n=== STRATEGY 1: CLEAN MAPPING (NO CONTAMINATION) ===")
        
        print(f"Using CleanFileMapper results: {len(self.clean_mapper.file_to_row)} files")
        
        # Convert CleanFileMapper results to our format
        mapped_count = 0
        for file_path, mapping_info in self.clean_mapper.file_to_row.items():
            row_id = mapping_info['row_id']
            source = mapping_info['source']
            
            # Get row data from CSV
            row_data = self.df[self.df['row_id'] == row_id]
            if not row_data.empty:
                row = row_data.iloc[0]
                self._add_mapping(file_path, {
                    'row_id': row_id,
                    'type': row['type'],
                    'name': row['name'],
                    'email': row.get('email', 'unknown'),
                    'source': f'clean_{source}',  # Mark as clean mapping
                    'metadata_path': None
                })
                mapped_count += 1
        
        print(f"  Mapped {mapped_count} files using clean logic")
        self.mapping_sources['clean_mapping'] = mapped_count
    
    def map_from_csv(self) -> None:
        """Strategy 2: CSV mapping already handled by CleanFileMapper"""
        print("\n=== STRATEGY 2: CSV MAPPING ===")
        print("  CSV mapping already handled by CleanFileMapper in Strategy 1")
        print("  (Includes robust comma/semicolon separator handling)")
    
    def map_from_filename_patterns(self) -> None:
        """Strategy 3: Map files using filename patterns"""
        print("\n=== STRATEGY 3: FILENAME PATTERN MAPPING ===")
        
        mapped_count = 0
        
        # Pattern 1: Files with row ID in name
        row_pattern = re.compile(r'_row(\d+)_')
        
        # Pattern 2: Files with type in name
        type_patterns = [
            re.compile(r'(FF-[^_/]+)'),
            re.compile(r'(FM-[^_/]+)'),
            re.compile(r'(MF-[^_/]+)'),
            re.compile(r'(MM-[^_/]+)')
        ]
        
        for file_path in self.file_hashes.keys():
            if file_path in self.mapped_files:
                continue
                
            basename = os.path.basename(file_path)
            
            # Try to extract row ID
            row_match = row_pattern.search(basename)
            if row_match:
                row_id = row_match.group(1)
                
                # Find corresponding CSV row
                csv_row = self.df[self.df['row_id'] == row_id]
                if not csv_row.empty:
                    row_data = csv_row.iloc[0]
                    self._add_mapping(file_path, {
                        'row_id': row_id,
                        'type': row_data['type'],
                        'name': row_data['name'],
                        'email': row_data['email'],
                        'source': 'filename_rowid',
                        'pattern': f'row{row_id}'
                    })
                    mapped_count += 1
                    continue
            
            # Try to extract type
            for type_pattern in type_patterns:
                type_match = type_pattern.search(basename)
                if type_match:
                    personality_type = type_match.group(1)
                    
                    # Find matching rows by type
                    matching_rows = self.df[self.df['type'].str.contains(personality_type, na=False)]
                    if len(matching_rows) == 1:
                        # Unique match
                        row_data = matching_rows.iloc[0]
                        self._add_mapping(file_path, {
                            'row_id': row_data['row_id'],
                            'type': row_data['type'],
                            'name': row_data['name'],
                            'email': row_data['email'],
                            'source': 'filename_type',
                            'pattern': personality_type
                        })
                        mapped_count += 1
                        
        print(f"  Mapped {mapped_count} files from filename patterns")
    
    def map_by_content_matching(self) -> None:
        """Strategy 4: Map files by matching content IDs"""
        print("\n=== STRATEGY 4: CONTENT ID MATCHING ===")
        
        mapped_count = 0
        
        # YouTube video ID pattern
        youtube_pattern = re.compile(r'([a-zA-Z0-9_-]{11})(?:\.|_)')
        
        # Drive file ID pattern  
        drive_pattern = re.compile(r'([a-zA-Z0-9_-]{28,33})(?:\.|_)')
        
        for file_path in self.file_hashes.keys():
            if file_path in self.mapped_files:
                continue
                
            basename = os.path.basename(file_path)
            
            # Check YouTube pattern
            if 'youtube' in file_path:
                match = youtube_pattern.search(basename)
                if match:
                    video_id = match.group(1)
                    
                    # Search in youtube_media_id column
                    matching_rows = self.df[self.df['youtube_media_id'] == video_id]
                    if not matching_rows.empty:
                        row_data = matching_rows.iloc[0]
                        self._add_mapping(file_path, {
                            'row_id': row_data['row_id'],
                            'type': row_data['type'],
                            'name': row_data['name'],
                            'email': row_data['email'],
                            'source': 'youtube_id',
                            'media_id': video_id
                        })
                        mapped_count += 1
                        continue
                    
                    # Search in youtube_files text
                    for idx, row in self.df.iterrows():
                        if pd.notna(row.get('youtube_files')) and video_id in str(row['youtube_files']):
                            self._add_mapping(file_path, {
                                'row_id': row['row_id'],
                                'type': row['type'],
                                'name': row['name'],
                                'email': row['email'],
                                'source': 'youtube_id_in_files',
                                'media_id': video_id
                            })
                            mapped_count += 1
                            break
            
            # Check Drive pattern
            elif 'drive' in file_path:
                match = drive_pattern.search(basename)
                if match:
                    drive_id = match.group(1)
                    
                    # Search in drive_media_id column
                    matching_rows = self.df[self.df['drive_media_id'] == drive_id]
                    if not matching_rows.empty:
                        row_data = matching_rows.iloc[0]
                        self._add_mapping(file_path, {
                            'row_id': row_data['row_id'],
                            'type': row_data['type'],
                            'name': row_data['name'],
                            'email': row_data['email'],
                            'source': 'drive_id',
                            'media_id': drive_id
                        })
                        mapped_count += 1
                        
        print(f"  Mapped {mapped_count} files by content ID")
    
    def identify_unmapped_and_issues(self) -> None:
        """Identify remaining unmapped files and data issues"""
        print("\n=== PHASE 2: ISSUE IDENTIFICATION ===")
        
        # Identify unmapped files
        for file_path in self.file_hashes.keys():
            if file_path not in self.mapped_files:
                self.unmapped_files.append(file_path)
        
        # Check for orphaned CSV entries
        print("Checking for orphaned CSV entries...")
        
        for idx, row in self.df.iterrows():
            orphaned = False
            
            # Check YouTube files
            if pd.notna(row.get('youtube_files')) and row.get('youtube_status') == 'completed':
                files = str(row['youtube_files']).split(';')
                files_found = 0
                for file in files:
                    file = file.strip()
                    if file:
                        # Check if file exists anywhere
                        found = any(file in fp for fp in self.file_hashes.keys())
                        if found:
                            files_found += 1
                
                if files_found == 0:
                    self.orphaned_csv_entries.append({
                        'row_id': row['row_id'],
                        'name': row['name'],
                        'type': row['type'],
                        'field': 'youtube_files',
                        'files': row['youtube_files']
                    })
            
            # Check Drive files
            if pd.notna(row.get('drive_files')) and row.get('drive_status') == 'completed':
                files = str(row['drive_files']).split(',')
                files_found = 0
                for file in files:
                    file = file.strip()
                    if file:
                        found = any(file in fp for fp in self.file_hashes.keys())
                        if found:
                            files_found += 1
                
                if files_found == 0:
                    self.orphaned_csv_entries.append({
                        'row_id': row['row_id'],
                        'name': row['name'],
                        'type': row['type'],
                        'field': 'drive_files',
                        'files': row['drive_files']
                    })
        
        # Identify files without metadata
        for file_path in self.mapped_files:
            if self.mapped_files[file_path].get('source') != 'metadata':
                metadata_exists = any(
                    os.path.exists(os.path.join(os.path.dirname(file_path), f))
                    for f in ['metadata.json', '*_metadata.json']
                )
                if not metadata_exists:
                    self.missing_metadata.append(file_path)
    
    def generate_comprehensive_report(self) -> None:
        """Generate detailed analysis report"""
        print("\n=== COMPREHENSIVE MAPPING REPORT ===")
        
        total_files = len(self.file_hashes)
        
        print(f"\nFILE STATISTICS:")
        print(f"  Total files scanned: {total_files}")
        print(f"  Successfully mapped: {len(self.mapped_files)} ({len(self.mapped_files)/total_files*100:.1f}%)")
        print(f"  Unmapped files: {len(self.unmapped_files)} ({len(self.unmapped_files)/total_files*100:.1f}%)")
        print(f"  Temporary files: {len(self.temporary_files)}")
        
        # Mapping source breakdown
        print(f"\nMAPPING SOURCES:")
        source_counts = defaultdict(int)
        for info in self.mapped_files.values():
            source_counts[info['source']] += 1
        for source, count in sorted(source_counts.items(), key=lambda x: x[1], reverse=True):
            print(f"  {source}: {count} files")
        
        # Duplicate analysis
        print(f"\nDUPLICATE ANALYSIS:")
        duplicate_sets = [files for files in self.duplicate_files.values() if len(files) > 1]
        print(f"  Duplicate sets found: {len(duplicate_sets)}")
        total_duplicates = sum(len(files) - 1 for files in duplicate_sets)
        print(f"  Total duplicate files: {total_duplicates}")
        
        if duplicate_sets:
            print("  Sample duplicates:")
            for files in duplicate_sets[:3]:
                print(f"    - {os.path.basename(files[0])}")
                for dup in files[1:]:
                    print(f"      = {dup}")
        
        # Data integrity issues
        print(f"\nDATA INTEGRITY ISSUES:")
        print(f"  Orphaned CSV entries: {len(self.orphaned_csv_entries)}")
        print(f"  Files without metadata: {len(self.missing_metadata)}")
        
        # Unmapped file analysis
        if self.unmapped_files:
            print(f"\nUNMAPPED FILE ANALYSIS:")
            
            # Categorize unmapped files
            unmapped_categories = defaultdict(list)
            for file_path in self.unmapped_files:
                basename = os.path.basename(file_path)
                if re.match(r'^[a-zA-Z0-9_-]{11}\.(mp4|webm|mkv)', basename):
                    unmapped_categories['youtube_videos'].append(file_path)
                elif re.match(r'^[a-zA-Z0-9_-]{28,33}\.', basename):
                    unmapped_categories['drive_files'].append(file_path)
                elif basename.endswith('.vtt') or basename.endswith('.srt'):
                    unmapped_categories['transcripts'].append(file_path)
                else:
                    unmapped_categories['other'].append(file_path)
            
            for category, files in unmapped_categories.items():
                print(f"  {category}: {len(files)} files")
                for file in files[:2]:
                    print(f"    - {os.path.basename(file)}")
        
        # Save detailed reports
        self._save_detailed_reports()
    
    def _add_mapping(self, file_path: str, mapping_info: Dict) -> None:
        """Add a file mapping"""
        if file_path not in self.mapped_files:
            self.mapped_files[file_path] = mapping_info
            self.mapping_sources[mapping_info['source']] += 1
    
    def _calculate_file_hash(self, file_path: str, chunk_size: int = 1024*1024) -> Optional[str]:
        """Calculate file hash (first 1MB for speed)"""
        try:
            hasher = hashlib.md5()
            with open(file_path, 'rb') as f:
                chunk = f.read(chunk_size)
                hasher.update(chunk)
            return hasher.hexdigest()
        except:
            return None
    
    def _save_detailed_reports(self) -> None:
        """Save all findings to CSV files"""
        
        # Main mapping report
        mapping_data = []
        for file_path, info in self.mapped_files.items():
            mapping_data.append({
                'file_path': file_path,
                'filename': os.path.basename(file_path),
                'file_size': os.path.getsize(file_path),
                'row_id': info['row_id'],
                'personality_type': info['type'],
                'person_name': info['name'],
                'person_email': info['email'],
                'mapping_source': info['source'],
                'has_metadata': 'metadata_path' in info
            })
        
        if mapping_data:
            df_mapped = pd.DataFrame(mapping_data)
            df_mapped.to_csv('comprehensive_file_mapping.csv', index=False)
            print(f"\nMapped files saved to: comprehensive_file_mapping.csv")
        
        # Unmapped files report
        if self.unmapped_files:
            unmapped_data = []
            for file_path in self.unmapped_files:
                unmapped_data.append({
                    'file_path': file_path,
                    'filename': os.path.basename(file_path),
                    'file_size': os.path.getsize(file_path),
                    'directory': os.path.dirname(file_path),
                    'file_type': os.path.splitext(file_path)[1]
                })
            
            df_unmapped = pd.DataFrame(unmapped_data)
            df_unmapped.to_csv('unmapped_files.csv', index=False)
            print(f"Unmapped files saved to: unmapped_files.csv")
        
        # Duplicates report
        duplicate_data = []
        for file_hash, files in self.duplicate_files.items():
            if len(files) > 1:
                for i, file_path in enumerate(files):
                    duplicate_data.append({
                        'duplicate_set': file_hash[:8],
                        'file_path': file_path,
                        'filename': os.path.basename(file_path),
                        'file_size': os.path.getsize(file_path),
                        'is_primary': i == 0
                    })
        
        if duplicate_data:
            df_duplicates = pd.DataFrame(duplicate_data)
            df_duplicates.to_csv('duplicate_files.csv', index=False)
            print(f"Duplicate files saved to: duplicate_files.csv")
        
        # Orphaned entries report
        if self.orphaned_csv_entries:
            df_orphaned = pd.DataFrame(self.orphaned_csv_entries)
            df_orphaned.to_csv('orphaned_csv_entries.csv', index=False)
            print(f"Orphaned CSV entries saved to: orphaned_csv_entries.csv")
        
        # Summary statistics
        summary = {
            'total_files': len(self.file_hashes),
            'mapped_files': len(self.mapped_files),
            'unmapped_files': len(self.unmapped_files),
            'temporary_files': len(self.temporary_files),
            'duplicate_sets': len([f for f in self.duplicate_files.values() if len(f) > 1]),
            'orphaned_csv_entries': len(self.orphaned_csv_entries),
            'files_without_metadata': len(self.missing_metadata),
            'mapping_success_rate': f"{len(self.mapped_files)/len(self.file_hashes)*100:.1f}%"
        }
        
        with open('mapping_summary.json', 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"Summary statistics saved to: mapping_summary.json")


def main():
    parser = argparse.ArgumentParser(description='Comprehensive file mapping and analysis')
    parser.add_argument('--csv-path', default='outputs/output.csv',
                       help='Path to CSV file')
    parser.add_argument('--fix-unmapped', action='store_true',
                       help='Attempt to fix unmapped files')
    parser.add_argument('--clean-duplicates', action='store_true',
                       help='Remove duplicate files (keep primary)')
    
    args = parser.parse_args()
    
    # Run comprehensive mapping
    mapper = ComprehensiveFileMapper(args.csv_path)
    
    # Phase 1: Scan all files
    mapper.scan_all_files()
    
    # Apply all mapping strategies
    mapper.map_from_metadata()
    mapper.map_from_csv()
    mapper.map_from_filename_patterns()
    mapper.map_by_content_matching()
    
    # Phase 2: Identify issues
    mapper.identify_unmapped_and_issues()
    
    # Generate comprehensive report
    mapper.generate_comprehensive_report()
    
    # Optional: Fix issues
    if args.fix_unmapped:
        print("\n=== ATTEMPTING TO FIX UNMAPPED FILES ===")
        # Implementation for fixing unmapped files
        
    if args.clean_duplicates:
        print("\n=== CLEANING DUPLICATE FILES ===")
        # Implementation for cleaning duplicates


class FileMapper:
    """
    Unified file mapping interface that consolidates all mapping functionality.
    
    This class absorbs functionality from:
    - clean_file_mapper.py (core engine)
    - create_definitive_mapping.py (authoritative mapping)
    - map_files_to_types.py (type-based organization)
    - csv_file_integrity_mapper.py (integrity checking)
    
    Usage:
        # Comprehensive mapping (default)
        mapper = FileMapper()
        results = mapper.map_files()
        
        # Specific mode mapping
        mapper = FileMapper(mode='definitive')
        results = mapper.map_files()
        
        # Type-based organization
        mapper = FileMapper(mode='type_mapping')
        results = mapper.organize_by_type()
        
        # Integrity checking
        mapper = FileMapper(mode='integrity')
        results = mapper.check_integrity()
    """
    
    def __init__(self, csv_path: str = 'outputs/output.csv', mode: str = 'comprehensive'):
        """
        Initialize unified file mapper
        
        Args:
            csv_path: Path to CSV file
            mode: Mapping mode ('comprehensive', 'clean', 'definitive', 'type_mapping', 'integrity')
        """
        self.csv_path = csv_path
        self.mode = mode
        self.df = safe_csv_read(csv_path, 'basic')  # Use standardized CSV reading
        
        # Initialize base mappers
        self.clean_mapper = CleanFileMapper(csv_path)
        self.comprehensive_mapper = ComprehensiveFileMapper(csv_path)
        
        # Results storage
        self.results = {}
        
    def map_files(self) -> Dict:
        """
        Execute file mapping based on selected mode
        
        Returns:
            Dictionary with mapping results
        """
        if self.mode == 'comprehensive':
            return self._map_comprehensive()
        elif self.mode == 'clean':
            return self._map_clean()
        elif self.mode == 'definitive':
            return self._map_definitive()
        elif self.mode == 'type_mapping':
            return self._map_by_type()
        elif self.mode == 'integrity':
            return self._check_integrity()
        else:
            raise ValueError(f"Unknown mapping mode: {self.mode}")
    
    def _map_comprehensive(self) -> Dict:
        """Comprehensive mapping using ComprehensiveFileMapper"""
        self.comprehensive_mapper.scan_all_files()
        self.comprehensive_mapper.map_files_to_csv()
        self.comprehensive_mapper.identify_issues()
        
        return {
            'mapped_files': self.comprehensive_mapper.mapped_files,
            'unmapped_files': self.comprehensive_mapper.unmapped_files,
            'duplicate_files': dict(self.comprehensive_mapper.duplicate_files),
            'orphaned_csv_entries': self.comprehensive_mapper.orphaned_csv_entries,
            'mapping_sources': dict(self.comprehensive_mapper.mapping_sources),
            'mode': 'comprehensive'
        }
    
    def _map_clean(self) -> Dict:
        """Clean mapping using CleanFileMapper (core engine)"""
        self.clean_mapper.map_all_files()
        
        return {
            'file_to_row': self.clean_mapper.file_to_row,
            'row_to_files': self.clean_mapper.row_to_files,
            'unmapped_files': self.clean_mapper.unmapped_files,
            'mapping_issues': self.clean_mapper.mapping_issues,
            'mode': 'clean'
        }
    
    def _map_definitive(self) -> Dict:
        """Definitive mapping (from create_definitive_mapping.py)"""
        # Build definitive mapping using CleanFileMapper as base
        definitive_mapping = {}
        mapped_files = set()
        
        # Use CleanFileMapper results
        self.clean_mapper.map_all_files()
        
        for file_path, mapping_info in self.clean_mapper.file_to_row.items():
            row_id = mapping_info['row_id']
            
            if row_id not in definitive_mapping:
                # Get row data from CSV
                row_data = self.df[self.df['row_id'] == row_id]
                if not row_data.empty:
                    row = row_data.iloc[0]
                    definitive_mapping[row_id] = {
                        'files': [],
                        'metadata': {
                            'name': row['name'],
                            'type': row['type'],
                            'email': row.get('email', 'unknown')
                        }
                    }
            
            if row_id in definitive_mapping:
                definitive_mapping[row_id]['files'].append(file_path)
                mapped_files.add(file_path)
        
        return {
            'definitive_mapping': definitive_mapping,
            'mapped_files': mapped_files,
            'total_rows_with_files': len(definitive_mapping),
            'mode': 'definitive'
        }
    
    def _map_by_type(self) -> Dict:
        """Type-based mapping (from map_files_to_types.py)"""
        # Build type-based mapping
        file_mapping = defaultdict(list)
        unmapped_files = []
        
        # Use CleanFileMapper results
        self.clean_mapper.map_all_files()
        
        for file_path, mapping_info in self.clean_mapper.file_to_row.items():
            row_id = mapping_info['row_id']
            
            # Get personality type from CSV
            row_data = self.df[self.df['row_id'] == row_id]
            if not row_data.empty:
                personality_type = row_data.iloc[0]['type']
                file_mapping[personality_type].append({
                    'file_path': file_path,
                    'row_id': row_id,
                    'name': row_data.iloc[0]['name'],
                    'source': mapping_info['source']
                })
            else:
                unmapped_files.append(file_path)
        
        return {
            'type_mapping': dict(file_mapping),
            'unmapped_files': unmapped_files,
            'personality_types': list(file_mapping.keys()),
            'mode': 'type_mapping'
        }
    
    def _check_integrity(self) -> Dict:
        """Integrity checking (from csv_file_integrity_mapper.py)"""
        # Check CSV-to-file integrity
        integrity_issues = []
        rows_with_files = 0
        rows_missing_files = 0
        orphaned_files = []
        
        # Use CleanFileMapper results
        self.clean_mapper.map_all_files()
        
        # Check each CSV row
        for idx, row in self.df.iterrows():
            row_id = row['row_id']
            has_youtube = pd.notna(row.get('youtube_playlist'))
            has_drive = pd.notna(row.get('google_drive'))
            
            if has_youtube or has_drive:
                # Should have files
                files_for_row = [fp for fp, info in self.clean_mapper.file_to_row.items() 
                               if info['row_id'] == row_id]
                
                if files_for_row:
                    rows_with_files += 1
                else:
                    rows_missing_files += 1
                    integrity_issues.append({
                        'type': 'missing_files',
                        'row_id': row_id,
                        'name': row['name'],
                        'issue': 'Row has links but no downloaded files'
                    })
        
        # Check for orphaned files
        all_csv_row_ids = set(self.df['row_id'].astype(str))
        for file_path, mapping_info in self.clean_mapper.file_to_row.items():
            if mapping_info['row_id'] not in all_csv_row_ids:
                orphaned_files.append(file_path)
                integrity_issues.append({
                    'type': 'orphaned_file',
                    'file_path': file_path,
                    'row_id': mapping_info['row_id'],
                    'issue': 'File mapped to non-existent CSV row'
                })
        
        return {
            'integrity_issues': integrity_issues,
            'rows_with_files': rows_with_files,
            'rows_missing_files': rows_missing_files,
            'orphaned_files': orphaned_files,
            'integrity_score': rows_with_files / (rows_with_files + rows_missing_files) if (rows_with_files + rows_missing_files) > 0 else 1.0,
            'mode': 'integrity'
        }
    
    def organize_by_type(self, output_dir: str = 'organized_by_type', copy_files: bool = True) -> Dict:
        """
        Organize files by personality type (from organize_by_type_final.py functionality)
        
        Args:
            output_dir: Directory to organize files into
            copy_files: If True, copy files; if False, create symbolic links
            
        Returns:
            Dictionary with organization results
        """
        type_mapping = self._map_by_type()
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        
        organized_count = 0
        type_counts = defaultdict(int)
        
        for personality_type, files in type_mapping['type_mapping'].items():
            # Clean personality type for directory name
            clean_type = str(personality_type).replace('/', '-').replace(' ', '_')
            type_dir = os.path.join(output_dir, clean_type)
            os.makedirs(type_dir, exist_ok=True)
            
            for file_info in files:
                file_path = file_info['file_path']
                if os.path.exists(file_path):
                    try:
                        target_path = os.path.join(type_dir, os.path.basename(file_path))
                        
                        if copy_files:
                            shutil.copy2(file_path, target_path)
                        else:
                            if not os.path.exists(target_path):
                                os.symlink(os.path.abspath(file_path), target_path)
                        
                        organized_count += 1
                        type_counts[clean_type] += 1
                        
                    except Exception as e:
                        print(f"Error organizing {file_path}: {e}")
        
        return {
            'organized_count': organized_count,
            'type_counts': dict(type_counts),
            'output_directory': output_dir,
            'copy_mode': copy_files,
            'mode': 'organize'
        }
    
    # === ISSUE RESOLUTION (from fix_mapping_issues.py) ===
    
    def find_mapping_conflicts(self) -> List[Dict]:
        """Identify files mapped to wrong rows"""
        conflicts = []
        self.clean_mapper.map_all_files()
        
        for file_path, mapping_info in self.clean_mapper.file_to_row.items():
            row_id = mapping_info['row_id']
            
            # Check if file location matches expected download directory
            expected_dir = 'youtube_downloads' if 'youtube' in file_path else 'drive_downloads'
            
            # Get row data
            row_data = self.df[self.df['row_id'] == row_id]
            if not row_data.empty:
                row = row_data.iloc[0]
                
                # Check for potential conflicts (simplified heuristic)
                if expected_dir == 'youtube_downloads' and pd.isna(row.get('youtube_playlist')):
                    conflicts.append({
                        'file_path': file_path,
                        'mapped_row_id': row_id,
                        'conflict_type': 'youtube_file_no_playlist',
                        'row_name': row['name']
                    })
                elif expected_dir == 'drive_downloads' and pd.isna(row.get('google_drive')):
                    conflicts.append({
                        'file_path': file_path,
                        'mapped_row_id': row_id,
                        'conflict_type': 'drive_file_no_drive_link',
                        'row_name': row['name']
                    })
        
        return conflicts
    
    def fix_orphaned_files(self) -> Dict:
        """Resolve files without proper CSV mappings"""
        self.clean_mapper.map_all_files()
        unmapped_files = self.clean_mapper.unmapped_files.copy()
        
        fixed_count = 0
        fixed_mappings = []
        
        for file_path in unmapped_files:
            basename = os.path.basename(file_path)
            
            # Try to match by row ID in filename
            row_match = re.search(r'row[_-]?(\d+)', basename, re.IGNORECASE)
            if row_match:
                row_id = row_match.group(1)
                
                # Check if row exists in CSV
                row_data = self.df[self.df['row_id'] == row_id]
                if not row_data.empty:
                    row = row_data.iloc[0]
                    fixed_mappings.append({
                        'file_path': file_path,
                        'row_id': row_id,
                        'name': row['name'],
                        'type': row['type'],
                        'fix_method': 'row_id_in_filename'
                    })
                    fixed_count += 1
        
        return {
            'fixed_count': fixed_count,
            'fixed_mappings': fixed_mappings,
            'remaining_unmapped': len(unmapped_files) - fixed_count
        }
    
    def clean_duplicate_files(self, action: str = 'move') -> Dict:
        """Remove or move duplicate files"""
        # Find duplicates by calculating file hashes
        file_hashes = {}
        duplicate_sets = defaultdict(list)
        
        # Get all mapped files
        self.clean_mapper.map_all_files()
        all_files = list(self.clean_mapper.file_to_row.keys())
        
        # Calculate hashes
        for file_path in all_files:
            if os.path.exists(file_path):
                try:
                    hasher = hashlib.md5()
                    with open(file_path, 'rb') as f:
                        # Read first 1MB for speed
                        chunk = f.read(1024 * 1024)
                        hasher.update(chunk)
                    file_hash = hasher.hexdigest()
                    
                    file_hashes[file_path] = file_hash
                    duplicate_sets[file_hash].append(file_path)
                except Exception:
                    continue
        
        # Process duplicates
        removed_count = 0
        duplicate_info = []
        
        for file_hash, files in duplicate_sets.items():
            if len(files) > 1:
                # Keep first file, handle others
                primary_file = files[0]
                duplicates = files[1:]
                
                for dup_file in duplicates:
                    if action == 'move':
                        # Move to duplicates directory
                        dup_dir = 'removed_duplicates'
                        os.makedirs(dup_dir, exist_ok=True)
                        dest_path = os.path.join(dup_dir, os.path.basename(dup_file))
                        
                        if os.path.exists(dup_file):
                            shutil.move(dup_file, dest_path)
                            removed_count += 1
                    
                    duplicate_info.append({
                        'primary_file': primary_file,
                        'duplicate_file': dup_file,
                        'action': action,
                        'file_hash': file_hash[:8]
                    })
        
        return {
            'removed_count': removed_count,
            'duplicate_sets': len([s for s in duplicate_sets.values() if len(s) > 1]),
            'duplicate_info': duplicate_info,
            'action': action
        }
    
    def validate_mappings(self) -> Dict:
        """Comprehensive mapping validation"""
        self.clean_mapper.map_all_files()
        
        validation_results = {
            'total_files_mapped': len(self.clean_mapper.file_to_row),
            'total_unmapped_files': len(self.clean_mapper.unmapped_files),
            'mapping_conflicts': self.find_mapping_conflicts(),
            'orphaned_files': self.fix_orphaned_files(),
            'duplicate_analysis': self.clean_duplicate_files(action='analyze'),
            'integrity_check': self._check_integrity()
        }
        
        # Calculate overall mapping quality score
        total_files = validation_results['total_files_mapped'] + validation_results['total_unmapped_files']
        mapping_rate = validation_results['total_files_mapped'] / total_files if total_files > 0 else 0
        
        validation_results['mapping_quality_score'] = {
            'mapping_rate': mapping_rate,
            'conflict_rate': len(validation_results['mapping_conflicts']) / total_files if total_files > 0 else 0,
            'overall_score': mapping_rate * (1 - len(validation_results['mapping_conflicts']) / total_files) if total_files > 0 else 0
        }
        
        return validation_results
    
    # === FILE RECOVERY (from recover_unmapped_files.py) ===
    
    def extract_video_id(self, filename: str) -> Optional[str]:
        """Extract YouTube video ID from filename"""
        pattern = r'([a-zA-Z0-9_-]{11})(?:\.|_)'
        match = re.search(pattern, filename)
        return match.group(1) if match else None
    
    def extract_drive_id(self, filename: str) -> Optional[str]:
        """Extract Google Drive file ID from filename"""
        pattern = r'([a-zA-Z0-9_-]{28,33})(?:\.|_)'
        match = re.search(pattern, filename)
        return match.group(1) if match else None
    
    def recover_unmapped_files(self, recovery_strategy: str = 'id_matching') -> Dict:
        """Attempt to map previously unmapped files"""
        self.clean_mapper.map_all_files()
        unmapped_files = self.clean_mapper.unmapped_files.copy()
        
        recovered_mappings = []
        
        for file_path in unmapped_files:
            if not os.path.isfile(file_path) or file_path.endswith('.json'):
                continue
            
            basename = os.path.basename(file_path)
            
            # Skip if already has mapping indicators
            if '_row' in basename or any(x in basename for x in ['FF-', 'FM-', 'MF-', 'MM-']):
                continue
            
            matched_row = None
            match_method = None
            
            if recovery_strategy == 'id_matching':
                # Try YouTube video ID matching
                if 'youtube' in file_path:
                    video_id = self.extract_video_id(basename)
                    if video_id:
                        for idx, row in self.df.iterrows():
                            if pd.notna(row.get('youtube_files')) and video_id in str(row['youtube_files']):
                                matched_row = row
                                match_method = 'youtube_id_match'
                                break
                
                # Try Drive file ID matching
                elif 'drive' in file_path:
                    drive_id = self.extract_drive_id(basename)
                    if drive_id:
                        for idx, row in self.df.iterrows():
                            if pd.notna(row.get('drive_files')) and drive_id in str(row['drive_files']):
                                matched_row = row
                                match_method = 'drive_id_match'
                                break
            
            if matched_row is not None:
                recovered_mappings.append({
                    'file_path': file_path,
                    'row_id': matched_row['row_id'],
                    'name': matched_row['name'],
                    'type': matched_row['type'],
                    'match_method': match_method
                })
        
        return {
            'recovered_count': len(recovered_mappings),
            'recovered_mappings': recovered_mappings,
            'recovery_rate': len(recovered_mappings) / len(unmapped_files) if unmapped_files else 0,
            'strategy': recovery_strategy
        }
    
    def identify_recoverable_files(self) -> List[str]:
        """Find files that can potentially be mapped"""
        self.clean_mapper.map_all_files()
        recoverable = []
        
        for file_path in self.clean_mapper.unmapped_files:
            basename = os.path.basename(file_path)
            
            # Check for recoverable patterns
            has_video_id = self.extract_video_id(basename) is not None
            has_drive_id = self.extract_drive_id(basename) is not None
            has_row_id = bool(re.search(r'row[_-]?(\d+)', basename, re.IGNORECASE))
            has_person_name = any(name.replace(' ', '_') in basename for name in self.df['name'].dropna())
            
            if any([has_video_id, has_drive_id, has_row_id, has_person_name]):
                recoverable.append(file_path)
        
        return recoverable
    
    # === ENHANCED DEFINITIVE MAPPING ===
    
    def create_definitive_mapping(self) -> Dict:
        """Create authoritative CSV-to-file mapping with metadata"""
        self.clean_mapper.map_all_files()
        
        definitive_mapping = {}
        mapped_files = set()
        missing_files = []
        
        # Initialize mapping structure for all rows
        for idx, row in self.df.iterrows():
            row_id = str(row['row_id'])
            definitive_mapping[row_id] = {
                'name': row['name'],
                'type': row['type'],
                'email': row.get('email', 'unknown'),
                'youtube_files': [],
                'drive_files': [],
                'all_files': [],
                'missing_files': []
            }
        
        # Import CleanFileMapper results
        for file_path, mapping_info in self.clean_mapper.file_to_row.items():
            row_id = mapping_info['row_id']
            
            if row_id in definitive_mapping:
                # Categorize file by type
                if 'youtube_downloads' in file_path:
                    definitive_mapping[row_id]['youtube_files'].append(file_path)
                elif 'drive_downloads' in file_path:
                    definitive_mapping[row_id]['drive_files'].append(file_path)
                
                definitive_mapping[row_id]['all_files'].append(file_path)
                mapped_files.add(file_path)
        
        # Identify missing files
        for idx, row in self.df.iterrows():
            row_id = str(row['row_id'])
            
            # Check for missing YouTube files
            if pd.notna(row.get('youtube_files')) and row.get('youtube_status') == 'completed':
                expected_files = str(row['youtube_files']).replace(';', ',').split(',')
                found_files = definitive_mapping[row_id]['youtube_files']
                
                for expected_file in expected_files:
                    expected_file = expected_file.strip()
                    if expected_file and not any(expected_file in found for found in found_files):
                        definitive_mapping[row_id]['missing_files'].append(('youtube', expected_file))
                        missing_files.append({
                            'row_id': row_id,
                            'name': row['name'],
                            'file': expected_file,
                            'type': 'youtube'
                        })
            
            # Check for missing Drive files
            if pd.notna(row.get('drive_files')) and row.get('drive_status') == 'completed':
                expected_files = str(row['drive_files']).replace(';', ',').split(',')
                found_files = definitive_mapping[row_id]['drive_files']
                
                for expected_file in expected_files:
                    expected_file = expected_file.strip()
                    if expected_file and not any(expected_file in found for found in found_files):
                        definitive_mapping[row_id]['missing_files'].append(('drive', expected_file))
                        missing_files.append({
                            'row_id': row_id,
                            'name': row['name'],
                            'file': expected_file,
                            'type': 'drive'
                        })
        
        return {
            'definitive_mapping': definitive_mapping,
            'mapped_files': mapped_files,
            'missing_files': missing_files,
            'total_rows_with_files': sum(1 for m in definitive_mapping.values() if m['all_files']),
            'mode': 'definitive_enhanced'
        }
    
    def create_corrected_metadata(self) -> int:
        """Generate _definitive.json metadata files"""
        definitive_result = self.create_definitive_mapping()
        definitive_mapping = definitive_result['definitive_mapping']
        
        metadata_created = 0
        
        for row_id, mapping in definitive_mapping.items():
            if not mapping['all_files']:
                continue
            
            # Create metadata for this row's files
            metadata = {
                'source_csv_row_id': int(row_id),
                'personality_type': mapping['type'],
                'person_name': mapping['name'],
                'person_email': mapping['email'],
                'mapping_source': 'definitive_csv_listing',
                'files_in_row': len(mapping['all_files'])
            }
            
            for file_path in mapping['all_files']:
                # Create definitive metadata next to each file
                metadata_path = file_path.replace(
                    os.path.splitext(file_path)[1], 
                    f'_row{row_id}_definitive.json'
                )
                
                file_metadata = metadata.copy()
                file_metadata['file_path'] = file_path
                file_metadata['file_type'] = 'youtube' if file_path in mapping['youtube_files'] else 'drive'
                
                try:
                    with open(metadata_path, 'w') as f:
                        json.dump(file_metadata, f, indent=2)
                    metadata_created += 1
                except Exception as e:
                    print(f"Error creating metadata for {file_path}: {e}")
        
        return metadata_created
    
    # === ENHANCED TYPE ORGANIZATION ===
    
    def add_type_to_filenames(self, dry_run: bool = True) -> Dict:
        """Rename files to include personality type information"""
        self.clean_mapper.map_all_files()
        
        renamed_count = 0
        rename_actions = []
        
        for file_path, mapping_info in self.clean_mapper.file_to_row.items():
            row_id = mapping_info['row_id']
            
            # Get row data
            row_data = self.df[self.df['row_id'] == row_id]
            if not row_data.empty and os.path.exists(file_path):
                row = row_data.iloc[0]
                
                # Skip if already has type in name
                basename = os.path.basename(file_path)
                if any(x in basename for x in ['FF-', 'FM-', 'MF-', 'MM-']):
                    continue
                
                # Create new filename with type
                dir_path = os.path.dirname(file_path)
                old_name = basename
                name_parts = os.path.splitext(old_name)
                
                # Clean type for filename
                type_clean = str(row['type']).replace('/', '-').replace(' ', '_').replace('#', 'num')
                new_name = f"{name_parts[0]}_row{row_id}_{type_clean}{name_parts[1]}"
                new_path = os.path.join(dir_path, new_name)
                
                rename_actions.append({
                    'old_path': file_path,
                    'new_path': new_path,
                    'old_name': old_name,
                    'new_name': new_name,
                    'row_id': row_id,
                    'type': row['type']
                })
                
                if not dry_run and not os.path.exists(new_path):
                    try:
                        os.rename(file_path, new_path)
                        renamed_count += 1
                    except Exception as e:
                        print(f"Error renaming {file_path}: {e}")
        
        return {
            'renamed_count': renamed_count,
            'total_candidates': len(rename_actions),
            'rename_actions': rename_actions,
            'dry_run': dry_run
        }
    
    # === COMPREHENSIVE REPORTING ===
    
    def generate_comprehensive_report(self) -> Dict:
        """Generate complete mapping analysis report"""
        report = {
            'clean_mapping': self._map_clean(),
            'definitive_mapping': self.create_definitive_mapping(),
            'type_mapping': self._map_by_type(),
            'integrity_check': self._check_integrity(),
            'validation_results': self.validate_mappings(),
            'recovery_analysis': self.recover_unmapped_files(),
            'timestamp': datetime.now().isoformat()
        }
        
        # Calculate summary statistics
        clean_results = report['clean_mapping']
        definitive_results = report['definitive_mapping']
        
        report['summary'] = {
            'total_files_analyzed': len(clean_results['file_to_row']) + len(clean_results['unmapped_files']),
            'successfully_mapped': len(clean_results['file_to_row']),
            'unmapped_files': len(clean_results['unmapped_files']),
            'mapping_rate': len(clean_results['file_to_row']) / (len(clean_results['file_to_row']) + len(clean_results['unmapped_files'])) if (len(clean_results['file_to_row']) + len(clean_results['unmapped_files'])) > 0 else 0,
            'rows_with_files': definitive_results['total_rows_with_files'],
            'missing_files': len(definitive_results['missing_files']),
            'personality_types': len(report['type_mapping']['personality_types']),
            'integrity_score': report['integrity_check']['integrity_score']
        }
        
        return report
    
    def export_mappings(self, format: str = 'csv', output_path: str = None) -> str:
        """Export mappings in various formats"""
        mapping_data = []
        self.clean_mapper.map_all_files()
        
        for file_path, mapping_info in self.clean_mapper.file_to_row.items():
            row_id = mapping_info['row_id']
            
            # Get row data
            row_data = self.df[self.df['row_id'] == row_id]
            if not row_data.empty:
                row = row_data.iloc[0]
                mapping_data.append({
                    'file_path': file_path,
                    'filename': os.path.basename(file_path),
                    'row_id': row_id,
                    'name': row['name'],
                    'type': row['type'],
                    'email': row.get('email', 'unknown'),
                    'mapping_source': mapping_info['source'],
                    'file_exists': os.path.exists(file_path)
                })
        
        # Set default output path
        if output_path is None:
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            output_path = f'enhanced_file_mappings_{timestamp}.{format}'
        
        # Export in requested format
        if format == 'csv':
            df_export = pd.DataFrame(mapping_data)
            df_export.to_csv(output_path, index=False)
        elif format == 'json':
            with open(output_path, 'w') as f:
                json.dump(mapping_data, f, indent=2)
        elif format == 'excel':
            df_export = pd.DataFrame(mapping_data)
            df_export.to_excel(output_path, index=False)
        else:
            raise ValueError(f"Unsupported format: {format}")
        
        return output_path


# Add import for standardized CSV reading at the top
try:
    from csv_tracker import safe_csv_read
except ImportError:
    from .csv_tracker import safe_csv_read


if __name__ == "__main__":
    main()