#!/usr/bin/env python3
"""
Map downloaded files to personality types using metadata files.
Provides multiple organization strategies to preserve type information with content.

UPDATED: Now uses CleanFileMapper to eliminate directory-based contamination.
"""

import json
import os
import glob
import shutil
import pandas as pd
from pathlib import Path
from collections import defaultdict
import argparse
from typing import Dict, List, Tuple
try:
    from utils.clean_file_mapper import CleanFileMapper
except ImportError:
    from clean_file_mapper import CleanFileMapper


class FileTypeMapper:
    """Maps downloaded files to personality types using clean mapping (no contamination)"""
    
    def __init__(self, csv_path: str = 'outputs/output.csv'):
        self.csv_path = csv_path
        self.df = pd.read_csv(csv_path)
        
        # Use CleanFileMapper to eliminate contamination
        self.clean_mapper = CleanFileMapper(csv_path)
        self.clean_mapper.map_all_files()
        
        self.file_mapping = {}
        self.unmapped_files = []
        
    def scan_metadata_files(self) -> None:
        """Build file-to-type mapping using CleanFileMapper (contamination-free)"""
        print("Building clean file mappings (no directory contamination)...")
        
        # Convert CleanFileMapper results to our format
        for file_path, mapping_info in self.clean_mapper.file_to_row.items():
            row_id = mapping_info['row_id']
            
            # Get row data from CSV (compare strings to strings)
            row_data = self.df[self.df['row_id'] == row_id]
            if not row_data.empty:
                row = row_data.iloc[0]
                self.file_mapping[file_path] = {
                    'row_id': row_id,
                    'type': row['type'],
                    'name': row['name'],
                    'metadata_path': None  # Clean mapper doesn't expose this
                }
        
        print(f"Mapped {len(self.file_mapping)} files using clean logic")
    
    def scan_csv_mappings(self) -> None:
        """Legacy method - now handled by CleanFileMapper"""
        print("CSV mappings already handled by CleanFileMapper")
        # CleanFileMapper already processed CSV listings with proper separator handling
    
    def find_unmapped_files(self) -> None:
        """Identify files that couldn't be mapped to types"""
        # Use CleanFileMapper's unmapped files
        self.unmapped_files = self.clean_mapper.unmapped_files.copy()
        print(f"Found {len(self.unmapped_files)} unmapped files")
    
    def create_type_organized_structure(self, output_dir: str = 'organized_by_type') -> None:
        """Create directory structure organized by personality type"""
        print(f"\nCreating type-organized structure in {output_dir}/")
        
        os.makedirs(output_dir, exist_ok=True)
        
        # Create summary report
        summary = []
        
        for file_path, info in self.file_mapping.items():
            if not os.path.exists(file_path):
                continue
                
            # Clean type for directory name
            type_dir = info['type'].replace('/', '-').replace(' ', '_').replace('#', 'num')
            person_dir = f"{info['row_id']}_{info['name'].replace(' ', '_')}"
            
            # Create nested directory structure
            dest_dir = os.path.join(output_dir, type_dir, person_dir)
            os.makedirs(dest_dir, exist_ok=True)
            
            # Copy file with preserved name
            dest_file = os.path.join(dest_dir, os.path.basename(file_path))
            
            if not os.path.exists(dest_file):
                shutil.copy2(file_path, dest_file)
                
            summary.append({
                'original_path': file_path,
                'new_path': dest_file,
                'row_id': info['row_id'],
                'type': info['type'],
                'name': info['name']
            })
        
        # Save mapping summary
        summary_df = pd.DataFrame(summary)
        summary_df.to_csv(os.path.join(output_dir, 'file_mapping_summary.csv'), index=False)
        print(f"Organized {len(summary)} files by type")
        
    def add_type_to_filenames(self, dry_run: bool = True) -> None:
        """Rename files to include type information"""
        print(f"\n{'[DRY RUN] ' if dry_run else ''}Adding type information to filenames...")
        
        renamed_count = 0
        
        for file_path, info in self.file_mapping.items():
            if not os.path.exists(file_path):
                continue
                
            # Skip if already has type in name
            if any(x in os.path.basename(file_path) for x in ['FF-', 'FM-', 'MF-', 'MM-']):
                continue
            
            # Create new filename with type
            dir_path = os.path.dirname(file_path)
            old_name = os.path.basename(file_path)
            name_parts = os.path.splitext(old_name)
            
            # Clean type for filename
            type_clean = info['type'].replace('/', '-').replace(' ', '_').replace('#', 'num')
            new_name = f"{name_parts[0]}_row{info['row_id']}_{type_clean}{name_parts[1]}"
            new_path = os.path.join(dir_path, new_name)
            
            if dry_run:
                print(f"  Would rename: {old_name} → {new_name}")
            else:
                if not os.path.exists(new_path):
                    os.rename(file_path, new_path)
                    renamed_count += 1
                    
                    # Update CSV if needed
                    self._update_csv_filename(info['row_id'], old_name, new_name)
        
        print(f"{'Would rename' if dry_run else 'Renamed'} {renamed_count} files")
    
    def _update_csv_filename(self, row_id: str, old_name: str, new_name: str) -> None:
        """Update filename in CSV after renaming"""
        # Implementation depends on your CSV update requirements
        pass
    
    def generate_report(self) -> None:
        """Generate comprehensive mapping report"""
        print("\n=== FILE MAPPING REPORT ===")
        
        # Type distribution
        type_counts = defaultdict(int)
        for info in self.file_mapping.values():
            type_counts[info['type']] += 1
        
        print(f"\nTotal mapped files: {len(self.file_mapping)}")
        print(f"Unmapped files: {len(self.unmapped_files)}")
        
        print("\nTop 10 personality types by file count:")
        for ptype, count in sorted(type_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
            print(f"  {ptype}: {count} files")
        
        if self.unmapped_files:
            print(f"\nSample unmapped files:")
            for file in self.unmapped_files[:5]:
                print(f"  {file}")
        
        # Save detailed report
        report_data = []
        for file_path, info in self.file_mapping.items():
            report_data.append({
                'file_path': file_path,
                'filename': os.path.basename(file_path),
                'row_id': info['row_id'],
                'type': info['type'],
                'name': info['name'],
                'has_metadata': info['metadata_path'] is not None
            })
        
        report_df = pd.DataFrame(report_data)
        report_df.to_csv('file_type_mapping_report.csv', index=False)
        print(f"\nDetailed report saved to: file_type_mapping_report.csv")


def main():
    parser = argparse.ArgumentParser(description='Map downloaded files to personality types')
    parser.add_argument('--organize', action='store_true', 
                       help='Create type-organized directory structure')
    parser.add_argument('--rename', action='store_true',
                       help='Add type information to filenames')
    parser.add_argument('--output-dir', default='organized_by_type',
                       help='Output directory for organized files')
    parser.add_argument('--csv-path', default='outputs/output.csv',
                       help='Path to CSV file')
    parser.add_argument('--dry-run', action='store_true',
                       help='Show what would be done without making changes')
    
    args = parser.parse_args()
    
    # Initialize mapper
    mapper = FileTypeMapper(args.csv_path)
    
    # Build mappings using clean logic
    print("Building file-to-type mappings using CleanFileMapper...")
    mapper.scan_metadata_files()  # Now uses CleanFileMapper internally
    mapper.scan_csv_mappings()    # Legacy - handled by CleanFileMapper
    mapper.find_unmapped_files()  # Uses CleanFileMapper results
    
    # Generate report
    mapper.generate_report()
    
    # Perform requested actions
    if args.organize:
        mapper.create_type_organized_structure(args.output_dir)
    
    if args.rename:
        mapper.add_type_to_filenames(dry_run=args.dry_run)


if __name__ == "__main__":
    main()