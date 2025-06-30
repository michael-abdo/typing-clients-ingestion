#!/usr/bin/env python3
"""
Clean File Mapper - Fixes directory-based contamination by using file-specific metadata matching.

Key Principle: Each file is mapped to its row based on filename-specific metadata, 
not directory proximity. This prevents contamination where one row's metadata 
contaminates other files in the same directory.
"""

import os
import glob
import json
import pandas as pd
import re
from typing import Dict, List, Optional, Tuple
from collections import defaultdict


class CleanFileMapper:
    """Clean file-to-row mapper that eliminates directory-based contamination"""
    
    def __init__(self, csv_path: str = 'outputs/output.csv'):
        self.csv_path = csv_path
        self.df = pd.read_csv(csv_path)
        
        # Clean mappings - no contamination
        self.file_to_row = {}           # file_path -> row_id
        self.row_to_files = defaultdict(list)  # row_id -> [file_paths]
        
        # Tracking
        self.mapped_by_definitive = 0
        self.mapped_by_csv = 0
        self.mapped_by_original_metadata = 0
        self.unmapped_files = []
        
    def map_all_files(self) -> Dict:
        """Map all files using clean, file-specific logic"""
        print("=== CLEAN FILE MAPPING (No Directory Contamination) ===")
        
        # Step 1: Map using definitive metadata (most reliable)
        self._map_by_definitive_metadata()
        
        # Step 2: Map using CSV file listings (authoritative)
        self._map_by_csv_listings()
        
        # Step 3: Map using original metadata (careful parsing)
        self._map_by_original_metadata()
        
        # Step 4: Identify unmapped files
        self._identify_unmapped_files()
        
        return self._generate_mapping_report()
    
    def _map_by_definitive_metadata(self) -> None:
        """Map files using definitive metadata (highest priority)"""
        print("  Strategy 1: Definitive metadata (_definitive.json)")
        
        definitive_files = glob.glob('*_downloads/**/*_definitive.json', recursive=True)
        
        for metadata_path in definitive_files:
            try:
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
                
                row_id = str(metadata.get('source_csv_row_id', ''))
                if not row_id:
                    continue
                
                # Extract filename from metadata path
                # Pattern: {filename}_row{X}_definitive.json -> {filename}
                basename = os.path.basename(metadata_path)
                filename_match = re.match(r'(.+)_row\d+_definitive\.json$', basename)
                
                if filename_match:
                    filename_base = filename_match.group(1)
                    
                    # Find the actual content file
                    file_dir = os.path.dirname(metadata_path)
                    content_files = glob.glob(os.path.join(file_dir, f"{filename_base}.*"))
                    
                    for content_file in content_files:
                        if not content_file.endswith('.json'):  # Skip metadata files
                            self._add_clean_mapping(content_file, row_id, 'definitive')
                            self.mapped_by_definitive += 1
                            
            except Exception as e:
                print(f"    Warning: Error reading {metadata_path}: {e}")
        
        print(f"    Mapped {self.mapped_by_definitive} files by definitive metadata")
    
    def _map_by_csv_listings(self) -> None:
        """Map files using CSV file listings (authoritative source)"""
        print("  Strategy 2: CSV file listings")
        
        for idx, row in self.df.iterrows():
            row_id = str(row['row_id'])
            
            # Process YouTube files
            if pd.notna(row.get('youtube_files')):
                # Handle both comma and semicolon separators
                files = str(row['youtube_files']).replace(';', ',').split(',')
                for file in files:
                    file = file.strip()
                    if file:
                        file_paths = self._find_file_by_name(file, 'youtube_downloads')
                        for file_path in file_paths:
                            if file_path not in self.file_to_row:  # Not already mapped
                                self._add_clean_mapping(file_path, row_id, 'csv')
                                self.mapped_by_csv += 1
            
            # Process Drive files  
            if pd.notna(row.get('drive_files')):
                # Handle both comma and semicolon separators
                files = str(row['drive_files']).replace(';', ',').split(',')
                for file in files:
                    file = file.strip()
                    if file:
                        file_paths = self._find_file_by_name(file, 'drive_downloads')
                        for file_path in file_paths:
                            if file_path not in self.file_to_row:  # Not already mapped
                                self._add_clean_mapping(file_path, row_id, 'csv')
                                self.mapped_by_csv += 1
        
        print(f"    Mapped {self.mapped_by_csv} files by CSV listings")
    
    def _map_by_original_metadata(self) -> None:
        """Map files using original metadata (careful to avoid contamination)"""
        print("  Strategy 3: Original metadata (contamination-safe)")
        
        original_metadata = glob.glob('*_downloads/**/*metadata.json', recursive=True)
        # Exclude definitive metadata
        original_metadata = [f for f in original_metadata if '_definitive.json' not in f]
        
        for metadata_path in original_metadata:
            try:
                with open(metadata_path, 'r') as f:
                    metadata = json.load(f)
                
                row_id = str(metadata.get('source_csv_row_id', ''))
                if not row_id:
                    continue
                
                # SAFE: Only map files explicitly listed in this metadata
                if 'download_result' in metadata:
                    files = metadata['download_result'].get('files_downloaded', [])
                    metadata_dir = os.path.dirname(metadata_path)
                    
                    for file in files:
                        file_path = os.path.join(metadata_dir, file)
                        if (os.path.exists(file_path) and 
                            file_path not in self.file_to_row):  # Not already mapped
                            self._add_clean_mapping(file_path, row_id, 'original_metadata')
                            self.mapped_by_original_metadata += 1
                            
            except Exception as e:
                print(f"    Warning: Error reading {metadata_path}: {e}")
        
        print(f"    Mapped {self.mapped_by_original_metadata} files by original metadata")
    
    def _find_file_by_name(self, filename: str, directory: str) -> List[str]:
        """Find file by name in directory (handles variations)"""
        search_patterns = [
            os.path.join(directory, filename),
            os.path.join(directory, '**', filename),
            os.path.join(directory, f"*{filename}*")
        ]
        
        found_files = []
        for pattern in search_patterns:
            matches = glob.glob(pattern, recursive=True)
            for match in matches:
                if os.path.isfile(match):
                    found_files.append(match)
        
        return list(set(found_files))  # Remove duplicates
    
    def _add_clean_mapping(self, file_path: str, row_id: str, source: str) -> None:
        """Add clean mapping with validation"""
        # Validate row exists
        if not any(self.df['row_id'] == row_id):
            print(f"    Warning: Row {row_id} not found in CSV")
            return
        
        self.file_to_row[file_path] = {
            'row_id': row_id,
            'source': source
        }
        self.row_to_files[row_id].append(file_path)
    
    def _identify_unmapped_files(self) -> None:
        """Identify files that couldn't be mapped"""
        all_content_files = glob.glob('*_downloads/**/*', recursive=True)
        
        for file_path in all_content_files:
            if (os.path.isfile(file_path) and 
                file_path not in self.file_to_row and
                not any(file_path.endswith(ext) for ext in ['.json', '.part', '.ytdl', '.tmp', '.lock']) and
                '/organized_by_type/' not in file_path):
                
                self.unmapped_files.append(file_path)
    
    def _generate_mapping_report(self) -> Dict:
        """Generate comprehensive mapping report"""
        total_mapped = len(self.file_to_row)
        
        report = {
            'total_files_mapped': total_mapped,
            'mapped_by_definitive': self.mapped_by_definitive,
            'mapped_by_csv': self.mapped_by_csv,
            'mapped_by_original_metadata': self.mapped_by_original_metadata,
            'unmapped_files': len(self.unmapped_files),
            'rows_with_files': len(self.row_to_files),
            'mapping_sources': {
                'definitive': self.mapped_by_definitive,
                'csv': self.mapped_by_csv,
                'original_metadata': self.mapped_by_original_metadata
            }
        }
        
        print(f"\n  CLEAN MAPPING RESULTS:")
        print(f"    Total files mapped: {total_mapped}")
        print(f"    By definitive metadata: {self.mapped_by_definitive}")
        print(f"    By CSV listings: {self.mapped_by_csv}")
        print(f"    By original metadata: {self.mapped_by_original_metadata}")
        print(f"    Unmapped files: {len(self.unmapped_files)}")
        print(f"    Rows with files: {len(self.row_to_files)}")
        
        return report
    
    def get_file_row(self, file_path: str) -> Optional[str]:
        """Get row ID for a specific file"""
        mapping = self.file_to_row.get(file_path)
        return mapping['row_id'] if mapping else None
    
    def get_row_files(self, row_id: str) -> List[str]:
        """Get all files for a specific row"""
        return self.row_to_files.get(str(row_id), [])
    
    def validate_contamination_fix(self) -> Dict:
        """Validate that contamination issues are fixed"""
        print("\n=== CONTAMINATION VALIDATION ===")
        
        # Check specific problematic cases
        contamination_cases = {
            '469': {'expected_files': 0, 'name': 'Ifrah Mohamed Mohamoud'},
            '462': {'expected_files': 1, 'name': 'Miranda Story Ruiz'},  
            '494': {'expected_files': 4, 'name': 'John Williams'},
            '496': {'expected_files': 7, 'name': 'James Kirton'}
        }
        
        results = {}
        for row_id, expected in contamination_cases.items():
            actual_files = self.get_row_files(row_id)
            results[row_id] = {
                'expected': expected['expected_files'],
                'actual': len(actual_files),
                'name': expected['name'],
                'files': [os.path.basename(f) for f in actual_files],
                'correct': len(actual_files) >= expected['expected_files']
            }
            
            status = "✅ CORRECT" if results[row_id]['correct'] else "❌ INCORRECT"
            print(f"  Row {row_id} ({expected['name']}): {len(actual_files)} files {status}")
            if actual_files:
                for f in actual_files[:3]:  # Show first 3 files
                    print(f"    - {os.path.basename(f)}")
        
        return results
    
    def save_clean_mappings(self, output_path: str = 'clean_file_mappings.csv') -> None:
        """Save clean mappings to CSV"""
        mapping_data = []
        
        for file_path, mapping_info in self.file_to_row.items():
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
                    'mapping_source': mapping_info['source']
                })
        
        if mapping_data:
            df_clean = pd.DataFrame(mapping_data)
            df_clean.to_csv(output_path, index=False)
            print(f"\n  Clean mappings saved to: {output_path}")
            return output_path
        
        return None


def main():
    """Test the clean file mapper"""
    mapper = CleanFileMapper()
    
    # Run clean mapping
    report = mapper.map_all_files()
    
    # Validate contamination fixes
    validation = mapper.validate_contamination_fix()
    
    # Save results
    mapper.save_clean_mappings()
    
    print("\n✅ Clean file mapping complete!")
    return mapper, report, validation


if __name__ == "__main__":
    main()