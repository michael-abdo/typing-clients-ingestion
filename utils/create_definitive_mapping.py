#!/usr/bin/env python3
"""
DEPRECATED: Create Definitive CSV-to-File Mapping - USE comprehensive_file_mapper.FileMapper INSTEAD

This module is deprecated. All functionality has been moved to comprehensive_file_mapper.py
for better consolidation and DRY principle adherence.

Use: from utils.comprehensive_file_mapper import FileMapper
"""
import warnings
warnings.warn(
    "create_definitive_mapping.py is deprecated. Use comprehensive_file_mapper.FileMapper for definitive mapping.",
    DeprecationWarning, 
    stacklevel=2
)

import os
import glob
import json
import pandas as pd
from collections import defaultdict
from typing import Dict, List, Set, Tuple
import shutil

# Import consolidated functionality
try:
    from .comprehensive_file_mapper import FileMapper
    from .clean_file_mapper import CleanFileMapper
except ImportError:
    from comprehensive_file_mapper import FileMapper
    from clean_file_mapper import CleanFileMapper


class DefinitiveMapper:
    """Creates definitive CSV row to file mappings (contamination-free)"""
    
    def __init__(self, csv_path: str = 'outputs/output.csv'):
        self.csv_path = csv_path
        self.df = pd.read_csv(csv_path)
        
        # Use CleanFileMapper for robust mapping
        self.clean_mapper = CleanFileMapper(csv_path)
        self.clean_mapper.map_all_files()
        
        # The definitive mapping we'll build
        self.definitive_mapping = {}  # row_id -> {files: [], metadata: {}}
        
        # Track all files we've mapped
        self.mapped_files = set()
        
        # Track issues
        self.conflicts = []
        self.missing_files = []
        self.unmapped_files = []
        
    def build_definitive_mapping(self) -> None:
        """Build the definitive mapping using CleanFileMapper results"""
        print("=== BUILDING DEFINITIVE CSV-TO-FILE MAPPING ===")
        
        # Initialize mapping structure for all rows
        for idx, row in self.df.iterrows():
            row_id = str(row['row_id'])
            self.definitive_mapping[row_id] = {
                'name': row['name'],
                'type': row['type'],
                'email': row['email'],
                'youtube_files': [],
                'drive_files': [],
                'all_files': [],
                'missing_files': []
            }
        
        # Import CleanFileMapper results (contamination-free)
        print(f"Importing {len(self.clean_mapper.file_to_row)} files from CleanFileMapper...")
        
        for file_path, mapping_info in self.clean_mapper.file_to_row.items():
            row_id = mapping_info['row_id']
            
            if row_id in self.definitive_mapping:
                # Categorize file by type
                if 'youtube_downloads' in file_path:
                    self.definitive_mapping[row_id]['youtube_files'].append(file_path)
                elif 'drive_downloads' in file_path:
                    self.definitive_mapping[row_id]['drive_files'].append(file_path)
                
                self.definitive_mapping[row_id]['all_files'].append(file_path)
                self.mapped_files.add(file_path)
        
        # Use CleanFileMapper's unmapped files
        self.unmapped_files = self.clean_mapper.unmapped_files.copy()
        
        # Identify missing files (CSV lists files but CleanFileMapper didn't find them)
        for idx, row in self.df.iterrows():
            row_id = str(row['row_id'])
            
            # Check for missing YouTube files
            if pd.notna(row.get('youtube_files')) and row.get('youtube_status') == 'completed':
                expected_files = str(row['youtube_files']).replace(';', ',').split(',')
                found_files = self.definitive_mapping[row_id]['youtube_files']
                
                for expected_file in expected_files:
                    expected_file = expected_file.strip()
                    if expected_file and not any(expected_file in found for found in found_files):
                        self.definitive_mapping[row_id]['missing_files'].append(('youtube', expected_file))
                        self.missing_files.append({
                            'row_id': row_id,
                            'name': row['name'],
                            'file': expected_file,
                            'type': 'youtube'
                        })
            
            # Check for missing Drive files
            if pd.notna(row.get('drive_files')) and row.get('drive_status') == 'completed':
                expected_files = str(row['drive_files']).replace(';', ',').split(',')
                found_files = self.definitive_mapping[row_id]['drive_files']
                
                for expected_file in expected_files:
                    expected_file = expected_file.strip()
                    if expected_file and not any(expected_file in found for found in found_files):
                        self.definitive_mapping[row_id]['missing_files'].append(('drive', expected_file))
                        self.missing_files.append({
                            'row_id': row_id,
                            'name': row['name'],
                            'file': expected_file,
                            'type': 'drive'
                        })
        
        # Print summary
        total_mapped = sum(len(m['all_files']) for m in self.definitive_mapping.values())
        print(f"\nMapping Summary (Clean, No Contamination):")
        print(f"  Total rows with files: {sum(1 for m in self.definitive_mapping.values() if m['all_files'])}")
        print(f"  Total files mapped: {total_mapped}")
        print(f"  Missing files: {len(self.missing_files)}")
        print(f"  Unmapped files: {len(self.unmapped_files)}")
        print(f"  Using CleanFileMapper (no directory contamination)")
    
    def _check_current_mapping(self, file_path: str) -> str:
        """Check what row a file is currently mapped to via metadata"""
        dir_path = os.path.dirname(file_path)
        metadata_files = glob.glob(os.path.join(dir_path, '*metadata.json'))
        
        for metadata_file in metadata_files:
            try:
                with open(metadata_file, 'r') as f:
                    metadata = json.load(f)
                    return str(metadata.get('source_csv_row_id', ''))
            except:
                pass
        
        return None
    
    def create_corrected_metadata(self) -> None:
        """Create corrected metadata for all mapped files"""
        print("\n=== CREATING CORRECTED METADATA ===")
        
        metadata_created = 0
        
        for row_id, mapping in self.definitive_mapping.items():
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
                
                with open(metadata_path, 'w') as f:
                    json.dump(file_metadata, f, indent=2)
                
                metadata_created += 1
        
        print(f"  Created {metadata_created} definitive metadata files")
    
    def save_definitive_mapping(self) -> None:
        """Save the complete definitive mapping"""
        print("\n=== SAVING DEFINITIVE MAPPING ===")
        
        # Create flat format for CSV
        flat_mappings = []
        for row_id, mapping in self.definitive_mapping.items():
            for file_path in mapping['all_files']:
                flat_mappings.append({
                    'row_id': row_id,
                    'name': mapping['name'],
                    'type': mapping['type'],
                    'email': mapping['email'],
                    'file_path': file_path,
                    'filename': os.path.basename(file_path),
                    'file_type': 'youtube' if file_path in mapping['youtube_files'] else 'drive'
                })
        
        if flat_mappings:
            df_mapping = pd.DataFrame(flat_mappings)
            df_mapping.to_csv('definitive_csv_file_mapping.csv', index=False)
            print(f"  Saved {len(flat_mappings)} mappings to: definitive_csv_file_mapping.csv")
        
        # Save conflicts
        if self.conflicts:
            df_conflicts = pd.DataFrame(self.conflicts)
            df_conflicts.to_csv('mapping_conflicts.csv', index=False)
            print(f"  Saved {len(self.conflicts)} conflicts to: mapping_conflicts.csv")
        
        # Save missing files
        if self.missing_files:
            df_missing = pd.DataFrame(self.missing_files)
            df_missing.to_csv('definitive_missing_files.csv', index=False)
            print(f"  Saved {len(self.missing_files)} missing files to: definitive_missing_files.csv")
        
        # Save unmapped files
        if self.unmapped_files:
            df_unmapped = pd.DataFrame({'file_path': self.unmapped_files})
            df_unmapped.to_csv('definitive_unmapped_files.csv', index=False)
            print(f"  Saved {len(self.unmapped_files)} unmapped files to: definitive_unmapped_files.csv")
        
        # Save complete mapping as JSON
        with open('definitive_mapping_complete.json', 'w') as f:
            json.dump(self.definitive_mapping, f, indent=2)
        print(f"  Saved complete mapping to: definitive_mapping_complete.json")
        
        # Summary statistics
        summary = {
            'total_csv_rows': len(self.df),
            'rows_with_files': sum(1 for m in self.definitive_mapping.values() if m['all_files']),
            'total_files_mapped': len(self.mapped_files),
            'conflicts': len(self.conflicts),
            'missing_files': len(self.missing_files),
            'unmapped_files': len(self.unmapped_files),
            'success_rate': f"{len(self.mapped_files) / (len(self.mapped_files) + len(self.missing_files)) * 100:.1f}%"
        }
        
        with open('definitive_mapping_summary.json', 'w') as f:
            json.dump(summary, f, indent=2)
        print(f"\n  Summary saved to: definitive_mapping_summary.json")


def main():
    mapper = DefinitiveMapper()
    
    # Build the definitive mapping from CSV
    mapper.build_definitive_mapping()
    
    # Create corrected metadata
    mapper.create_corrected_metadata()
    
    # Save all results
    mapper.save_definitive_mapping()
    
    print("\n✅ Definitive CSV-to-file mapping complete!")
    print("\nNext steps:")
    print("1. Review mapping_conflicts.csv to see files mapped to wrong rows")
    print("2. Review definitive_missing_files.csv to see what's missing")
    print("3. Use definitive_csv_file_mapping.csv as the authoritative mapping")


if __name__ == "__main__":
    main()